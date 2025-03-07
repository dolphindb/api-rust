use std::net::SocketAddr;
use std::sync::Arc;

use futures::Stream;
use tokio::runtime::Handle;
use tokio::task::block_in_place;
use tokio::{
    net::ToSocketAddrs,
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};

use crate::client::{Client, ClientBuilder};
use crate::error::Error;
use crate::stream_client::reconnect::ReConnector;
use crate::types::{
    Any, Constant, ConstantImpl, DataType, DolphinString, ScalarImpl, Vector, VectorImpl,
};
use crate::{BehaviorOptions, Result};

use super::message_parser::MessageParser;
use super::{message::Message, request::Request};

pub struct Subscriber {
    pub(crate) client: Option<Client>,
    pub(crate) req: Request,
    pub(crate) rx: UnboundedReceiver<Arc<Message>>,
}

impl Subscriber {
    async fn unsubscribe(&mut self) -> Result<()> {
        if let Some(client) = self.client.as_mut() {
            let topic = get_topic(
                client,
                self.req.table_name.clone(),
                self.req.action_name.clone(),
            )
            .await?;
            stop_publish_table(client, topic, &self.req).await?;
        }
        Ok(())
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        block_in_place(|| {
            Handle::current().block_on(async move {
                let _ = self.unsubscribe().await;
            });
        });
    }
}

impl Stream for Subscriber {
    type Item = Arc<Message>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

#[derive(Default)]
pub struct SubscriberBuilder {}

impl SubscriberBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe a stream table.
    /// This funcion will instruct the dolphindb server to publish data.
    pub async fn subscribe<A>(&mut self, addr: A, req: Request) -> Result<Subscriber>
    where
        A: ToSocketAddrs + Clone + Send + Sync + 'static,
    {
        if req.reconnect {
            return self.reconnect_subscribe(addr, req).await;
        }

        let rx = self.subscribe_channel(addr.clone(), &req).await?;

        let mut builder = ClientBuilder::new(addr.clone());
        if let Some(ref auth) = req.auth {
            builder.with_auth((auth.0.as_str(), auth.1.as_str()));
        }
        let client = control_client(addr, &req.auth).await?;

        Ok(Subscriber {
            client: Some(client),
            req,
            rx,
        })
    }

    pub(crate) async fn subscribe_channel<A>(
        &mut self,
        addr: A,
        req: &Request,
    ) -> Result<UnboundedReceiver<Arc<Message>>>
    where
        A: ToSocketAddrs + Clone + Send + Sync + 'static,
    {
        let mut stream = stream_client(addr.clone(), &req.auth).await?;

        let topic = get_topic(&mut stream, req.table_name.clone(), req.action_name.clone()).await?;

        publish_table(&mut stream, topic, req).await?;

        let (tx, rx) = unbounded_channel();
        let mut parser = MessageParser::new(tx);

        tokio::spawn(async move { parser.run(stream).await });

        Ok(rx)
    }

    async fn reconnect_subscribe<A>(&mut self, addr: A, req: Request) -> Result<Subscriber>
    where
        A: ToSocketAddrs + Clone + Send + Sync + 'static,
    {
        let (tx, rx) = unbounded_channel();
        let re_connector = ReConnector::new(req.clone(), tx);

        tokio::spawn(async move { re_connector.run(addr).await });

        Ok(Subscriber {
            client: None,
            req,
            rx,
        })
    }
}

async fn stream_client<A>(addr: A, auth: &Option<(String, String)>) -> Result<Client>
where
    A: ToSocketAddrs,
{
    let mut option = BehaviorOptions::default();
    option.is_subscribe(true);

    let mut stream_builder = ClientBuilder::new(addr);
    stream_builder.with_option(option);

    if let Some(auth) = auth {
        stream_builder.with_auth((auth.0.as_str(), auth.1.as_str()));
    }

    stream_builder.connect().await
}

pub(crate) async fn control_client<A>(addr: A, auth: &Option<(String, String)>) -> Result<Client>
where
    A: ToSocketAddrs,
{
    let mut builder = ClientBuilder::new(addr);

    if let Some(auth) = auth {
        builder.with_auth((auth.0.as_str(), auth.1.as_str()));
    }

    builder.connect().await
}

async fn get_topic(client: &mut Client, table: String, action: String) -> Result<String> {
    let res = client
        .run_function(
            "getSubscriptionTopic",
            &[
                DolphinString::from(table).into(),
                DolphinString::from(action).into(),
            ],
        )
        .await?
        .ok_or(Error::BadResponse("null topic string from server".into()))?;

    let v = VectorImpl::try_from(res)?;
    let mut v = Vector::<Any>::try_from(v)?;

    if v.len() != 2 {
        return Err(Error::InvalidData {
            expect: "any vector of size 2".into(),
            actual: format!("any vector of size {}", v.len()),
        });
    }

    if v[1].get().data_type() == DataType::Void {
        return Err(Error::BadResponse("no publish table exist".into()));
    }

    let s = ScalarImpl::try_from(v.swap_remove(0).into_inner())?; // dump way to move out value from Vec
    let s = DolphinString::try_from(s)?;
    s.into_inner()
        .ok_or(Error::BadResponse("null topic string from server".into()))
}

fn publish_table_args(addr: SocketAddr, req: Request) -> Vec<ConstantImpl> {
    let mut res = Vec::with_capacity(7);

    res.push(ScalarImpl::String(addr.ip().to_string().into()).into());
    res.push(ScalarImpl::Int((addr.port() as i32).into()).into());
    res.push(ScalarImpl::String(req.table_name.into()).into());
    res.push(ScalarImpl::String(req.action_name.into()).into());
    res.push(ScalarImpl::Long(req.offset.into()).into());

    if let Some(filter) = req.filter {
        res.push(filter.into());
    } else {
        res.push(ScalarImpl::Void(().into()).into());
    }

    res
}

async fn publish_table(client: &mut Client, _topic: String, req: &Request) -> Result<()> {
    let _res = client
        .run_function(
            "publishTable",
            &publish_table_args(client.local_addr(), req.clone()),
        )
        .await?;

    // todo: bind topic and res to support ha and reconnect

    Ok(())
}

fn stop_publish_table_args(addr: SocketAddr, req: Request) -> Vec<ConstantImpl> {
    let mut res = Vec::with_capacity(7);

    res.push(ScalarImpl::String(addr.ip().to_string().into()).into());
    res.push(ScalarImpl::Int((addr.port() as i32).into()).into());
    res.push(ScalarImpl::String(req.table_name.into()).into());
    res.push(ScalarImpl::String(req.action_name.into()).into());

    res
}

async fn stop_publish_table(client: &mut Client, _topic: String, req: &Request) -> Result<()> {
    let _res = client
        .run_function(
            "stopPublishTable",
            &stop_publish_table_args(client.local_addr(), req.clone()),
        )
        .await?;

    Ok(())
}
