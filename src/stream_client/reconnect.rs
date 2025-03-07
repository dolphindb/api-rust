use std::sync::Arc;

use tokio::{net::ToSocketAddrs, sync::mpsc::UnboundedSender};

use super::{
    message::Message,
    request::Request,
    subscriber::{control_client, Subscriber, SubscriberBuilder},
};

use crate::error::Result;

pub(crate) struct ReConnector {
    req: Request,
    tx: UnboundedSender<Arc<Message>>,
}

impl ReConnector {
    pub(crate) fn new(req: Request, tx: UnboundedSender<Arc<Message>>) -> Self {
        Self { req, tx }
    }

    pub(crate) async fn run<A>(self, addr: A) -> Result<()>
    where
        A: ToSocketAddrs + Clone + Send + Sync + 'static,
    {
        loop {
            let mut builder = SubscriberBuilder::new();
            let mut rx = builder.subscribe_channel(addr.clone(), &self.req).await?;

            while let Some(msg) = rx.recv().await {
                let res = self.tx.send(msg);

                // send error means client subscriber's rx is dropped, no need to reconnect more.
                if res.is_err() {
                    // construct a immediately dropped subscriber to unsubscribe
                    let client = control_client(addr, &self.req.auth).await?;
                    let _subscriber = Subscriber {
                        client: Some(client),
                        req: self.req,
                        rx,
                    };

                    return Ok(());
                }
            }

            tokio::time::sleep(self.req.reconnect_timeout).await;
        }
    }
}
