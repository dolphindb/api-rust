use std::time::Duration;

use dolphindb::stream_client::{request::Request, subscriber::SubscriberBuilder};
use futures::StreamExt;

async fn example(action: String) {
    let mut req = Request::new("shared_stream_table".into(), action);
    req.with_offset(0);
    req.with_auth(("admin", "123456"));

    let mut builder = SubscriberBuilder::new();

    // subscriber implement `futures::Stream`, so it can be treated as iterator.
    let mut subscriber = builder
        .subscribe("127.0.0.1:8848", req)
        .await
        .unwrap()
        .skip(3)
        .take(3);

    // consume one message at a time, similar to C++ api's `MessageHandler`.
    while let Some(msg) = subscriber.next().await {
        println!(
            "topic: {}, offset: {}, content: {}",
            msg.topic(),
            msg.offset(),
            msg.msg()
        );
    }

    let mut batch = Vec::with_capacity(1024);
    let throttle = Duration::from_millis(100);

    // consume messages in batch, similar to C++ api's `MessageBatchHandler`.
    loop {
        tokio::select! {
            Some(msg) = subscriber.next() => {
                batch.push(msg);
                if batch.len() == batch.capacity() {
                    println!("consume {} messages", batch.len());
                    batch.clear();
                }
            }
            _ = tokio::time::sleep(throttle) => {
                println!("consume {} messages", batch.len());
                batch.clear();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let t1 = tokio::spawn(async { example("example1".into()).await });
    let t2 = tokio::spawn(async { example("example2".into()).await });

    t1.await.unwrap();
    t2.await.unwrap();
}
