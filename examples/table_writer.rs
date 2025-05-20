use chrono::{NaiveDateTime, Utc};
use dolphindb::{
    client::{ClientBuilder, TableWriter},
    types::PrimitiveType,
};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

#[derive(Clone)]
struct TickerEvent {
    event_time: i64,
    event_time2: NaiveDateTime,
    symbol: String,
    event_id: i64,
    prices: Vec<f64>,
}

fn build_table_row(event: &TickerEvent) -> Vec<PrimitiveType> {
    vec![
        event.event_time.into(),
        event.event_time2.into(),
        event.symbol.clone().into(),
        event.event_id.into(),
        event.prices.clone().into(),
    ]
}

#[tokio::main]
async fn main() {
    // connect to DolphinDB
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    // create a stream table
    let stream_table = "depthStreamTable";
    let script = format!(
        r#"
        colNames = ["event_time", "event_time2", "symbol", "event_id", "prices"]
        colTypes = [TIMESTAMP, TIMESTAMP, SYMBOL, LONG, DOUBLE[]]

        if (!existsStreamTable("{stream_table}")) {{
            enableTableShareAndCachePurge(streamTable(1000000:0, colNames, colTypes), "{stream_table}", 1000000)
        }}
    "#
    );
    client.run_script(&script).await.unwrap();

    // generate data in rust
    let event = TickerEvent {
        event_time: Utc::now().timestamp_millis(),
        event_time2: Utc::now().naive_utc(),
        symbol: "BTCUSDT".into(),
        event_id: 1000,
        prices: vec![5000.0; 100],
    };

    // TableWriter is NOT thread safe.
    // This example show how to insert data from multiple sources.
    let (tx, mut rx) = mpsc::unbounded_channel::<TickerEvent>();
    let symbol_number = 500;
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    let event1 = event.clone();
    let event2 = event.clone();

    // This data source generates 500 * 20 rows every second.
    tokio::spawn(async move {
        loop {
            for _ in 0..symbol_number {
                let _ = tx1.send(event1.clone());
            }
            sleep(Duration::from_millis(1000 / 20)).await;
        }
    });
    // This data source generates 500 * 10 rows every second.
    tokio::spawn(async move {
        loop {
            for _ in 0..symbol_number {
                let _ = tx2.send(event2.clone());
            }
            sleep(Duration::from_millis(1000 / 10)).await;
        }
    });

    tokio::spawn(async move {
        let mut inserted = 0usize;
        let mut writer = TableWriter::new(client, stream_table, 512).await;
        while let Some(event) = rx.recv().await {
            let mut row = build_table_row(&event);
            let res = writer.append_row(&mut row).await;

            match res {
                Ok(_) => {
                    inserted += 1;
                    if inserted % 10000 == 0 {
                        println!("{} rows inserted and {} rows in buffer", inserted, rx.len());
                    }
                }
                Err(e) => {
                    eprintln!("Insertion failed: {:?}", e);
                    inserted += 1;
                }
            }
        }
    });

    println!("Insertion started.");
    futures::future::pending::<()>().await;
}
