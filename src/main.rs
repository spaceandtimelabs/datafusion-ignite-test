mod ignite_tableprovider;
mod ignite_exec;

use std::sync::Arc;
use datafusion::arrow;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableReference;
use ignite_rs::{ClientConfig, Ignite};
use crate::ignite_tableprovider::IgniteTable;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init DataFusion
    let ctx = SessionContext::new();

    // Add tables from ignite
    let client_config = ClientConfig::new("localhost:10800");
    let mut ignite = ignite_rs::new_client(client_config)?;
    let names = ignite.get_cache_names()?;
    for name in names.iter() {
        println!("Got: {}", name);
    }
    let name = "SQL_PUBLIC_REGION";
    let cfg = ignite.get_cache_config(name)?;
    let table_name = TableReference::Full { catalog: "datafusion", schema: "public", table: name };
    let provider = Arc::new(IgniteTable::new(cfg)?);
    ctx.register_table(table_name, provider)?;

    ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await?;

    // create a plan
    // let df = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;
    let df = ctx.sql("SELECT * from SQL_PUBLIC_REGION").await?;

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await?;

    // format the results
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?
        .to_string();

    let expected = vec![
        "+---+----------------+",
        "| a | MIN(example.b) |",
        "+---+----------------+",
        "| 1 | 2              |",
        "+---+----------------+"
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
    Ok(())
}
