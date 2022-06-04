use std::any::Any;
use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::ExecutionPlan;
use crate::arrow::datatypes::SchemaRef;
use crate::Expr;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use ignite_rs::cache::CacheConfiguration;
use ignite_rs::Client;
use crate::ignite_exec::IgniteExec;

pub struct IgniteTable {
    client: Client,
    cfg: CacheConfiguration,
    schema: SchemaRef,
}

impl IgniteTable {
    pub fn new(client: Client, cfg: CacheConfiguration) -> anyhow::Result<IgniteTable> {
        let mut fields: Vec<Field> = vec![];
        if let Some(ref entities) = cfg.query_entities {
            let len = entities.len();
            if len > 1 {
                return Err(anyhow!("More than one query entity!"))
            }
            let entity = entities.iter().next().ok_or(anyhow!("No query entities!"))?;
            for field in entity.query_fields.iter() {
                println!("   field {} {}", field.name, field.type_name);
                let field_type = match field.type_name.as_str() {
                    "java.lang.Integer" => DataType::Int32,
                    "java.lang.String" => DataType::Utf8,
                    _ => Err(anyhow!("Type not supported: {}", field.type_name))?
                };
                let field = Field::new(field.name.as_str(), field_type, !field.not_null_constraint);
                fields.push(field);
            }
            let schema = SchemaRef::new(Schema::new(fields));
            Ok(Self { client, cfg, schema })
        } else {
            Err(anyhow!("No query entities!"))
        }
    }
}

#[async_trait]
impl TableProvider for IgniteTable {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(&self,
                  projection: &Option<Vec<usize>>,
                  _filters: &[Expr],
                  _limit: Option<usize>
    ) -> crate::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IgniteExec::new(
            self.client.clone(),
            self.cfg.name.as_str(),
            projection,
            self.schema(),
        )))
    }
}