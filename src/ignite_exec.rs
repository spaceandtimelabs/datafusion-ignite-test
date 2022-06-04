use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc};
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray, UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, project_schema, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::memory::MemoryStream;
use ignite_rs::{Client, Ignite};
use crate::arrow::datatypes::SchemaRef;
use crate::dynamic_type::DynamicIgniteType;

pub struct IgniteExec {
    client: Client,
    table_name: String,
    projected_schema: SchemaRef,
}

impl IgniteExec {
    pub fn new(
        client: Client,
        table_name: &str,
        projections: &Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> IgniteExec {
        let projected_schema = project_schema(&schema, projections.as_ref()).unwrap();
        Self {
            client,
            table_name: table_name.to_string(),
            projected_schema,
        }
    }
}

impl Debug for IgniteExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for IgniteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> crate::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> crate::Result<SendableRecordBatchStream> {
        let mut _id_array = UInt8Builder::new(0);
        let mut _account_array = UInt64Builder::new(0);

        let mut client = self.client.clone();
        let cache = client
            .get_or_create_cache::<DynamicIgniteType, DynamicIgniteType>(&self.table_name)
            .map_err(|e| DataFusionError::Execution(e.to_string()) )?;

        if let Some(ref entities) = cache.cfg.query_entities {
            if let Some(entity) = entities.get(0) {
                println!("entity={:?}", entity);
                for field in entity.query_fields.iter() {
                    println!("field={:?}", field);
                }
            }
        }
        let records = cache.query_scan(1024)
            .map_err(|e| DataFusionError::Execution(e.to_string()) )?;
        for _record in records {
            println!("record!");
        }

        let mut columns: Vec<ArrayRef> = vec![];
        for field in self.projected_schema.fields().iter() {
            let column: ArrayRef = match field.data_type() {
                DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
                DataType::Utf8 => Arc::new(StringArray::from(Vec::<String>::new())),
                _ => return Err(DataFusionError::NotImplemented(format!("Unknown type: {}", field.data_type())))
            };
            columns.push(column);
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                columns,
            )?],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}