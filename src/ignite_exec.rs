use std::any::Any;
use std::fmt::{Debug};
use std::sync::Arc;
use anyhow::anyhow;
use datafusion::arrow::array::{ArrayRef, Int32Array, Int32Builder, StringArray, UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::Utf8Builder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, project_schema, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::memory::MemoryStream;
use crate::arrow::datatypes::SchemaRef;
use crate::col;

#[derive(Debug)]
pub struct IgniteExec {
    projected_schema: SchemaRef,
}

impl IgniteExec {
    pub fn new(
        projections: &Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> IgniteExec {
        let projected_schema = project_schema(&schema, projections.as_ref()).unwrap();
        Self {
            projected_schema,
        }
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

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> crate::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> crate::Result<SendableRecordBatchStream> {
        let mut id_array = UInt8Builder::new(0);
        let mut account_array = UInt64Builder::new(0);

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