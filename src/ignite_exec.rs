use std::any::Any;
use std::fmt::{Debug};
use std::sync::Arc;
use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::memory::MemoryStream;
use crate::arrow::datatypes::SchemaRef;

#[derive(Debug)]
pub struct IgniteExec {
    schema: SchemaRef,
}

impl IgniteExec {
    pub fn new(schema: SchemaRef) -> IgniteExec {
        IgniteExec {
            schema,
        }
    }
}

impl ExecutionPlan for IgniteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        let mut id_array = UInt8Builder::new(users.len());
        let mut account_array = UInt64Builder::new(users.len());

        for user in users {
            id_array.append_value(user.id)?;
            account_array.append_value(user.bank_account)?;
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(id_array.finish()),
                    Arc::new(account_array.finish()),
                ],
            )?],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}