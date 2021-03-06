use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use datafusion::arrow::array::{ArrayBuilder, ArrayRef, Int32Array, StringBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, project_schema, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::memory::MemoryStream;
use ignite_rs::{Client, Ignite};
use ignite_rs::cache::QueryEntity;
use ignite_rs::error::IgniteError;
use ignite_rs::protocol::{read_complex_obj_dyn, read_i32, read_string, read_u8, read_wrapped_data_dyn, TypeCode};
use crate::arrow::array::PrimitiveBuilder;
use crate::arrow::datatypes::{Int32Type, SchemaRef};
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
        // Get the cache
        let mut client = self.client.clone();
        let cache = client
            .get_or_create_cache::<DynamicIgniteType, DynamicIgniteType>(&self.table_name)
            .map_err(|e| DataFusionError::Execution(e.to_string()) )?;

        // Get the entity definition
        let entity = (|entities: &Option<Vec<QueryEntity>>| -> crate::Result<QueryEntity> {
            let entities = entities.as_ref()
                .ok_or(DataFusionError::Internal("No entities!".to_string()))?;
            if entities.len() > 1 {
                Err(DataFusionError::Internal("More than one entity!".to_string()))?;
            }
            let entity = entities.get(0)
                .ok_or(DataFusionError::Internal("No entities!".to_string()))?;
            Ok(entity.clone())
        })(&cache.cfg.query_entities)?;

        let columns: Arc<Mutex<Vec<Box<dyn ArrayBuilder>>>> = Arc::new(Mutex::new(vec![]));
        cache.query_scan_dyn(1024, &mut |reader, count| {

            // allocate columns
            for field in entity.query_fields.iter() {
                println!("field={:?}", field);
                match field.type_name.as_str() {
                    "java.lang.Integer" => {
                        let ar = Int32Array::builder(count as usize);
                        columns.lock().unwrap().push(Box::new(ar));
                    }
                    "java.lang.String" => {
                        let ar = StringBuilder::new(count as usize);
                        columns.lock().unwrap().push(Box::new(ar));
                    }
                    _ => todo!("Unknown field type: {}", field.type_name)
                }
            }

            // fill columns from rows
            for _ in 0..count {
                // key
                match entity.key_type.as_str() {
                    "java.lang.Integer" => {
                        let key_type_code = read_u8(reader)?;
                        match TypeCode::try_from(key_type_code)? {
                            TypeCode::Int => {
                                let mut cols = columns.lock().unwrap();
                                let sb = cols.get_mut(0).unwrap().as_mut();
                                let sb = sb.as_any_mut().downcast_mut::<PrimitiveBuilder<Int32Type>>().unwrap();
                                let v = read_i32(reader)?;
                                sb.append_value(v).unwrap();
                            },
                            _ => todo!("Unknown type: {}", key_type_code)
                        }
                    },
                    _ => {
                        let str = format!("Unknown type: {}", entity.key_type.as_str());
                        Err(IgniteError::from(str.as_str()))?
                    }
                }

                // value
                read_wrapped_data_dyn(reader, &mut |reader, _len| {
                    read_complex_obj_dyn(reader, &mut |reader, _len| {
                        let _len = read_i32(reader)?;

                        let mut cols = columns.lock().unwrap();
                        for idx in 1..entity.query_fields.len() {
                            let col = cols.get_mut(idx).unwrap();
                            let _name = read_u8(reader)?; // col_name? always null
                            let field = entity.query_fields.get(idx).unwrap();
                            match field.type_name.as_str() {
                                "java.lang.String" => {
                                    let ar = col.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                                    let v = read_string(reader)?;
                                    ar.append_value(v).unwrap();
                                }
                                _ => todo!("Unknown field type: {}", field.type_name)
                            }
                        }

                        Ok(())
                    })
                })?;
            }

            Ok(())
        }).map_err(|e| DataFusionError::Execution(e.to_string()) )?;

        let mut vals = columns.lock().unwrap();
        let cols: Vec<ArrayRef> = vals.iter_mut().map(|it| it.finish()).collect();

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                cols,
            )?],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}