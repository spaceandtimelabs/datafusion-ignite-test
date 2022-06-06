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
use ignite_rs::cache::QueryEntity;
use ignite_rs::error::IgniteError;
use ignite_rs::protocol::{FLAG_COMPACT_FOOTER, FLAG_HAS_SCHEMA, FLAG_OFFSET_ONE_BYTE, read_i32, read_string, read_u16, read_u8, read_wrapped_data, TypeCode};
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
        for field in entity.query_fields.iter() {
            println!("field={:?}", field);
        }

        cache.query_scan_dyn(1024, &mut |reader, count| {
            // key
            match entity.key_type.as_str() {
                "java.lang.Int" => {
                    let key_type_code = read_u8(reader)?; // 3 = i32
                    if TypeCode::try_from(key_type_code)? != TypeCode::Int {
                        Err(IgniteError::from("Invalid type!"))?
                    }
                    let region_key = read_i32(reader)?; // i32
                },
                _ => Err(IgniteError::from("Invalid type!"))?
            }

            // value
            let val_type_code = read_u8(reader)?; // 27 = WrappedData
            let wrapped_len = read_i32(reader)?;

            let val_type = read_u8(reader)?; // 103 = Complex object
            let ver = read_u8(reader)?; // 1
            let flags = read_u16(reader)?; // 43
            let type_id = read_i32(reader)?;
            let hash_code = read_i32(reader)?;
            let complex_obj_len = read_i32(reader)?; // 157
            let schema_id = read_i32(reader)?;
            let t2 = read_i32(reader)?; // 155
            
            let t3 = read_u8(reader)?; // 9 = Null
            let name = read_string(reader)?;
            println!("{} {}", name.len(), name);

            let t5 = read_u8(reader)?; // 9 = Null
            let comment = read_string(reader)?;
            println!("{} {}", comment.len(), comment);

            // Footer / schema
            if flags & FLAG_HAS_SCHEMA != 0 {
                if flags & FLAG_COMPACT_FOOTER != 0 {
                    if flags & FLAG_OFFSET_ONE_BYTE != 0 {
                        let field_count = 2;
                        for _ in 0..field_count {
                            let _ = read_u8(reader)?;
                        }
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            } else {
                // no schema, nothing to do
            }
            
            let wrapped_offset = read_i32(reader)?;

            Ok(())
        }).map_err(|e| DataFusionError::Execution(e.to_string()) )?;

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