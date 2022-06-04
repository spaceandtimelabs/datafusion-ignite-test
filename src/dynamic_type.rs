use std::io::{Read, Write};
use datafusion::arrow::datatypes::SchemaRef;
use ignite_rs::error::IgniteResult;
use ignite_rs::protocol::TypeCode;
use ignite_rs::{ReadableType, WritableType};

pub struct DynamicIgniteType {
    schema: SchemaRef,
}

impl DynamicIgniteType {
    pub fn new(schema: SchemaRef) -> DynamicIgniteType {
        Self {
            schema,
        }
    }
}

impl ReadableType for DynamicIgniteType {
    fn read_unwrapped(
        type_code: TypeCode,
        reader: &mut impl Read,
    ) -> IgniteResult<Option<Self>> {
        let value: Option<Self> = match type_code {
            TypeCode::Null => None,
            _ => {
                ignite_rs::protocol::read_u8(reader)?;
                let flags = ignite_rs::protocol::read_u16(reader)?;
                if (flags & ignite_rs::protocol::FLAG_HAS_SCHEMA) == 0 {
                    return Err(ignite_rs::error::IgniteError::from(
                        "Serialized object schema expected!",
                    ));
                }
                if (flags & ignite_rs::protocol::FLAG_COMPACT_FOOTER) != 0 {
                    return Err(ignite_rs::error::IgniteError::from(
                        "Compact footer is not supported!",
                    ));
                }
                if (flags & ignite_rs::protocol::FLAG_OFFSET_ONE_BYTE) != 0
                    || (flags & ignite_rs::protocol::FLAG_OFFSET_TWO_BYTES) != 0
                {
                    return Err(ignite_rs::error::IgniteError::from(
                        "Schema offset=4 is expected!",
                    ));
                }
                let type_id = ignite_rs::protocol::read_i32(reader)?;
                if type_id != -1976154330i32 {
                    return Err(ignite_rs::error::IgniteError::from("Unknown format type!"));
                }
                ignite_rs::protocol::read_i32(reader)?;
                ignite_rs::protocol::read_i32(reader)?;
                ignite_rs::protocol::read_i32(reader)?;
                ignite_rs::protocol::read_i32(reader)?;
                let _bar = <String>::read(reader)?.unwrap();
                let _foo = <i32>::read(reader)?.unwrap();
                for _ in 0..2usize {
                    ignite_rs::protocol::read_i64(reader)?;
                }
                todo!("Instantiate and return")
            }
        };
        Ok(value)
    }
}

impl WritableType for DynamicIgniteType {
    fn write(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }
}