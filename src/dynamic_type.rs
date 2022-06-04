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
    fn read_unwrapped(_type_code: TypeCode, _reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        todo!()
    }

    fn read(_reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        todo!()
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