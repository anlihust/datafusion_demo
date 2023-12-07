use arrow::{
    array::{Int64Array, StringArray, StructArray},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    execution::context::SessionContext,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{fs::File, sync::Arc};

#[tokio::main]
async fn main() {
    let file_path = "./test.parquet";
    write_file(file_path);
    search(file_path).await;
}

async fn search(data_path: &str) {
    let ctx = SessionContext::new();
    let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));
    ctx.register_listing_table("base_table", data_path, opt, None, None)
        .await
        .unwrap();
    let sql = "select * from base_table";
    ctx.sql(sql).await.unwrap().show().await.unwrap();
    // will output 2 results
    //     +---------------------+----+--------+
    //     | struct              | id | name   |
    //     +---------------------+----+--------+
    //     | {id: 1, name: aaa1} | 1  | test01 |
    //     | {id: 2, name: aaa2} | 2  | test02 |
    //     +---------------------+----+--------+
    let sql = "select * from base_table where name='test01'";
    ctx.sql(sql).await.unwrap().show().await.unwrap();
    // output empty results
}

/// write 2 row to parquet
///     +---------------------+----+--------+
///     | struct              | id | name   |
///     +---------------------+----+--------+
///     | {id: 1, name: aaa1} | 1  | test01 |
///     | {id: 2, name: aaa2} | 2  | test02 |
///     +---------------------+----+--------+
fn write_file(file: &str) {
    let struct_fields = Fields::from(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let schema = Schema::new(vec![
        Field::new("struct", DataType::Struct(struct_fields.clone()), false),
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, false),
    ]);
    let id_array = Int64Array::from(vec![Some(1), Some(2)]);
    let columns = vec![
        Arc::new(Int64Array::from(vec![3, 4])) as _,
        Arc::new(StringArray::from(vec!["aaa1", "aaa2"])) as _,
    ];
    let struct_array = StructArray::new(struct_fields, columns, None);

    let name_array = StringArray::from(vec![Some("test01"), Some("test02")]);
    let schema = Arc::new(schema);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(struct_array),
            Arc::new(id_array),
            Arc::new(name_array),
        ],
    )
    .unwrap();
    let file = File::create(file).unwrap();
    let w_opt = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(w_opt)).unwrap();
    writer.write(&batch).unwrap();
    writer.flush().unwrap();
    writer.close().unwrap();
}
