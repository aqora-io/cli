use bytes::Bytes;
use futures::channel::mpsc;
use futures::prelude::*;
use tokio::io::AsyncReadExt;
use wasm_bindgen::JsCast;
use wasm_bindgen::{closure::Closure, JsValue};
use wasm_bindgen_test::*;

use aqora_data_utils::format::FileKind;
use aqora_data_utils::wasm::{
    error::set_console_error_panic_hook,
    format::{JsFormat, JsFormatReader},
    io::readable_stream_to_async_read,
    serde::{from_value, to_value},
    write::{
        JsColumnProperties, JsCompression, JsPartWriter, JsPartWriterOptions, JsWriteOptions,
        JsWriterProperties,
    },
};

use super::data::*;
use super::utils::check_serde;

#[wasm_bindgen_test]
pub fn test_format_serde() {
    set_console_error_panic_hook();
    let default_json = JsFormat::Json(Default::default());
    check_serde(&default_json);
    let default_csv = JsFormat::Csv(Default::default());
    check_serde(&default_csv);

    let csv_format_custom_null = aqora_data_utils::csv::CsvFormat {
        regex: aqora_data_utils::csv::CsvFormatRegex {
            null_regex: Some(regex::Regex::new("^NIL$").unwrap()),
            ..Default::default()
        },
        ..Default::default()
    };
    let format_custom_null = JsFormat::Csv(csv_format_custom_null.try_into().unwrap());
    check_serde(&format_custom_null);
}

fn new_blob(file: &include_dir::File) -> web_sys::Blob {
    let options = web_sys::BlobPropertyBag::new();
    let file_kind = FileKind::from_ext(file.path().extension().unwrap_or_default()).unwrap();
    options.set_type(match file_kind {
        FileKind::Csv => "text/csv",
        FileKind::Json => "application/json",
        _ => unreachable!(),
    });
    web_sys::Blob::new_with_u8_array_sequence_and_options(
        &js_sys::Array::of1(&js_sys::Uint8Array::from(file.contents())),
        &options,
    )
    .unwrap()
}

#[wasm_bindgen_test]
pub async fn test_infer_schema() {
    for format in [CSV, JSON] {
        for file in data_files(format).files() {
            let data_schema = data_schema_for(file.path());
            let data_config = data_config_for(file.path());
            let blob = new_blob(file);
            let reader = if let Some(config) = data_config {
                JsFormatReader::new(blob, to_value(&config).unwrap())
            } else {
                JsFormatReader::infer_blob(blob, None).await.unwrap()
            };
            let schema = reader.infer_schema(JsValue::UNDEFINED).await;
            match data_schema {
                DataSchema::Error => {
                    assert!(schema.is_err());
                }
                DataSchema::Schema(expected) => {
                    let schema = schema.unwrap();
                    assert_eq!(expected, from_value(schema).unwrap())
                }
            }
        }
    }
}

fn build_part_writer() -> (JsPartWriter, mpsc::Receiver<Bytes>) {
    let (tx, rx) = mpsc::channel(1);
    let part_writer = JsPartWriter::new(JsPartWriterOptions {
        on_stream: Closure::<dyn FnMut(web_sys::ReadableStream)>::new(
            move |readable: web_sys::ReadableStream| {
                let mut tx = tx.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    let mut array = Vec::new();
                    readable_stream_to_async_read(readable)
                        .read_to_end(&mut array)
                        .await
                        .unwrap();
                    tx.send(Bytes::from(array)).await.unwrap();
                });
            },
        )
        .into_js_value()
        .unchecked_into(),
        max_partition_size: None,
        buffer_size: None,
    });
    (part_writer, rx)
}

#[wasm_bindgen_test]
pub async fn test_write_to_parquet() {
    for format in [CSV, JSON] {
        for file in data_files(format).files() {
            if matches!(data_schema_for(file.path()), DataSchema::Error) {
                continue;
            }
            let data_config = data_config_for(file.path());
            let blob = new_blob(file);
            let reader = if let Some(config) = data_config {
                JsFormatReader::new(blob, to_value(&config).unwrap())
            } else {
                JsFormatReader::infer_blob(blob, None).await.unwrap()
            };
            let (mut part_writer, mut rx) = build_part_writer();
            reader
                .infer_and_stream_record_batches(JsValue::UNDEFINED)
                .await
                .unwrap()
                .write_parquet(&mut part_writer, JsValue::UNDEFINED)
                .await
                .unwrap();
            assert!(!rx.next().await.unwrap().is_empty());
        }
    }
}

#[wasm_bindgen_test]
pub async fn test_compression_codecs() {
    let reader = JsFormatReader::infer_blob(
        new_blob(TEST_DATA.get_file("files/json/basic.json").unwrap()),
        None,
    )
    .await
    .unwrap();
    let schema = reader.infer_schema(JsValue::UNDEFINED).await.unwrap();
    for compression in [
        JsCompression::Uncompressed,
        JsCompression::Snappy,
        JsCompression::Gzip { level: Some(4) },
        JsCompression::Brotli { level: Some(6) },
        JsCompression::Lz4,
        JsCompression::Lz4Raw,
        JsCompression::Zstd { level: Some(10) },
    ] {
        let record_batches = reader
            .stream_record_batches(schema.clone(), JsValue::UNDEFINED)
            .await
            .unwrap();
        let (mut part_writer, mut rx) = build_part_writer();
        record_batches
            .write_parquet(
                &mut part_writer,
                to_value(&JsWriteOptions {
                    writer_properties: JsWriterProperties {
                        default_column_properties: JsColumnProperties {
                            compression: Some(compression),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(!rx.next().await.unwrap().is_empty());
    }
}
