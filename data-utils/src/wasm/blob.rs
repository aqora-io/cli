use std::io;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::wasm_bindgen;
use web_sys::{Blob, BlobPropertyBag};

use crate::error::Result;
use crate::format::{FileKind, FormatReader};

use super::format::JsFormatReader;
use super::io::AsyncBlobReader;

impl FormatReader<AsyncBlobReader> {
    pub async fn infer_blob(blob: Blob, max_records: Option<usize>) -> io::Result<Self> {
        let file_kind = FileKind::from_mime(blob.type_())
            .ok_or_else(|| io::Error::other("Extension does not match known formats"))?;
        FormatReader::infer_format(AsyncBlobReader::new(blob), file_kind, max_records).await
    }
}

#[wasm_bindgen(js_class = FormatReader)]
impl JsFormatReader {
    #[wasm_bindgen(js_name = "inferBlob")]
    pub async fn infer_blob(
        blob: Blob,
        max_records: Option<usize>,
    ) -> Result<JsFormatReader, JsError> {
        FormatReader::infer_blob(blob, max_records)
            .await?
            .try_into()
    }
}

#[wasm_bindgen]
pub async fn open(blob: Blob) -> Result<JsFormatReader, JsError> {
    JsFormatReader::infer_blob(blob, Some(100)).await
}

fn chunk_vector<T>(mut vec: Vec<T>, chunk_size: usize) -> Box<[Box<[T]>]> {
    let rem = vec.len() % chunk_size;
    let len = if rem == 0 {
        vec.len() / chunk_size
    } else {
        vec.len() / chunk_size + 1
    };
    let mut out = Box::new_uninit_slice(len);
    let mut index = len;
    if rem != 0 {
        index -= 1;
        out[index].write(vec.split_off(vec.len() - rem).into_boxed_slice());
        vec.shrink_to_fit();
    }
    while !vec.is_empty() {
        index -= 1;
        out[index].write(vec.split_off(vec.len() - chunk_size).into_boxed_slice());
        vec.shrink_to_fit();
    }
    unsafe { out.assume_init() }
}

pub fn vec_to_blob(vec: Vec<u8>, chunk_size: u32, options: &BlobPropertyBag) -> Result<Blob> {
    Ok(Blob::new_with_u8_array_sequence_and_options(
        super::iter::iterable(
            Vec::from(chunk_vector(vec, chunk_size as usize))
                .into_iter()
                .map(|chunk| {
                    let buffer = js_sys::Uint8Array::new_with_length(chunk.len() as u32);
                    buffer.copy_from(&chunk);
                    Ok(buffer.into())
                }),
        )?
        .as_ref(),
        options,
    )?)
}
