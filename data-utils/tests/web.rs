//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn new_blob(size: usize) -> web_sys::Blob {
    let buffer = js_sys::Uint8Array::new_with_length(size as u32);
    let buffers = js_sys::Array::new();
    buffers.push(&buffer);
    web_sys::Blob::new_with_buffer_source_sequence(&buffers).unwrap()
}

async fn read_to_vec<R>(reader: &mut R) -> Vec<u8>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut buffer = Vec::new();
    assert!(reader.read_to_end(&mut buffer).await.is_ok());
    buffer
}

#[wasm_bindgen_test]
fn test_js_async_reader() {
    for size in [0, 1, 2, 32, 64, 128, 500, 1000, 10_000] {
        aqora_data_utils::wasm::utils::set_console_error_panic_hook();
        wasm_bindgen_futures::spawn_local(async move {
            let mut reader =
                aqora_data_utils::wasm::utils::JsAsyncReader::new(new_blob(size).stream().values());
            assert_eq!(read_to_vec(&mut reader).await.len(), size);
        });
    }
}

#[wasm_bindgen_test]
fn test_async_blob_reader_read() {
    aqora_data_utils::wasm::utils::set_console_error_panic_hook();
    for size in [0, 1, 2, 32, 64, 128, 500, 1000, 10_000] {
        wasm_bindgen_futures::spawn_local(async move {
            let mut reader = aqora_data_utils::wasm::utils::AsyncBlobReader::new(new_blob(size));
            assert_eq!(read_to_vec(&mut reader).await.len(), size);
        });
    }
}

#[wasm_bindgen_test]
fn test_async_blob_reader_seek() {
    aqora_data_utils::wasm::utils::set_console_error_panic_hook();
    wasm_bindgen_futures::spawn_local(async move {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
        let mut reader = aqora_data_utils::wasm::utils::AsyncBlobReader::new(new_blob(100));
        assert_eq!(read_to_vec(&mut reader).await.len(), 100);
        assert_eq!(read_to_vec(&mut reader).await.len(), 0);
        reader.rewind().await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 100);
        reader.seek(SeekFrom::Start(50)).await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 50);
        reader.seek(SeekFrom::End(-25)).await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 25);
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        let mut buffer = vec![0; 10];
        reader.read_exact(&mut buffer).await.unwrap();
        reader.seek(SeekFrom::Current(10)).await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 70);
        reader.seek(SeekFrom::Start(50)).await.unwrap();
        reader.seek(SeekFrom::Current(-10)).await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 60);
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        reader.seek(SeekFrom::Current(-10)).await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 100);
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        assert!(reader.seek(SeekFrom::Current(-11)).await.is_err());
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        reader.seek(SeekFrom::Current(90)).await.unwrap();
        assert_eq!(read_to_vec(&mut reader).await.len(), 0);
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        assert!(reader.seek(SeekFrom::Current(91)).await.is_err());
    });
}
