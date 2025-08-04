use wasm_bindgen_test::*;

use aqora_data_utils::wasm::{blob::vec_to_blob, error::set_console_error_panic_hook};
use std::io::SeekFrom;

fn new_blob(size: usize) -> web_sys::Blob {
    vec_to_blob(vec![0; size], 32, &web_sys::BlobPropertyBag::new()).unwrap()
}

fn read_to_vec<R>(reader: &mut R) -> Vec<u8>
where
    R: std::io::Read,
{
    let mut buffer = Vec::new();
    assert!(reader.read_to_end(&mut buffer).is_ok());
    buffer
}

#[wasm_bindgen_test]
pub fn test_blob_reader_read() {
    set_console_error_panic_hook();
    for size in [0, 1, 2, 32, 64, 128, 500, 1000, 10_000] {
        let mut reader = aqora_data_utils::wasm::io::BlobReader::new(new_blob(size));
        assert_eq!(read_to_vec(&mut reader).len(), size);
    }
}

#[wasm_bindgen_test]
pub fn test_blob_reader_seek() {
    use std::io::{Read, Seek};
    set_console_error_panic_hook();
    let mut reader = aqora_data_utils::wasm::io::BlobReader::new(new_blob(100));
    assert_eq!(read_to_vec(&mut reader).len(), 100);
    assert_eq!(read_to_vec(&mut reader).len(), 0);
    reader.rewind().unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 100);
    reader.seek(SeekFrom::Start(50)).unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 50);
    reader.seek(SeekFrom::End(-25)).unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 25);
    reader.seek(SeekFrom::Start(10)).unwrap();
    let mut buffer = vec![0; 10];
    reader.read_exact(&mut buffer).unwrap();
    reader.seek(SeekFrom::Current(10)).unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 70);
    reader.seek(SeekFrom::Start(50)).unwrap();
    reader.seek(SeekFrom::Current(-10)).unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 60);
    reader.seek(SeekFrom::Start(10)).unwrap();
    reader.seek(SeekFrom::Current(-10)).unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 100);
    reader.seek(SeekFrom::Start(10)).unwrap();
    assert!(reader.seek(SeekFrom::Current(-11)).is_err());
    reader.seek(SeekFrom::Start(10)).unwrap();
    reader.seek(SeekFrom::Current(90)).unwrap();
    assert_eq!(read_to_vec(&mut reader).len(), 0);
    reader.seek(SeekFrom::Start(10)).unwrap();
}

async fn async_read_to_vec<R>(reader: &mut R) -> Vec<u8>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut buffer = Vec::new();
    assert!(reader.read_to_end(&mut buffer).await.is_ok());
    buffer
}

#[wasm_bindgen_test]
pub async fn test_async_blob_reader_read() {
    set_console_error_panic_hook();
    for size in [0, 1, 2, 32, 64, 128, 500, 1000, 10_000] {
        let mut reader = aqora_data_utils::wasm::io::AsyncBlobReader::new(new_blob(size));
        assert_eq!(async_read_to_vec(&mut reader).await.len(), size);
    }
}

#[wasm_bindgen_test]
pub async fn test_async_blob_reader_seek() {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    set_console_error_panic_hook();
    let mut reader = aqora_data_utils::wasm::io::AsyncBlobReader::new(new_blob(100));
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 100);
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 0);
    reader.rewind().await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 100);
    reader.seek(SeekFrom::Start(50)).await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 50);
    reader.seek(SeekFrom::End(-25)).await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 25);
    reader.seek(SeekFrom::Start(10)).await.unwrap();
    let mut buffer = vec![0; 10];
    reader.read_exact(&mut buffer).await.unwrap();
    reader.seek(SeekFrom::Current(10)).await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 70);
    reader.seek(SeekFrom::Start(50)).await.unwrap();
    reader.seek(SeekFrom::Current(-10)).await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 60);
    reader.seek(SeekFrom::Start(10)).await.unwrap();
    reader.seek(SeekFrom::Current(-10)).await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 100);
    reader.seek(SeekFrom::Start(10)).await.unwrap();
    assert!(reader.seek(SeekFrom::Current(-11)).await.is_err());
    reader.seek(SeekFrom::Start(10)).await.unwrap();
    reader.seek(SeekFrom::Current(90)).await.unwrap();
    assert_eq!(async_read_to_vec(&mut reader).await.len(), 0);
    reader.seek(SeekFrom::Start(10)).await.unwrap();
}
