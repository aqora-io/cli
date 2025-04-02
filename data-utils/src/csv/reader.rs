use std::marker::PhantomData;
use std::{fmt, io};

use bytes::BytesMut;
use csv::ByteRecord;
use csv_core::{ReadRecordResult, Reader};
use serde::de::DeserializeOwned;

use crate::process::{ByteProcessResult, ByteProcessor, ProcessReadStream};

use super::CsvFormatChars;

pub type CsvReadStream<R, T> = ProcessReadStream<R, CsvProcessor<T>>;

const RECORD_BUFFER_SIZE: usize = 4096;
const MAX_RECORD_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
const ENDS_BUFFER_SIZE: usize = 128;
const MAX_ENDS_BUFFER_SIZE: usize = 1024;

#[derive(Default)]
pub struct CsvProcessor<T> {
    format: CsvFormatChars,
    reader: Reader,
    record_buffer: BytesMut,
    buffer_pos: usize,
    ends_buffer: Vec<usize>,
    end_pos: usize,
    ty: PhantomData<T>,
}

impl<T> CsvProcessor<T> {
    pub fn new(format: CsvFormatChars) -> Self {
        Self {
            reader: Reader::from(format),
            record_buffer: BytesMut::zeroed(RECORD_BUFFER_SIZE),
            buffer_pos: 0,
            ends_buffer: vec![0; ENDS_BUFFER_SIZE],
            end_pos: 0,
            format,
            ty: PhantomData,
        }
    }

    fn extend_record_buffer(&mut self) -> io::Result<()> {
        let new_len = self.record_buffer.len() * 2;
        if new_len > MAX_RECORD_BUFFER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record buffer size exceeded",
            ));
        }
        self.record_buffer.resize(new_len, 0);
        Ok(())
    }

    fn extend_ends_buffer(&mut self) -> io::Result<()> {
        let new_len = self.ends_buffer.len() * 2;
        if new_len > MAX_ENDS_BUFFER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ends buffer size exceeded",
            ));
        }
        self.ends_buffer.resize(new_len, 0);
        Ok(())
    }

    fn csv_process(&mut self, bytes: &[u8], consumed: &mut usize) -> ReadRecordResult {
        let (res, read, written, ends) = self.reader.read_record(
            bytes,
            &mut self.record_buffer[self.buffer_pos..],
            &mut self.ends_buffer[self.end_pos..],
        );
        *consumed += read;
        self.buffer_pos += written;
        self.end_pos += ends;
        res
    }

    fn take_record(&mut self) -> io::Result<Vec<&[u8]>> {
        let mut items = Vec::with_capacity(self.end_pos);
        let mut last_end = 0;
        for end in self.ends_buffer[..self.end_pos].iter().copied() {
            if end > self.buffer_pos {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "end position is greater than buffer position",
                ));
            } else if end < last_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "end positions out of order",
                ));
            } else {
                items.push(&self.record_buffer[last_end..end]);
                last_end = end;
            }
        }
        self.buffer_pos = 0;
        self.end_pos = 0;
        Ok(items)
    }
}

impl<T> fmt::Debug for CsvProcessor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvProcessor")
            .field("format", &self.format)
            .field("reader", &self.reader)
            .field("ty", &self.ty)
            .finish()
    }
}

impl<T> ByteProcessor for CsvProcessor<T>
where
    T: DeserializeOwned + fmt::Debug,
{
    type Item = T;
    type Error = csv::Error;
    fn process(
        &mut self,
        bytes: &[u8],
        is_eof: bool,
    ) -> ByteProcessResult<Self::Item, Self::Error> {
        let mut consumed = 0;
        let record = loop {
            let res = self.csv_process(bytes, &mut consumed);
            match res {
                ReadRecordResult::InputEmpty => {
                    return ByteProcessResult::NotReady(consumed);
                }
                ReadRecordResult::OutputFull => {
                    if let Err(err) = self.extend_record_buffer() {
                        return ByteProcessResult::Err(err.into());
                    }
                }
                ReadRecordResult::OutputEndsFull => {
                    if let Err(err) = self.extend_ends_buffer() {
                        return ByteProcessResult::Err(err.into());
                    }
                }
                ReadRecordResult::End => return ByteProcessResult::Done(consumed),
                ReadRecordResult::Record => {
                    if consumed < 1 || bytes.is_empty() {
                        return ByteProcessResult::Done(0);
                    } else if self
                        .format
                        .terminator
                        .is_some_and(|marker| bytes[consumed - 1] == marker)
                        || matches!(bytes[consumed - 1], b'\n' | b'\r')
                        || is_eof
                    {
                        match self.take_record() {
                            Ok(record) => {
                                break record;
                            }
                            Err(err) => {
                                return ByteProcessResult::Err(err.into());
                            }
                        }
                    } else {
                        return ByteProcessResult::NotReady(consumed);
                    }
                }
            }
        };
        match ByteRecord::from(record).deserialize::<T>(None) {
            Ok(record) => ByteProcessResult::Ok((0, consumed, record)),
            Err(err) => ByteProcessResult::Err(err),
        }
    }
}
