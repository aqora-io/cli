use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use indicatif::ProgressBar;

pub struct IndicatifReader<R: Read> {
    reader: R,
    progress_bar: ProgressBar,
}

impl<R: Read> IndicatifReader<R> {
    pub fn new(reader: R, progress_bar: ProgressBar, length: u64) -> Self {
        progress_bar.set_position(0);
        progress_bar.set_length(length);
        // progress_bar.set_style(crate::progress_bar::pretty_bytes());
        Self {
            reader,
            progress_bar,
        }
    }
}

impl IndicatifReader<File> {
    pub fn for_file(mut file: File, progress_bar: ProgressBar) -> std::io::Result<Self> {
        let length = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self::new(file, progress_bar, length))
    }
}

impl<R: Read> Read for IndicatifReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader
            .read(buf)
            .inspect(|read| self.progress_bar.inc(*read as u64))
    }
}

impl<R: Read + Seek> Seek for IndicatifReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = self.reader.seek(pos)?;
        self.progress_bar.set_position(new_pos);
        Ok(new_pos)
    }
}
