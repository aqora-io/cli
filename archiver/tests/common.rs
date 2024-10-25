use std::io::Write;

pub struct Crc32Writer(crc32fast::Hasher);

impl Write for Crc32Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Crc32Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finalize(self) -> u32 {
        self.0.finalize()
    }
}

impl Default for Crc32Writer {
    fn default() -> Self {
        Self(crc32fast::Hasher::new())
    }
}
