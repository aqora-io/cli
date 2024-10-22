use crate::{
    error::Error,
    utils::{ArchiveKind, Compression, PathExt},
};
use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
};

#[derive(Debug)]
pub struct Unarchiver {
    input: PathBuf,
    output: PathBuf,
    #[cfg(feature = "indicatif")]
    progress_bar: Option<indicatif::ProgressBar>,
}

impl Unarchiver {
    #[cfg(feature = "indicatif")]
    pub fn new(input: PathBuf, output: PathBuf) -> Self {
        Self {
            input,
            output,
            progress_bar: None,
        }
    }

    #[cfg(feature = "indicatif")]
    pub fn new_with_progress_bar(
        input: PathBuf,
        output: PathBuf,
        progress_bar: indicatif::ProgressBar,
    ) -> Self {
        Self {
            input,
            output,
            progress_bar: Some(progress_bar),
        }
    }

    #[cfg(not(feature = "indicatif"))]
    pub fn new(input: PathBuf, output: PathBuf) -> Self {
        Self { input, output }
    }

    #[cfg(feature = "indicatif")]
    fn create_reader(&self) -> io::Result<Box<dyn Read>> {
        if let Some(pb) = &self.progress_bar {
            Ok(Box::new(crate::indicatif::IndicatifReader::for_file(
                File::open(&self.input)?,
                pb.clone(),
            )?))
        } else {
            Ok(Box::new(File::open(&self.input)?))
        }
    }

    #[cfg(not(feature = "indicatif"))]
    fn create_reader(&self) -> io::Result<Box<dyn Read>> {
        Ok(Box::new(File::open(&self.input)?))
    }

    pub fn synchronously(self) -> Result<(), Error> {
        match self.input.archive_kind() {
            None => Err(Error::UnsupportedCompression),

            Some(ArchiveKind::Tar(compression)) => {
                let input_file: Box<dyn std::io::Read> = match compression {
                    None => self.create_reader()?,
                    Some(Compression::Gzip) => {
                        Box::new(flate2::read::MultiGzDecoder::new(self.create_reader()?))
                    }
                    Some(Compression::Zstandard) => {
                        Box::new(zstd::stream::read::Decoder::new(self.create_reader()?)?)
                    }
                };
                let mut tar = tar::Archive::new(input_file);

                for tar_entry in tar.entries()? {
                    let mut tar_entry = tar_entry?;

                    if !tar_entry.unpack_in(&self.output)? {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("{:?} was not unpacked", tar_entry.path());
                    }
                }

                Ok(())
            }

            Some(ArchiveKind::Zip) => todo!(),
        }
    }

    #[cfg(feature = "tokio")]
    pub async fn asynchronously(self, runtime: tokio::runtime::Handle) -> Result<(), Error> {
        runtime.spawn_blocking(move || self.synchronously()).await?
    }
}
