use crate::{
    error::{Error, Result},
    utils::{ArchiveKind, Compression, PathExt},
};
#[cfg(feature = "indicatif")]
use indicatif::ProgressBar;
use std::{
    fs::{self, File},
    io::{self, Read, Seek},
    path::PathBuf,
};

#[derive(Debug)]
pub struct Unarchiver {
    input: PathBuf,
    output: PathBuf,
    source_kind: Option<ArchiveKind>,

    #[cfg(feature = "indicatif")]
    progress_bar: Option<ProgressBar>,
}

impl Unarchiver {
    pub fn new(input: PathBuf, output: PathBuf) -> Self {
        Self {
            input,
            output,
            source_kind: None,

            #[cfg(feature = "indicatif")]
            progress_bar: None,
        }
    }

    pub fn with_source_kind(self, source_kind: ArchiveKind) -> Self {
        Self {
            source_kind: Some(source_kind),
            ..self
        }
    }

    pub fn without_source_kind(self) -> Self {
        Self {
            source_kind: None,
            ..self
        }
    }

    #[cfg(feature = "indicatif")]
    pub fn with_progress_bar(self, progress_bar: ProgressBar) -> Self {
        Self {
            progress_bar: Some(progress_bar),
            ..self
        }
    }

    #[cfg(feature = "indicatif")]
    pub fn without_progress_bar(self) -> Self {
        Self {
            progress_bar: None,
            ..self
        }
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

    #[cfg(feature = "indicatif")]
    fn create_readseeker(&self) -> io::Result<Box<dyn ReadSeek>> {
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

    #[cfg(not(feature = "indicatif"))]
    fn create_readseeker(&self) -> io::Result<Box<dyn ReadSeek>> {
        Ok(Box::new(File::open(&self.input)?))
    }

    pub fn synchronously(self) -> Result<()> {
        match self.source_kind.or_else(|| self.input.archive_kind()) {
            None => Err(Error::UnsupportedCompression),

            Some(ArchiveKind::Tar(compression)) => {
                let input_file: Box<dyn Read> = match compression {
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

            Some(ArchiveKind::Zip) => {
                let mut zip = zip::read::ZipArchive::new(self.create_readseeker()?)?;
                for i in 0..zip.len() {
                    let mut src_file = zip.by_index(i)?;
                    let dst_path = self.output.join(src_file.mangled_name());
                    fs::create_dir_all(dst_path.parent().expect("dest path had no parent"))?;
                    let mut dst_file = File::create(dst_path)?;
                    io::copy(&mut src_file, &mut dst_file)?;
                }
                Ok(())
            }
        }
    }

    #[cfg(feature = "tokio")]
    pub async fn asynchronously(self, runtime: tokio::runtime::Handle) -> Result<()> {
        runtime.spawn_blocking(move || self.synchronously()).await?
    }
}

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}
