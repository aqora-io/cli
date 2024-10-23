use crate::{
    error::Error,
    utils::{ArchiveKind, Compression, PathExt},
};
use ignore::DirEntry;
#[cfg(feature = "indicatif")]
use indicatif::ProgressBar;
use std::{
    fs::File,
    io::{self, Write},
    path::PathBuf,
};

#[derive(Debug)]
pub struct Archiver {
    input: PathBuf,
    output: PathBuf,
    #[cfg(feature = "indicatif")]
    progress_bar: Option<ProgressBar>,
}

impl Archiver {
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
        progress_bar: ProgressBar,
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

    fn find_input_paths(&self) -> Result<impl Iterator<Item = PathBuf>, ignore::Error> {
        Ok(ignore::WalkBuilder::new(&self.input)
            .hidden(false)
            .build()
            .skip(1)
            .map(|result| result.map(DirEntry::into_path))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|path| path.metadata().is_ok_and(|meta| meta.is_file())))
    }

    #[cfg(feature = "indicatif")]
    fn input_paths(&self) -> Result<Box<dyn Iterator<Item = PathBuf>>, ignore::Error> {
        use indicatif::ProgressIterator as _;

        Ok(if let Some(pb) = &self.progress_bar {
            let input_paths = self.find_input_paths()?.collect::<Vec<_>>();
            pb.reset();
            pb.set_length(input_paths.len() as u64);

            Box::new(input_paths.into_iter().progress_with(pb.clone()))
        } else {
            Box::new(self.find_input_paths()?)
        })
    }

    #[cfg(not(feature = "indicatif"))]
    fn input_paths(&self) -> Result<Box<dyn Iterator<Item = PathBuf>>, ignore::Error> {
        Ok(Box::new(self.find_input_paths()?))
    }

    fn create_tar<W: WriteFinish>(&self, writer: W) -> Result<(), Error> {
        let mut tar = tar::Builder::new(writer);

        for input_path in self.input_paths()? {
            let arch_path = input_path
                .strip_prefix(&self.input)
                .expect("not a prefix")
                .to_path_buf();
            tar.append_file(arch_path, &mut File::open(input_path)?)?;
        }

        tar.into_inner()?.finish()?;

        Ok(())
    }

    pub fn synchronously(self) -> Result<(), Error> {
        match self.output.archive_kind() {
            None => Err(Error::UnsupportedCompression),

            Some(ArchiveKind::Tar(compression)) => {
                let output_file = File::create(&self.output)?;
                match compression {
                    None => self.create_tar(NoWriteFinish(output_file)),

                    Some(Compression::Gzip) => self.create_tar(NoWriteFinish(
                        flate2::write::GzEncoder::new(output_file, flate2::Compression::default()),
                    )),

                    Some(Compression::Zstandard) => {
                        let n_workers = std::thread::available_parallelism()?.get() as u32;

                        #[cfg(feature = "tracing")]
                        tracing::debug!("n_workers = {}", n_workers);

                        let mut zst = zstd::stream::write::Encoder::new(
                            output_file,
                            zstd::DEFAULT_COMPRESSION_LEVEL,
                        )?;
                        zst.multithread(n_workers)?;

                        self.create_tar(ZstdWriteFinish(zst))
                    }
                }
            }

            Some(ArchiveKind::Zip) => todo!(),
        }
    }

    #[cfg(feature = "tokio")]
    pub async fn asynchronously(self, runtime: tokio::runtime::Handle) -> Result<(), Error> {
        runtime.spawn_blocking(move || self.synchronously()).await?
    }
}

// some writers need to be finished before being closed {{{
trait Finish {
    fn finish(self) -> io::Result<()>;
}

trait WriteFinish: Write + Finish {}

impl<W: Write + Finish> WriteFinish for W {}

// for writers that do not need to be finished {{{
struct NoWriteFinish<W: Write>(W);

impl<W: Write> Write for NoWriteFinish<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<W: Write> Finish for NoWriteFinish<W> {
    fn finish(self) -> io::Result<()> {
        Ok(())
    }
}
// }}}

// zstdmt needs to be finished {{{
struct ZstdWriteFinish<'a, W: Write>(zstd::stream::write::Encoder<'a, W>);

impl<W: Write> Write for ZstdWriteFinish<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<W: Write> Finish for ZstdWriteFinish<'_, W> {
    fn finish(self) -> io::Result<()> {
        self.0.finish()?.flush()
    }
}
// }}}
// }}}