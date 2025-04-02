use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::File;

use crate::format::{FileKind, FormatReader};
use crate::write::AsyncPartWriter;

impl FormatReader<File> {
    pub async fn infer_path(
        path: impl AsRef<Path>,
        max_records: Option<usize>,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let file_kind = path
            .extension()
            .and_then(FileKind::from_ext)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "Extension does not match known formats",
                )
            })?;
        let file = File::open(path).await?;
        FormatReader::infer_format(file, file_kind, max_records).await
    }
}

pub async fn open(path: impl AsRef<Path>) -> io::Result<FormatReader<File>> {
    FormatReader::infer_path(path, Some(100)).await
}

lazy_static::lazy_static! {
    static ref PART_RE: regex::Regex = regex::Regex::new(r"\{part(:0(?<pad>\d+))?\}").unwrap();
}

fn tempfile_path() -> PathBuf {
    let tempdir = std::env::temp_dir();
    let mut bytes = [0u8; 16];
    rand::fill(&mut bytes);
    let filename = bytes
        .iter()
        .map(|x| format!("{x:x?}"))
        .collect::<Vec<_>>()
        .join("");
    tempdir.join(filename)
}

struct TemplatePart {
    index: usize,
    padding: usize,
}

impl TemplatePart {
    fn format_part(&self, num: usize) -> String {
        let mut num = num.to_string();
        if self.padding > num.len() {
            let mut padding = "0".repeat(self.padding - num.len());
            padding.push_str(&num);
            num = padding
        }
        num
    }
}

pub struct DirWriter {
    path: PathBuf,
    max_part_size: usize,
    template: (String, Vec<TemplatePart>),
    try_single: Option<PathBuf>,
}

impl DirWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            max_part_size: 1_000_000_000, // 1GB
            template: (
                ".parquet".to_string(),
                vec![TemplatePart {
                    index: 0,
                    padding: 3,
                }],
            ),
            try_single: None,
        }
    }

    pub fn with_try_single(self, try_single: impl AsRef<Path>) -> Self {
        Self {
            try_single: Some(try_single.as_ref().to_owned()),
            ..self
        }
    }

    pub fn without_try_single(self) -> Self {
        Self {
            try_single: None,
            ..self
        }
    }

    pub fn with_max_part_size(self, max_part_size: usize) -> Self {
        Self {
            max_part_size,
            ..self
        }
    }

    pub fn with_template(self, template: impl Into<String>) -> Self {
        let mut template: String = template.into();
        let mut indices = vec![];
        let mut removed = 0;
        for (start, end, pad) in PART_RE
            .captures_iter(&template)
            .filter_map(|capture| {
                let mat = capture.get(0)?;
                let pad = capture
                    .name("pad")
                    .map(|pad| pad.as_str().parse::<usize>())
                    .transpose()
                    .ok()?;
                let start = mat.start();
                let end = mat.end();
                if start > 0
                    && end < template.len()
                    && template.get(start - 1..start) == Some("{")
                    && template.get(end..end + 1) == Some("}")
                {
                    None
                } else {
                    Some((start, end, pad))
                }
            })
            .collect::<Vec<_>>()
        {
            template.replace_range(start - removed..end - removed, "");
            indices.push(TemplatePart {
                index: start - removed,
                padding: pad.unwrap_or_default(),
            });
            removed += end - start;
        }
        if indices.is_empty() {
            indices.push(TemplatePart {
                index: template.rfind('.').unwrap_or(template.len()),
                padding: 0,
            })
        }
        Self {
            template: (template, indices),
            ..self
        }
    }

    fn construct_part(&self, num: usize) -> PathBuf {
        let mut out = self.template.0.clone();
        for part in self.template.1.iter().rev() {
            out.insert_str(part.index, &part.format_part(num));
        }
        self.path.join(out)
    }

    pub fn part_path(&self, num: usize) -> PathBuf {
        if let Some(path) = self.try_single.as_deref() {
            if num == 0 {
                path.to_owned()
            } else {
                self.construct_part(num)
            }
        } else {
            self.construct_part(num)
        }
    }
}

#[async_trait::async_trait]
impl<'a> AsyncPartWriter<'a> for DirWriter {
    type Writer = File;

    async fn create_part(&'a mut self, num: usize) -> io::Result<Self::Writer> {
        if let Some(path) = self.try_single.as_deref() {
            match num {
                0 => Ok(File::create(path).await?),
                1 => {
                    let tempfile = tempfile_path();
                    tokio::fs::rename(&path, &tempfile).await?;
                    if let Err(err) = tokio::fs::create_dir_all(&self.path).await {
                        let _ = tokio::fs::rename(&tempfile, &path).await;
                        return Err(err);
                    }
                    if let Err(err) = tokio::fs::rename(&tempfile, self.construct_part(0)).await {
                        let _ = tokio::fs::rename(&tempfile, &path).await;
                        return Err(err);
                    }
                    Ok(File::create(self.construct_part(1)).await?)
                }
                num => Ok(File::create(self.construct_part(num)).await?),
            }
        } else {
            match num {
                0 => {
                    tokio::fs::create_dir_all(&self.path).await?;
                    Ok(File::create(self.construct_part(0)).await?)
                }
                num => Ok(File::create(self.construct_part(num)).await?),
            }
        }
    }

    fn max_part_size(&self) -> Option<usize> {
        Some(self.max_part_size)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_template() {
        let writer = DirWriter::new("test_dir").with_template("cool-{part:03}.parquet");
        assert_eq!(
            writer.construct_part(1),
            Path::new("test_dir/cool-001.parquet")
        );
        let writer = DirWriter::new("test_dir");
        assert_eq!(writer.construct_part(1), Path::new("test_dir/1.parquet"));
        let writer = DirWriter::new("test_dir").with_template("template");
        assert_eq!(writer.construct_part(1), Path::new("test_dir/template1"));
        let writer = DirWriter::new("test_dir").with_template("template.parquet");
        assert_eq!(
            writer.construct_part(1),
            Path::new("test_dir/template1.parquet")
        );
        let writer = DirWriter::new("test_dir").with_template("cool-{{part}}.parquet");
        assert_eq!(
            writer.construct_part(1),
            Path::new("test_dir/cool-{{part}}1.parquet")
        );
        let writer = DirWriter::new("test_dir").with_template("cool-{part}-{part}.parquet");
        assert_eq!(
            writer.construct_part(1),
            Path::new("test_dir/cool-1-1.parquet")
        );
    }
}
