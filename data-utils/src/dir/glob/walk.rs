use std::collections::HashSet;
use std::path::PathBuf;

use super::error::GlobError;
use super::regex::Matches;
use super::GlobPath;

pub struct GlobPathWalk {
    inner: GlobPath,
    paths: Vec<PathBuf>,
    visited: HashSet<PathBuf>,
}

impl GlobPathWalk {
    fn process(&mut self, path: PathBuf) -> Result<Option<(PathBuf, Matches<'_>)>, GlobError> {
        if !self.visited.insert(path.canonicalize()?) {
            return Ok(None);
        }
        // std::fs::metadata follows symlinks so check on symlink is not necessary
        let metadata = std::fs::metadata(&path)?;
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path)? {
                let entry_path = entry?.path();
                if self.inner.maybe_matches(&entry_path)? {
                    self.paths.push(entry_path);
                }
            }
        } else if metadata.is_file() {
            if let Some(mat) = self.inner.matches(&path)? {
                return Ok(Some((path, mat)));
            }
        }
        Ok(None)
    }
}

impl Iterator for GlobPathWalk {
    type Item = Result<(PathBuf, Vec<(String, Option<String>)>), GlobError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let path = self.paths.pop()?;
            match self.process(path) {
                Ok(Some((path, mat))) => {
                    let mat_owned = mat.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
                    return Some(Ok((path, mat_owned)));
                }
                Err(err) => return Some(Err(err)),
                Ok(None) => continue,
            };
        }
    }
}

impl GlobPath {
    pub fn walk(self) -> GlobPathWalk {
        GlobPathWalk {
            paths: vec![self.root.clone()],
            inner: self,
            visited: HashSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn glob_path_walk_simple() {
        let glob = GlobPath::from_str(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/files/dir/simple/{split}/{animal}/{name}.json"
        ))
        .unwrap();
        for item in glob.walk() {
            println!("{:?}", item.unwrap());
        }
    }
}
