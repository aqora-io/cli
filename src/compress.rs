use crate::error::{self, Result};
use std::path::Path;

pub fn compress(
    input: impl AsRef<Path>,
    tar_path: impl AsRef<Path>,
    output_file: impl AsRef<Path>,
) -> Result<()> {
    let input = input.as_ref();
    let output_file = output_file.as_ref();
    let mut file = std::fs::File::create(output_file).map_err(|err| {
        error::user(
            &format!("Could not create {}: {}", output_file.display(), err),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    let mut gz = flate2::GzBuilder::new().write(&mut file, Default::default());
    let mut tar = tar::Builder::new(&mut gz);
    tar.append_dir_all(tar_path, input).map_err(|err| {
        error::user(
            &format!(
                "Could not add data contents to tar from {}: {}",
                input.display(),
                err
            ),
            "Please make sure the data directory exists and you have permission to read it",
        )
    })?;
    tar.finish()
        .map_err(|err| error::system(&format!("Could not finish tar: {}", err), ""))?;
    drop(tar);
    gz.finish()
        .map_err(|err| error::system(&format!("Could not finish gz: {}", err), ""))?;
    Ok(())
}
