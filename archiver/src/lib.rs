mod compress;
mod decompress;
mod error;
#[cfg(feature = "indicatif")]
mod indicatif;
mod utils;

pub use compress::Archiver;
pub use decompress::Unarchiver;
pub use error::Error;
pub use utils::{ArchiveKind, Compression};
