use aqora_client::{
    checksum::{crc32fast::Crc32, Checksum, S3ChecksumLayer},
    http::HttpArcLayer,
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(TS, Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export, rename = "Checksum")]
pub enum JsChecksum {
    #[cfg(feature = "aqora-client-crc32fast")]
    Crc32,
}

type BoxChecksum = Box<dyn Checksum>;

impl From<JsChecksum> for BoxChecksum {
    fn from(value: JsChecksum) -> Self {
        match value {
            #[cfg(feature = "aqora-client-crc32fast")]
            JsChecksum::Crc32 => Box::new(Crc32::new()),
        }
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
#[ts(export, rename = "S3ChecksumOptions")]
pub struct S3ChecksumOptions {
    pub algo: JsChecksum,
}

impl S3ChecksumOptions {
    pub fn into_arc_layer(self) -> HttpArcLayer {
        HttpArcLayer::new(S3ChecksumLayer::new(BoxChecksum::from(self.algo)))
    }
}
