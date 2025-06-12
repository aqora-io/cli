pub type DefaultChecksum = crc32fast::Hasher;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    Crc32,
}

pub trait Checksum {
    fn algo(&self) -> ChecksumAlgorithm;
    fn digest(&self, bytes: &[u8]) -> Vec<u8>;
}

impl<T> Checksum for &T
where
    T: ?Sized + Checksum,
{
    fn algo(&self) -> ChecksumAlgorithm {
        T::algo(self)
    }
    fn digest(&self, bytes: &[u8]) -> Vec<u8> {
        T::digest(self, bytes)
    }
}

impl Checksum for crc32fast::Hasher {
    #[inline]
    fn algo(&self) -> ChecksumAlgorithm {
        ChecksumAlgorithm::Crc32
    }
    #[inline]
    fn digest(&self, bytes: &[u8]) -> Vec<u8> {
        let mut hasher = self.clone();
        hasher.update(bytes);
        hasher.finalize().to_be_bytes().into()
    }
}
