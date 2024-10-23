use aqora_archiver::{ArchiveKind, Compression};

#[test]
fn test_parse_compression() {
    assert_eq!(Compression::Zstandard, "zst".parse().unwrap());
    assert_eq!(Compression::Gzip, "gz".parse().unwrap());
}

#[test]
fn test_parse_archive_kind() {
    assert_eq!(
        ArchiveKind::Tar(Some(Compression::Zstandard)),
        "hello_kitty.tar.zst".parse().unwrap()
    );
    assert_eq!(
        ArchiveKind::Tar(Some(Compression::Gzip)),
        "hello_kitty.tar.gz".parse().unwrap()
    );
    assert_eq!(ArchiveKind::Tar(None), "hello_kitty.tar".parse().unwrap());
    assert_eq!(ArchiveKind::Zip, "hello_kitty.zip".parse().unwrap());
    assert_eq!(Option::<ArchiveKind>::None, "hello_kitty.txt".parse().ok());
    assert_eq!(Option::<ArchiveKind>::None, "hello_kitty".parse().ok());
    assert_eq!(Option::<ArchiveKind>::None, "".parse().ok());
}
