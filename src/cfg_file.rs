use tokio::io::{self, AsyncBufRead, AsyncBufReadExt};

fn parse_cfg_line(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(index) = trimmed.find('=') {
        let key = trimmed[..index].trim();
        let value = trimmed[index + 1..].trim();
        Some((key, value))
    } else {
        None
    }
}

pub async fn read_cfg_file_key(
    input: impl AsyncBufRead + Unpin,
    key: impl AsRef<str>,
) -> io::Result<Option<String>> {
    let search = key.as_ref();
    let mut lines = input.lines();
    let mut value = None;
    while let Some(line) = lines.next_line().await? {
        match parse_cfg_line(&line) {
            Some((key, new_value)) if key == search => value = Some(new_value.to_string()),
            _ => (),
        }
    }
    Ok(value)
}
