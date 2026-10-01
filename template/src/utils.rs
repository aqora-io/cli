use regex::Regex;

/// Shared by [`crate::WorkspaceTemplate`] and [`crate::DatasetMarimoTemplate`]:
/// kubimo's image pre-builds a Python 3.12 environment for the scaffolded
/// notebook's PEP 723 header.
pub const DEFAULT_PYTHON_VERSION: &str = "3.12";

pub trait OptionExt<T> {
    fn flat_ref(&self) -> Option<&T>;
}

impl<T> OptionExt<T> for Option<Option<T>> {
    fn flat_ref(&self) -> Option<&T> {
        self.as_ref().and_then(|o| o.as_ref())
    }
}

#[inline]
pub fn is_semver(string: &str) -> bool {
    lazy_static::lazy_static! {
        static ref SEMVER_REGEX: Regex = Regex::new(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$").unwrap();
    }
    SEMVER_REGEX.is_match(string)
}

#[inline]
pub fn assert_semver(string: &str) -> Result<(), String> {
    if !is_semver(string) {
        return Err(format!("Invalid semver: {}", string));
    }
    Ok(())
}

#[inline]
pub fn is_python_minor(string: &str) -> bool {
    lazy_static::lazy_static! {
        static ref PYTHON_MINOR_REGEX: Regex = Regex::new(r"^3\.[0-9]+$").unwrap();
    }
    PYTHON_MINOR_REGEX.is_match(string)
}

#[inline]
pub fn assert_python_minor(string: &str) -> Result<(), String> {
    if !is_python_minor(string) {
        return Err(format!("Invalid Python version: {}", string));
    }
    Ok(())
}

#[inline]
pub fn is_slug(string: &str) -> bool {
    lazy_static::lazy_static! {
        static ref SLUG_REGEX: Regex = Regex::new(r"^[-a-zA-Z0-9_]*$").unwrap();
    }
    SLUG_REGEX.is_match(string)
}

#[inline]
pub fn assert_slug(string: &str) -> Result<(), String> {
    if !is_slug(string) {
        return Err(format!("Invalid slug: {}", string));
    }
    Ok(())
}

#[inline]
pub fn is_username(string: &str) -> bool {
    lazy_static::lazy_static! {
        static ref USERNAME_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9_]*$").unwrap();
    }
    USERNAME_REGEX.is_match(string)
}

#[inline]
pub fn assert_username(string: &str) -> Result<(), String> {
    if !is_username(string) {
        return Err(format!("Invalid username: {}", string));
    }
    Ok(())
}

#[inline]
pub fn has_control_chars(string: &str) -> bool {
    string.contains(|c: char| c.is_control())
}

#[inline]
pub fn assert_no_control_chars(string: &str) -> Result<(), String> {
    if has_control_chars(string) {
        return Err(format!("String contains control characters: {}", string));
    }
    Ok(())
}

/// Rejects text that cannot be embedded in a Python raw triple-quoted string.
///
/// Free-text display names are interpolated into `mo.md(r"""…""")` in the generated
/// `readme.py`, and the registry renders with `no_escape`, so the value reaches the file
/// verbatim. A `"""` closes the literal early; a trailing backslash escapes the closing
/// quote even in a raw string. Either produces a `readme.py` that is a Python syntax
/// error — and since that is the workspace's landing notebook, the workspace would be
/// born unopenable.
pub fn assert_python_raw_string_safe(string: &str) -> Result<(), String> {
    assert_no_control_chars(string)?;
    if string.contains("\"\"\"") {
        return Err(format!(
            "String contains a triple quote, which would end the generated Python string: {string}"
        ));
    }
    if string.ends_with('\\') {
        return Err(format!(
            "String ends with a backslash, which would escape the closing quote: {string}"
        ));
    }
    Ok(())
}

/// Test-only: extracts a PEP 723 inline script metadata block (`# /// script` … `# ///`)
/// from a rendered notebook and parses its body as TOML.
#[cfg(test)]
pub(crate) fn extract_pep723_toml(content: &str) -> toml::Value {
    lazy_static::lazy_static! {
        static ref PEP723_REGEX: Regex =
            Regex::new(r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$").unwrap();
    }
    let captures = PEP723_REGEX
        .captures(content)
        .expect("no PEP 723 block found");
    assert_eq!(&captures["type"], "script");
    let toml_source = captures["content"]
        .lines()
        .map(|line| {
            line.strip_prefix("# ")
                .or_else(|| line.strip_prefix('#'))
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n");
    toml::from_str(&toml_source).expect("PEP 723 block is not valid TOML")
}
