use regex::Regex;

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
