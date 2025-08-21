use std::fmt;
use std::str::FromStr;

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag},
    character::{
        complete::{char, one_of},
        none_of,
    },
    combinator::{complete, map, not},
    error::{context, ContextError, ErrorKind, ParseError},
    multi::{many1, separated_list1},
    sequence::delimited,
    Err, IResult, Parser,
};

use super::ast::{Expr, Group, Segment};

const ESCAPE_CHAR: char = '\\';
const SPECIAL_CHARS: &str = r"\/{}()|*";

fn escape(str: impl AsRef<str>) -> String {
    let mut escaped = String::with_capacity(str.as_ref().len());
    for c in str.as_ref().chars() {
        if SPECIAL_CHARS.contains(c) {
            escaped.push(ESCAPE_CHAR);
        }
        escaped.push(c);
    }
    escaped
}

fn unescape(str: impl AsRef<str>) -> String {
    let mut unescaped = String::with_capacity(str.as_ref().len());
    let mut chars = str.as_ref().chars().peekable();
    while let Some(c) = chars.next() {
        if c == ESCAPE_CHAR {
            if let Some(&next) = chars.peek() {
                if SPECIAL_CHARS.contains(next) {
                    unescaped.push(next);
                    chars.next(); // consume the special char
                    continue;
                }
            }
            // If not a special char, keep the backslash
            unescaped.push(c);
        } else {
            unescaped.push(c);
        }
    }
    unescaped
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tag(tag) => f.write_str(&escape(tag)),
            Self::Named(name) => f.write_str(&format!("{{{}}}", escape(name))),
            Self::Either(groups) => {
                f.write_str("(")?;
                let mut groups = groups.iter().peekable();
                while let Some(group) = groups.next() {
                    group.fmt(f)?;
                    if groups.peek().is_some() {
                        f.write_str("|")?;
                    }
                }
                f.write_str(")")
            }
            Self::Star => f.write_str("*"),
        }
    }
}

impl fmt::Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for expr in &self.exprs {
            expr.fmt(f)?;
        }
        Ok(())
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Match(group) => group.fmt(f),
            Self::DoubleStar => f.write_str("**"),
        }
    }
}

fn chars1<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let (rest, out) = escaped(none_of(SPECIAL_CHARS), ESCAPE_CHAR, one_of(SPECIAL_CHARS))(i)?;
    if out.is_empty() {
        Err(Err::Error(E::from_error_kind(i, ErrorKind::Many1)))
    } else {
        Ok((rest, out))
    }
}

fn tag_expr<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, &'a str, E> {
    context("tag", chars1).parse(i)
}

fn named_expr<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, &'a str, E> {
    context("named", delimited(char('{'), chars1, char('}'))).parse(i)
}

fn star_expr<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, char, E> {
    let (rest, star) = context("star", char('*')).parse(i)?;
    not(char('*')).parse(rest)?;
    Ok((rest, star))
}

fn expr<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, Expr, E> {
    context(
        "expr",
        alt((
            map(tag_expr, |s| Expr::Tag(unescape(s))),
            map(named_expr, |s| Expr::Named(unescape(s))),
            map(star_expr, |_| Expr::Star),
            map(
                delimited(char('('), separated_list1(char('|'), group), char(')')),
                Expr::Either,
            ),
        )),
    )
    .parse(i)
}

fn group<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, Group, E> {
    context("group", map(many1(expr), |exprs| Group { exprs })).parse(i)
}

fn segment<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, Segment, E> {
    context(
        "segment",
        alt((
            map(group, Segment::Match),
            tag("**").map(|_| Segment::DoubleStar),
        )),
    )
    .parse(i)
}

impl FromStr for Segment {
    type Err = Err<(String, ErrorKind)>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_, segment) = complete(segment::<(&str, ErrorKind)>)
            .parse(s)
            .map_err(|err| err.to_owned())?;
        Ok(segment)
    }
}

pub fn is_tag_expr(segment: &str) -> bool {
    complete(tag_expr::<(&str, ErrorKind)>)
        .parse(segment)
        .is_ok()
}
