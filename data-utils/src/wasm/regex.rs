use std::collections::HashSet;
use std::convert::Infallible;

use wasm_bindgen::prelude::*;

pub fn js_to_rust_regex(value: &JsValue) -> Result<regex::Regex, JsError> {
    let (mut expr, flags) = match value.dyn_ref::<js_sys::RegExp>() {
        Some(re) => (String::from(re.source()), String::from(re.flags())),
        None => match value.dyn_ref::<js_sys::JsString>() {
            Some(str) => (String::from(str), Default::default()),
            None => {
                return Err(JsError::new(&format!(
                    "incompatible JS value {value:?} for type RegExp | string",
                )))
            }
        },
    };
    if !flags.is_empty() {
        expr = format!("(?{flags}){expr}");
    }
    Ok(regex::Regex::new(&expr)?)
}

struct FoundFlags {
    end: usize,
    flags: HashSet<char>,
}

impl From<&regex_syntax::ast::SetFlags> for FoundFlags {
    fn from(value: &regex_syntax::ast::SetFlags) -> Self {
        use regex_syntax::ast::{Flag::*, FlagsItemKind};
        let end = value.span.end.offset;
        let mut flags = HashSet::new();
        let mut is_negation = false;
        for item in &value.flags.items {
            let flag = match item.kind {
                FlagsItemKind::Negation => {
                    is_negation = !is_negation;
                    continue;
                }
                FlagsItemKind::Flag(flag) => match flag {
                    CaseInsensitive => 'i',
                    MultiLine => 'm',
                    DotMatchesNewLine => 's',
                    SwapGreed => 'U',
                    Unicode => 'u',
                    CRLF => 'R',
                    IgnoreWhitespace => 'x',
                },
            };
            if is_negation {
                flags.remove(&flag);
            } else {
                flags.insert(flag);
            }
        }
        FoundFlags { end, flags }
    }
}

#[derive(Default)]
struct FlagsVisitor {
    found: Option<FoundFlags>,
}

impl regex_syntax::ast::Visitor for FlagsVisitor {
    type Output = Option<FoundFlags>;
    type Err = Infallible;
    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(self.found)
    }
    fn visit_pre(&mut self, ast: &regex_syntax::ast::Ast) -> Result<(), Self::Err> {
        if self.found.is_some() {
            return Ok(());
        }
        if let regex_syntax::ast::Ast::Flags(flags) = ast {
            if flags.span.start.offset == 0 {
                self.found = Some(flags.as_ref().into());
            }
        }
        Ok(())
    }
}

pub fn rust_to_js_regex(re: &regex::Regex) -> Result<JsValue, JsError> {
    let re_str = re.to_string();
    let ast = regex_syntax::ast::parse::Parser::new().parse(&re_str)?;
    if let Some(flags) = regex_syntax::ast::visit(&ast, FlagsVisitor::default()).unwrap() {
        Ok(js_sys::RegExp::new(
            &re_str[flags.end..],
            &flags.flags.iter().collect::<String>(),
        )
        .into())
    } else {
        Ok(js_sys::RegExp::new(&re_str, "").into())
    }
}
