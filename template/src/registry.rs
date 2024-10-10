use std::io::Write;
use std::path::Path;

use handlebars::{
    Context, Handlebars, Helper, HelperResult, Output, RenderContext, RenderError,
    RenderErrorReason,
};
use rust_embed::RustEmbed;
use serde::Serialize;
use serde_json::Value as JsonValue;
use toml::Value as TomlValue;

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

#[derive(RustEmbed)]
#[folder = "assets"]
pub struct Assets;

fn json_value_to_toml_value(json: &JsonValue) -> Option<TomlValue> {
    Some(match json {
        JsonValue::Null => return None,
        JsonValue::Bool(b) => TomlValue::Boolean(*b),
        JsonValue::String(s) => TomlValue::String(s.clone()),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                TomlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                TomlValue::Float(f)
            } else {
                return None;
            }
        }
        JsonValue::Array(a) => TomlValue::Array(
            a.iter()
                .map(json_value_to_toml_value)
                .collect::<Option<_>>()?,
        ),
        JsonValue::Object(m) => TomlValue::Table(
            m.iter()
                .map(|(s, v)| Some((s.clone(), json_value_to_toml_value(v)?)))
                .collect::<Option<_>>()?,
        ),
    })
}

fn toml_val(
    h: &Helper<'_>,
    _: &Handlebars<'_>,
    _: &Context,
    _: &mut RenderContext<'_, '_>,
    out: &mut dyn Output,
) -> HelperResult {
    let value = json_value_to_toml_value(
        h.param(0)
            .ok_or_else(|| RenderErrorReason::ParamNotFoundForIndex("toml_val", 0))?
            .value(),
    )
    .ok_or_else(|| RenderErrorReason::InvalidParamType("TOML value"))?;
    out.write(value.to_string().as_str())?;
    Ok(())
}

pub struct Registry {
    handlebars: Handlebars<'static>,
}

impl Registry {
    pub fn new() -> Self {
        let mut handlebars = Handlebars::new();
        #[cfg(debug_assertions)]
        handlebars.set_dev_mode(true);
        handlebars
            .register_embed_templates_with_extension::<Assets>(".hbs")
            .unwrap();
        handlebars.register_helper("toml_val", Box::new(toml_val));
        Self { handlebars }
    }

    pub fn render_static<W: Write>(&self, path: &str, mut writer: W) -> Result<(), RenderError> {
        writer.write_all(
            &Assets::get(path)
                .ok_or_else(|| RenderErrorReason::TemplateNotFound(path.to_string()))?
                .data,
        )?;
        Ok(())
    }

    pub fn render_template<W: Write, D: Serialize>(
        &self,
        path: &str,
        data: &D,
        writer: W,
    ) -> Result<(), RenderError> {
        self.handlebars.render_to_write(path, data, writer)?;
        Ok(())
    }

    pub fn render_all<D: Serialize>(
        &self,
        prefix: &str,
        data: &D,
        out: impl AsRef<Path>,
    ) -> Result<(), RenderError> {
        let out = out.as_ref();
        std::fs::create_dir_all(out)?;

        let prefix_path = Path::new(prefix);

        let open_relative = |relative_path: &Path| {
            let out_path = out.join(relative_path);
            std::fs::create_dir_all(out_path.parent().unwrap())?;
            std::fs::File::create(out_path)
        };

        for entry in Assets::iter() {
            let entry_path = Path::new(entry.as_ref());
            if entry_path
                .extension()
                .map(|ext| ext == "hbs")
                .unwrap_or(false)
            {
                continue;
            }
            if let Ok(relative_path) = entry_path.strip_prefix(prefix_path) {
                self.render_static(&entry, &mut open_relative(relative_path)?)?;
            }
        }

        for entry in self.handlebars.get_templates().keys() {
            let entry_path = Path::new(entry);
            if let Ok(relative_path) = entry_path.strip_prefix(prefix_path) {
                self.render_template(entry, data, &mut open_relative(relative_path)?)?;
            }
        }

        Ok(())
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
