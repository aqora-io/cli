use aqora_data_utils::schema::marrow::datatypes::{DataType, Field};
use comfy_table::Table;

pub fn from_json_str_or_file<T>(string: &str) -> crate::error::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(string)
        .or(std::fs::File::open(string)
            .and_then(|f| serde_json::from_reader(f).map_err(std::io::Error::from)))
        .map_err(|err| {
            crate::error::user(
                &format!("Could not read or parse '{string}: {err}"),
                "Please provide a valid JSON file or string",
            )
        })
}

fn dictionary_fields(key: DataType, value: DataType) -> Vec<Field> {
    vec![
        Field {
            name: "key".into(),
            data_type: key,
            ..Default::default()
        },
        Field {
            name: "value".into(),
            data_type: value,
            ..Default::default()
        },
    ]
}

fn nested_field_table(
    type_name: &str,
    nullable: bool,
    fields: Vec<Field>,
    preset: &str,
) -> Result<(String, bool), ron::Error> {
    let mut table = Table::new();
    table.load_preset(preset);
    let has_nullable = fields_into_table(&mut table, fields)?;
    Ok((
        format!(
            "{}{}\n{}",
            type_name,
            if nullable { " *" } else { "" },
            table
        ),
        has_nullable,
    ))
}

pub fn fields_into_table(table: &mut Table, fields: Vec<Field>) -> Result<bool, ron::Error> {
    let fields = fields.into_iter().collect::<Vec<_>>();
    let preset = table.current_style_as_preset();
    let header = fields.iter().map(|f| f.name.to_owned()).collect::<Vec<_>>();
    let fields_nullable = fields.iter().map(|f| f.nullable).collect::<Vec<_>>();
    let (row, nested_nullable): (Vec<_>, Vec<_>) = fields
        .into_iter()
        .map(|f| {
            Ok(match f.data_type {
                DataType::Struct(fields) => {
                    nested_field_table("Struct", f.nullable, fields, &preset)?
                }
                DataType::List(field) => {
                    nested_field_table("List", f.nullable, vec![field.as_ref().clone()], &preset)?
                }
                DataType::LargeList(field) => nested_field_table(
                    "LargeList",
                    f.nullable,
                    vec![field.as_ref().clone()],
                    &preset,
                )?,
                DataType::FixedSizeList(field, size) => nested_field_table(
                    &format!("FixedSizeList({size})"),
                    f.nullable,
                    vec![field.as_ref().clone()],
                    &preset,
                )?,
                DataType::Map(field, sorted) => nested_field_table(
                    &format!("Map {}", if sorted { "(sorted)" } else { "" }),
                    f.nullable,
                    vec![field.as_ref().clone()],
                    &preset,
                )?,
                DataType::Dictionary(key, value) => nested_field_table(
                    "Dictionary",
                    f.nullable,
                    dictionary_fields(key.as_ref().clone(), value.as_ref().clone()),
                    &preset,
                )?,
                DataType::RunEndEncoded(run_ends, values) => nested_field_table(
                    "RunEndEncoded",
                    f.nullable,
                    vec![run_ends.as_ref().clone(), values.as_ref().clone()],
                    &preset,
                )?,
                DataType::Union(fields, mode) => nested_field_table(
                    &format!("Union ({mode})"),
                    f.nullable,
                    fields.into_iter().map(|(_, f)| f).collect(),
                    &preset,
                )?,
                dt => (
                    format!(
                        "{}{}",
                        ron::to_string(&dt)?,
                        if f.nullable { " *" } else { "" }
                    ),
                    false,
                ),
            })
        })
        .collect::<Result<Vec<_>, ron::Error>>()?
        .into_iter()
        .unzip();
    let has_nullable = fields_nullable
        .into_iter()
        .chain(nested_nullable)
        .any(|nullable| nullable);
    header.into_iter().zip(row).for_each(|(h, r)| {
        table.add_row([h, r]);
    });
    Ok(has_nullable)
}

// pub fn render_value_table(preset: &str, value: &Value) -> Result<String, ron::Error> {
//     match value {
//         Value::Seq(seq) => {
//             let mut table = Table::new();
//             table.load_preset(preset);
//             for item in seq {
//                 table.add_row([render_value_table(preset, item)?]);
//             }
//             Ok(format!("{table}"))
//         }
//         Value::Map(map) => {
//             let mut table = Table::new();
//             table.load_preset(preset);
//             for (key, value) in map.iter() {
//                 table.add_row([
//                     render_value_table(preset, key)?,
//                     render_value_table(preset, value)?,
//                 ]);
//             }
//             Ok(format!("{table}"))
//         }
//         _ => ron::to_string(&value),
//     }
// }
