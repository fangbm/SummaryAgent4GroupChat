//! Format-preserving agent.toml document access built on `toml_edit`.
//!
//! Moved verbatim from main.rs.

use anyhow::{bail, Context, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;
use toml_edit::{value, Array, DocumentMut, InlineTable, Item, Table, Value as TomlValue};
pub(crate) fn table<'a>(doc: &'a DocumentMut, name: &str) -> Option<&'a Table> {
    doc.get(name).and_then(Item::as_table)
}

pub(crate) fn table_mut<'a>(doc: &'a mut DocumentMut, name: &str) -> &'a mut Table {
    if !matches!(doc.get(name), Some(Item::Table(_))) {
        doc[name] = Item::Table(Table::new());
    }
    doc[name].as_table_mut().expect("table must exist")
}

pub(crate) fn disabled_image_summary_rooms_from_doc(doc: &DocumentMut) -> String {
    let mut rooms = table(doc, "room_capabilities")
        .into_iter()
        .flat_map(|capabilities| capabilities.iter())
        .filter_map(|(room, item)| {
            let enabled = item
                .as_inline_table()
                .and_then(|entry| entry.get("image_summary_enabled"))
                .and_then(TomlValue::as_bool)
                .or_else(|| {
                    item.as_table()
                        .and_then(|entry| entry.get("image_summary_enabled"))
                        .and_then(Item::as_bool)
                });
            (enabled == Some(false)).then_some(room.to_string())
        })
        .collect::<Vec<_>>();
    rooms.sort();
    join_lines(&rooms)
}

pub(crate) fn set_disabled_image_summary_rooms(doc: &mut DocumentMut, rooms: &[String]) {
    let disabled = rooms
        .iter()
        .map(|room| room.trim())
        .filter(|room| !room.is_empty())
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let capabilities = table_mut(doc, "room_capabilities");
    let existing_rooms = capabilities
        .iter()
        .map(|(room, _)| room.to_string())
        .collect::<Vec<_>>();

    for room in existing_rooms {
        if disabled.contains(&room) {
            continue;
        }
        let mut remove_room = false;
        if let Some(item) = capabilities.get_mut(&room) {
            if let Some(entry) = item.as_inline_table_mut() {
                entry.remove("image_summary_enabled");
                remove_room = entry.is_empty();
            } else if let Some(entry) = item.as_table_mut() {
                entry.remove("image_summary_enabled");
                remove_room = entry.is_empty();
            }
        }
        if remove_room {
            capabilities.remove(&room);
        }
    }

    for room in disabled {
        if let Some(item) = capabilities.get_mut(&room) {
            if let Some(entry) = item.as_inline_table_mut() {
                entry.insert("image_summary_enabled", TomlValue::from(false));
                continue;
            }
            if let Some(entry) = item.as_table_mut() {
                entry["image_summary_enabled"] = value(false);
                continue;
            }
        }
        let mut entry = InlineTable::new();
        entry.insert("image_summary_enabled", TomlValue::from(false));
        capabilities.insert(&room, Item::Value(TomlValue::InlineTable(entry)));
    }
}

pub(crate) fn get_str(doc: &DocumentMut, table_name: &str, key: &str, default: &str) -> String {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_str)
        .unwrap_or(default)
        .to_string()
}

pub(crate) fn get_bool(doc: &DocumentMut, table_name: &str, key: &str, default: bool) -> bool {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_bool)
        .unwrap_or(default)
}

pub(crate) fn get_i64(doc: &DocumentMut, table_name: &str, key: &str, default: i64) -> i64 {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_integer)
        .unwrap_or(default)
}

pub(crate) fn get_u64(doc: &DocumentMut, table_name: &str, key: &str, default: u64) -> u64 {
    get_i64(doc, table_name, key, default as i64).max(0) as u64
}

pub(crate) fn get_str_alias(
    doc: &DocumentMut,
    preferred_table: &str,
    legacy_table: &str,
    key: &str,
    default: &str,
) -> String {
    table(doc, preferred_table)
        .and_then(|table| table.get(key))
        .and_then(Item::as_str)
        .or_else(|| {
            table(doc, legacy_table)
                .and_then(|table| table.get(key))
                .and_then(Item::as_str)
        })
        .unwrap_or(default)
        .to_string()
}

pub(crate) fn get_u64_alias(
    doc: &DocumentMut,
    preferred_table: &str,
    legacy_table: &str,
    key: &str,
    default: u64,
) -> u64 {
    get_u64_opt(doc, preferred_table, key)
        .or_else(|| get_u64_opt(doc, legacy_table, key))
        .unwrap_or(default)
}

pub(crate) fn get_u64_opt(doc: &DocumentMut, table_name: &str, key: &str) -> Option<u64> {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_integer)
        .map(|value| value.max(0) as u64)
}

pub(crate) fn get_history_max_messages(doc: &DocumentMut) -> u64 {
    get_u64_opt(doc, "history", "max_messages")
        .or_else(|| get_u64_opt(doc, "privacy", "max_messages_to_llm"))
        .or_else(|| get_u64_opt(doc, "wxdb", "max_messages"))
        .or_else(|| get_u64_opt(doc, "wx_cli", "max_messages"))
        .unwrap_or(10_000)
}

pub(crate) fn request_body_overrides_to_json(overrides: &impl serde::Serialize) -> String {
    match serde_json::to_value(overrides) {
        Ok(JsonValue::Object(object)) if object.is_empty() => "{}".to_string(),
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string()),
        Err(_) => "{}".to_string(),
    }
}

pub(crate) fn request_body_overrides_from_doc(doc: &DocumentMut) -> String {
    request_body_overrides_from_table_doc(doc, "llm")
}

pub(crate) fn request_body_overrides_from_table_doc(doc: &DocumentMut, table_name: &str) -> String {
    let Some(item) = table(doc, table_name).and_then(|table| table.get("request_body_overrides"))
    else {
        return "{}".to_string();
    };
    toml_item_to_json(item)
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| "{}".to_string())
}

pub(crate) fn toml_item_to_json(item: &Item) -> Option<JsonValue> {
    match item {
        Item::Table(table) => Some(JsonValue::Object(toml_table_to_json_map(table))),
        Item::Value(value) => toml_value_to_json(value),
        _ => None,
    }
}

pub(crate) fn toml_table_to_json_map(table: &Table) -> JsonMap<String, JsonValue> {
    table
        .iter()
        .filter_map(|(key, item)| toml_item_to_json(item).map(|value| (key.to_string(), value)))
        .collect()
}

pub(crate) fn toml_value_to_json(value: &TomlValue) -> Option<JsonValue> {
    if let Some(value) = value.as_bool() {
        return Some(JsonValue::Bool(value));
    }
    if let Some(value) = value.as_integer() {
        return Some(JsonValue::Number(value.into()));
    }
    if let Some(value) = value.as_float() {
        return serde_json::Number::from_f64(value).map(JsonValue::Number);
    }
    if let Some(value) = value.as_str() {
        return Some(JsonValue::String(value.to_string()));
    }
    if let Some(array) = value.as_array() {
        return Some(JsonValue::Array(
            array.iter().filter_map(toml_value_to_json).collect(),
        ));
    }
    if let Some(table) = value.as_inline_table() {
        let object = table
            .iter()
            .filter_map(|(key, value)| {
                toml_value_to_json(value).map(|value| (key.to_string(), value))
            })
            .collect();
        return Some(JsonValue::Object(object));
    }
    None
}

pub(crate) fn get_array(doc: &DocumentMut, table_name: &str, key: &str) -> Vec<String> {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn set_str(table: &mut Table, key: &str, new_value: &str) {
    table[key] = value(new_value);
}

pub(crate) fn set_bool(table: &mut Table, key: &str, new_value: bool) {
    table[key] = value(new_value);
}

pub(crate) fn set_int(table: &mut Table, key: &str, new_value: i64) {
    table[key] = value(new_value);
}

pub(crate) fn remove_key(table: &mut Table, key: &str) {
    table.remove(key);
}

pub(crate) fn migrate_legacy_table(doc: &mut DocumentMut, legacy: &str, current: &str) {
    if !matches!(doc.get(current), Some(Item::Table(_))) {
        if let Some(item) = doc.get(legacy).cloned() {
            doc[current] = item;
        }
    }
}

pub(crate) fn remove_table(doc: &mut DocumentMut, name: &str) {
    doc.remove(name);
}

pub(crate) fn set_array(table: &mut Table, key: &str, values: &[String]) {
    let mut array = Array::default();
    for item in values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        array.push(item);
    }
    table[key] = value(array);
}

pub(crate) fn set_json_object_table(table: &mut Table, key: &str, json_text: &str) -> Result<()> {
    let trimmed = json_text.trim();
    if trimmed.is_empty() {
        table.remove(key);
        return Ok(());
    }
    let json = serde_json::from_str::<JsonValue>(trimmed)
        .with_context(|| format!("parsing {key} JSON object"))?;
    let JsonValue::Object(object) = json else {
        bail!("{key} must be a JSON object");
    };
    if object.is_empty() {
        table.remove(key);
        return Ok(());
    }

    let mut toml_table = Table::new();
    for (field, value) in &object {
        toml_table[field] = json_to_toml_item(value)
            .with_context(|| format!("converting request body override field {field:?}"))?;
    }
    table[key] = Item::Table(toml_table);
    Ok(())
}

pub(crate) fn json_to_toml_item(value: &JsonValue) -> Result<Item> {
    match value {
        JsonValue::Object(object) => {
            let mut table = Table::new();
            for (key, value) in object {
                table[key] = json_to_toml_item(value)
                    .with_context(|| format!("converting nested JSON field {key:?}"))?;
            }
            Ok(Item::Table(table))
        }
        _ => json_to_toml_value(value).map(Item::Value),
    }
}

pub(crate) fn json_to_toml_value(value: &JsonValue) -> Result<TomlValue> {
    match value {
        JsonValue::Null => bail!("JSON null is not supported in TOML request body overrides"),
        JsonValue::Bool(value) => Ok(TomlValue::from(*value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(TomlValue::from(value))
            } else if let Some(value) = value.as_u64() {
                let value = i64::try_from(value)
                    .context("unsigned integer exceeds TOML signed integer range")?;
                Ok(TomlValue::from(value))
            } else if let Some(value) = value.as_f64() {
                Ok(TomlValue::from(value))
            } else {
                bail!("unsupported JSON number")
            }
        }
        JsonValue::String(value) => Ok(TomlValue::from(value.as_str())),
        JsonValue::Array(values) => {
            let mut array = Array::new();
            for value in values {
                array.push(json_to_toml_value(value)?);
            }
            Ok(TomlValue::Array(array))
        }
        JsonValue::Object(object) => {
            let mut table = InlineTable::new();
            for (key, value) in object {
                table.insert(key, json_to_toml_value(value)?);
            }
            Ok(TomlValue::InlineTable(table))
        }
    }
}

pub(crate) fn join_lines(values: &[String]) -> String {
    values.join("\n")
}

pub(crate) fn split_lines(value: &str) -> Vec<String> {
    value
        .split(['\n', '\r', ','])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}
