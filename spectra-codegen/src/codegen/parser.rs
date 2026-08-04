//! String-based DSL extraction from `spectra_schema!` / `spectra_metric!` source files.

#[derive(Debug, Clone)]
pub struct ParsedField {
    pub name: String,
    pub rust_type: String,
}

#[derive(Debug, Clone)]
pub struct ParsedEventSchema {
    pub schema_name: String,
    pub table: String,
    pub store: String,
    pub version: String,
    pub description: Option<String>,
    pub fields: Vec<ParsedField>,
}

#[derive(Debug, Clone)]
pub struct ParsedMetricSchema {
    pub schema_name: String,
    pub name: String,
    pub store: String,
    pub version: String,
    pub description: Option<String>,
}

pub fn parse_event_schema(content: &str) -> anyhow::Result<ParsedEventSchema> {
    let schema_name = extract_brace_name(content, "spectra_schema!")?;
    let table = extract_quoted_field(content, "table:")?;
    let store = extract_quoted_field(content, "store:").unwrap_or_else(|_| "default".to_string());
    let version = extract_quoted_field(content, "version:")?;
    let description = extract_quoted_field(content, "description:").ok();
    let fields = extract_event_fields(content)?;
    Ok(ParsedEventSchema {
        schema_name,
        table,
        store,
        version,
        description,
        fields,
    })
}

pub fn parse_metric_schema(content: &str) -> anyhow::Result<ParsedMetricSchema> {
    let schema_name = extract_brace_name(content, "spectra_metric!")?;
    let name = extract_quoted_field(content, "name:")?;
    let store = extract_quoted_field(content, "store:").unwrap_or_else(|_| "default".to_string());
    let version = extract_quoted_field(content, "version:")?;
    let description = extract_quoted_field(content, "description:").ok();
    Ok(ParsedMetricSchema {
        schema_name,
        name,
        store,
        version,
        description,
    })
}

fn extract_brace_name(content: &str, macro_name: &str) -> anyhow::Result<String> {
    let start = content
        .find(macro_name)
        .ok_or_else(|| anyhow::anyhow!("missing {macro_name}"))?;
    let after = &content[start + macro_name.len()..];
    let brace = after
        .find('{')
        .ok_or_else(|| anyhow::anyhow!("missing opening brace after {macro_name}"))?;
    let inner = after[brace + 1..].trim_start();
    let name_end = inner
        .find(|c: char| c == '{' || c.is_whitespace())
        .unwrap_or(inner.len());
    Ok(inner[..name_end].trim().to_string())
}

fn extract_quoted_field(content: &str, key: &str) -> anyhow::Result<String> {
    let idx = content
        .find(key)
        .ok_or_else(|| anyhow::anyhow!("missing field {key}"))?;
    let after = &content[idx + key.len()..];
    let quote = after
        .find('"')
        .ok_or_else(|| anyhow::anyhow!("missing quote for {key}"))?;
    let rest = &after[quote + 1..];
    let end = rest
        .find('"')
        .ok_or_else(|| anyhow::anyhow!("unterminated string for {key}"))?;
    Ok(rest[..end].to_string())
}

fn extract_event_fields(content: &str) -> anyhow::Result<Vec<ParsedField>> {
    let fields_key = "fields:";
    let idx = match content.find(fields_key) {
        Some(i) => i,
        None => return Ok(Vec::new()),
    };
    let after = &content[idx + fields_key.len()..];
    let bracket = after
        .find('[')
        .ok_or_else(|| anyhow::anyhow!("missing fields ["))?;
    let section = extract_bracket_section(&after[bracket + 1..])?;
    let mut fields = Vec::new();
    let mut rest = section.as_str();
    while let Some(brace) = rest.find('{') {
        let before = rest[..brace].trim();
        let name = before
            .trim_end_matches(':')
            .split_whitespace()
            .last()
            .unwrap_or("")
            .trim()
            .to_string();
        if !name.is_empty() && name != "classification" && name != "fields" {
            let block = extract_brace_block(&rest[brace..])?;
            let rust_type = extract_field_rust_type(&block);
            fields.push(ParsedField { name, rust_type });
        }
        rest = &rest[brace + extract_brace_block_len(&rest[brace..])?..];
        rest = rest.trim_start_matches(|c: char| c == ',' || c.is_whitespace());
        if rest.starts_with(']') || rest.is_empty() {
            break;
        }
    }
    Ok(fields)
}

fn extract_bracket_section(s: &str) -> anyhow::Result<String> {
    let mut depth = 0i32;
    for (i, c) in s.char_indices() {
        if c == '[' {
            depth += 1;
        } else if c == ']' {
            if depth == 0 {
                return Ok(s[..i].to_string());
            }
            depth -= 1;
        }
    }
    Ok(s.to_string())
}

fn extract_brace_block(s: &str) -> anyhow::Result<String> {
    let mut depth = 0i32;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(s[..=i].to_string());
                }
            }
            _ => {}
        }
    }
    anyhow::bail!("unclosed brace in field block");
}

fn extract_brace_block_len(s: &str) -> anyhow::Result<usize> {
    Ok(extract_brace_block(s)?.len())
}

fn extract_field_rust_type(block: &str) -> String {
    for key in ["r#type:", "type:"] {
        if let Some(idx) = block.find(key) {
            let after = &block[idx + key.len()..];
            let ident_end = after
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .unwrap_or(after.len());
            let t = after[..ident_end].trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }
    "String".to_string()
}
