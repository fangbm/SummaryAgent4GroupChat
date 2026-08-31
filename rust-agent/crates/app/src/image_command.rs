//! Built-in manual image command parsing.

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ImageCommand {
    pub(crate) trigger_symbol: String,
    pub(crate) prompt: String,
}

pub(crate) fn parse(content: &str) -> Option<ImageCommand> {
    let content = content.trim();
    let (trigger_symbol, remainder) = if let Some(value) = content.strip_prefix("/图片") {
        ("/图片", value)
    } else {
        let mut parts = content.splitn(2, char::is_whitespace);
        let command = parts.next()?;
        let remainder = parts.next().unwrap_or_default();
        if command.eq_ignore_ascii_case("/image") {
            ("/image", remainder)
        } else if command.eq_ignore_ascii_case("/img") {
            ("/img", remainder)
        } else {
            return None;
        }
    };
    let prompt = remainder.trim();
    if prompt.is_empty() {
        return None;
    }
    Some(ImageCommand {
        trigger_symbol: trigger_symbol.to_string(),
        prompt: prompt.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chinese_and_ascii_aliases() {
        assert_eq!(parse("/图片 星空下的城市").unwrap().prompt, "星空下的城市");
        assert_eq!(parse("/IMAGE anime girl").unwrap().trigger_symbol, "/image");
        assert_eq!(parse("/Img blue hour").unwrap().trigger_symbol, "/img");
    }

    #[test]
    fn rejects_empty_or_prefix_only_commands() {
        assert!(parse("/图片").is_none());
        assert!(parse("/image").is_none());
        assert!(parse("/图片abc").is_some());
    }
}
