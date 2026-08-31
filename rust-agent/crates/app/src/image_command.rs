//! Built-in manual image command parsing.

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ImageCommand {
    pub(crate) trigger_symbol: String,
    /// `None` means the user asked to replay one of the already generated images.
    pub(crate) prompt: Option<String>,
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
    Some(ImageCommand {
        trigger_symbol: trigger_symbol.to_string(),
        prompt: (!prompt.is_empty()).then(|| prompt.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chinese_and_ascii_aliases() {
        assert_eq!(
            parse("/图片 星空下的城市").unwrap().prompt.as_deref(),
            Some("星空下的城市")
        );
        assert_eq!(parse("/IMAGE anime girl").unwrap().trigger_symbol, "/image");
        assert_eq!(parse("/Img blue hour").unwrap().trigger_symbol, "/img");
    }

    #[test]
    fn parses_empty_commands_as_random_generated_image_requests() {
        assert!(parse("/图片").unwrap().prompt.is_none());
        assert!(parse("/image").unwrap().prompt.is_none());
        assert!(parse("/图片abc").is_some());
    }
}
