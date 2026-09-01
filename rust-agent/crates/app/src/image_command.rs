//! Built-in manual image command parsing.

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ImageCommand {
    pub(crate) trigger_symbol: String,
    pub(crate) mode: ImageMode,
    /// `None` means the user asked to replay one of the already generated images.
    pub(crate) prompt: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ImageMode {
    Single,
    Manga { pages: usize },
}

pub(crate) const DEFAULT_MANGA_PAGES: usize = 8;
const MIN_MANGA_PAGES: usize = 2;
const MAX_MANGA_PAGES: usize = 8;

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
    let (mode, prompt, error) = parse_mode_and_prompt(prompt);
    Some(ImageCommand {
        trigger_symbol: trigger_symbol.to_string(),
        mode,
        prompt: (!prompt.is_empty()).then(|| prompt.to_string()),
        error,
    })
}

fn parse_mode_and_prompt(input: &str) -> (ImageMode, &str, Option<String>) {
    let mut parts = input.splitn(3, char::is_whitespace);
    let first = parts.next().unwrap_or_default();
    if !matches!(
        first.to_ascii_lowercase().as_str(),
        "manga" | "comic" | "漫画" | "本子"
    ) {
        return (ImageMode::Single, input, None);
    }

    let second = parts.next().unwrap_or_default().trim();
    let (pages, rest) = if second.is_empty() {
        (DEFAULT_MANGA_PAGES, "")
    } else if let Some(value) = parse_page_token(second) {
        (value, parts.next().unwrap_or_default().trim())
    } else if second.chars().all(|ch| ch.is_ascii_digit()) {
        let value = second.parse::<usize>().unwrap_or(usize::MAX);
        let error = Some(format!(
            "漫画页数必须在 {MIN_MANGA_PAGES} 到 {MAX_MANGA_PAGES} 页之间"
        ));
        return (
            ImageMode::Manga { pages: value },
            parts.next().unwrap_or_default().trim(),
            error,
        );
    } else {
        (DEFAULT_MANGA_PAGES, input[first.len()..].trim())
    };

    let error = (!(MIN_MANGA_PAGES..=MAX_MANGA_PAGES).contains(&pages))
        .then(|| format!("漫画页数必须在 {MIN_MANGA_PAGES} 到 {MAX_MANGA_PAGES} 页之间"));
    (ImageMode::Manga { pages }, rest, error)
}

fn parse_page_token(token: &str) -> Option<usize> {
    let normalized = token.trim().to_ascii_lowercase();
    let number = normalized
        .strip_suffix("pages")
        .or_else(|| normalized.strip_suffix('p'))
        .or_else(|| normalized.strip_suffix('页'))?;
    (!number.is_empty()).then(|| number.parse().ok()).flatten()
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
        assert_eq!(
            parse("/image manga 2 seaside train").unwrap().mode,
            ImageMode::Manga { pages: 2 }
        );
        assert_eq!(
            parse("/图片 漫画 8页 海边").unwrap().prompt.as_deref(),
            Some("海边")
        );
    }

    #[test]
    fn parses_empty_commands_as_random_generated_image_requests() {
        assert!(parse("/图片").unwrap().prompt.is_none());
        assert!(parse("/image").unwrap().prompt.is_none());
        assert!(parse("/图片abc").is_some());
    }

    #[test]
    fn rejects_manga_page_counts_outside_the_bound() {
        let command = parse("/image manga 9 story").unwrap();
        assert!(command.error.is_some());
        assert_eq!(command.prompt.as_deref(), Some("story"));
    }
}
