//! Sanitization rules for model output before it becomes user-visible text.

pub(crate) fn sanitize_llm_visible_output(output: &str) -> String {
    let without_think_blocks = strip_tag_blocks_case_insensitive(output, "think");
    let without_chunk_header = strip_chunk_summary_header(&without_think_blocks);
    strip_reasoning_prelude(&without_chunk_header)
        .trim()
        .to_string()
}

fn strip_tag_blocks_case_insensitive(input: &str, tag: &str) -> String {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut remaining = input;
    let mut output = String::new();
    loop {
        let Some(open_start) = find_case_insensitive(remaining, &open) else {
            output.push_str(remaining);
            break;
        };
        output.push_str(&remaining[..open_start]);
        let after_open = &remaining[open_start..];
        let Some(open_end_rel) = after_open.find('>') else {
            break;
        };
        let after_open_tag = &after_open[open_end_rel + 1..];
        if let Some(close_start_rel) = find_case_insensitive(after_open_tag, &close) {
            remaining = &after_open_tag[close_start_rel + close.len()..];
        } else {
            break;
        }
    }
    output
}

fn strip_chunk_summary_header(input: &str) -> String {
    let trimmed = input.trim_start();
    if !trimmed.starts_with("[CHUNK_SUMMARIES]") {
        return input.to_string();
    }
    let without_marker = trimmed
        .strip_prefix("[CHUNK_SUMMARIES]")
        .unwrap_or(trimmed)
        .trim_start();
    without_marker
        .strip_prefix("以下是同一段群聊按时间顺序切分后的分段总结。")
        .unwrap_or(without_marker)
        .trim_start()
        .to_string()
}

fn strip_reasoning_prelude(input: &str) -> String {
    let markers = [
        "第一个，",
        "第一个时间段",
        "首先是",
        "1.",
        "一、",
        "群聊总结",
        "以下是",
        "主要内容",
        "本次群聊",
    ];
    let suspicious = [
        "用户现在需要总结",
        "需要总结这个超长",
        "首先得",
        "首先先",
        "我需要",
        "我们需要",
    ];
    let trimmed = input.trim_start();
    if !suspicious.iter().any(|marker| trimmed.contains(marker)) {
        return input.to_string();
    }

    let search_limit = trimmed
        .char_indices()
        .nth(600)
        .map(|(index, _)| index)
        .unwrap_or(trimmed.len());
    let search_area = &trimmed[..search_limit];
    let Some(cut) = markers
        .iter()
        .filter_map(|marker| search_area.find(marker))
        .filter(|index| *index > 0)
        .min()
    else {
        return input.to_string();
    };

    trimmed[cut..].to_string()
}

fn find_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    haystack.to_lowercase().find(&needle.to_lowercase())
}
