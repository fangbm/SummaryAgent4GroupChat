use regex::Regex;

use crate::config::PrivacyConfig;

pub struct PrivacyFilter {
    config: PrivacyConfig,
}

impl PrivacyFilter {
    pub fn new(config: PrivacyConfig) -> Self {
        Self { config }
    }

    pub fn apply(&self, input: &str) -> String {
        if !self.config.redact_enabled {
            return input.to_string();
        }

        let wxid = Regex::new(r"\b(wxid_|gh_)[A-Za-z0-9_-]+\b").unwrap();
        let phone = Regex::new(r"\b1[3-9]\d{9}\b").unwrap();
        let email = Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b").unwrap();

        let redacted = wxid.replace_all(input, "[REDACTED_WXID]");
        let redacted = phone.replace_all(&redacted, "[REDACTED_PHONE]");
        email.replace_all(&redacted, "[REDACTED_EMAIL]").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_does_not_redact() {
        let filter = PrivacyFilter::new(PrivacyConfig::default());
        let input = "wxid_abc123 13800138000 user@example.com";
        assert_eq!(filter.apply(input), input);
    }

    #[test]
    fn redacts_when_enabled() {
        let filter = PrivacyFilter::new(PrivacyConfig {
            redact_enabled: true,
            ..PrivacyConfig::default()
        });
        let output = filter.apply("wxid_abc123 13800138000 user@example.com");
        assert!(output.contains("[REDACTED_WXID]"));
        assert!(output.contains("[REDACTED_PHONE]"));
        assert!(output.contains("[REDACTED_EMAIL]"));
    }
}
