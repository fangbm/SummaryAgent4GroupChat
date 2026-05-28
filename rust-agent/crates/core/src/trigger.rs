use regex::Regex;

use crate::{
    config::{ListenConfig, MatchMode},
    models::IncomingMessage,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TriggerMatch {
    pub room_id: String,
    pub trigger_symbol: String,
    pub trigger_content: String,
}

#[derive(Debug, Clone)]
pub struct TriggerMatcher {
    config: ListenConfig,
    regexes: Vec<Regex>,
}

impl TriggerMatcher {
    pub fn new(config: ListenConfig) -> Result<Self, regex::Error> {
        let regexes = if config.match_mode == MatchMode::Regex {
            config
                .triggers
                .iter()
                .map(|pattern| Regex::new(pattern))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        Ok(Self { config, regexes })
    }

    pub fn match_message(&self, msg: &IncomingMessage) -> Option<TriggerMatch> {
        if self.config.ignore_self && msg.is_self {
            return None;
        }

        let room_allowed = self.config.whitelist_rooms.is_empty()
            || self.config.whitelist_rooms.iter().any(|room| {
                room == &msg.room_id
                    || msg
                        .room_name
                        .as_ref()
                        .is_some_and(|name| name.contains(room))
            });
        if !room_allowed {
            return None;
        }

        if self
            .config
            .blacklist_users
            .iter()
            .any(|user| user == &msg.sender_id)
        {
            return None;
        }

        if !self
            .config
            .content_types
            .iter()
            .any(|kind| kind == &msg.msg_type)
        {
            return None;
        }

        let trigger_symbol = match self.config.match_mode {
            MatchMode::Prefix => self
                .config
                .triggers
                .iter()
                .find(|trigger| msg.content.starts_with(trigger.as_str())),
            MatchMode::Contains => self
                .config
                .triggers
                .iter()
                .find(|trigger| msg.content.contains(trigger.as_str())),
            MatchMode::Regex => self
                .regexes
                .iter()
                .zip(self.config.triggers.iter())
                .find_map(|(regex, trigger)| regex.is_match(&msg.content).then_some(trigger)),
        }?;

        Some(TriggerMatch {
            room_id: msg.room_id.clone(),
            trigger_symbol: trigger_symbol.clone(),
            trigger_content: msg.content.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn config() -> ListenConfig {
        ListenConfig {
            triggers: vec!["/总结".into(), "@".into()],
            match_mode: MatchMode::Prefix,
            whitelist_rooms: vec!["room@chatroom".into()],
            blacklist_users: vec!["wxid_blocked".into()],
            content_types: vec!["text".into()],
            ignore_self: true,
        }
    }

    fn msg(content: &str) -> IncomingMessage {
        IncomingMessage {
            room_id: "room@chatroom".into(),
            room_name: Some("测试群".into()),
            sender_id: "wxid_user".into(),
            sender_name: Some("Alice".into()),
            content: content.into(),
            msg_type: "text".into(),
            timestamp: Utc.timestamp_opt(1_716_464_700, 0).unwrap(),
            is_self: false,
        }
    }

    #[test]
    fn matches_prefix_trigger() {
        let matcher = TriggerMatcher::new(config()).unwrap();
        let matched = matcher.match_message(&msg("/总结 刚才说了什么")).unwrap();
        assert_eq!(matched.trigger_symbol, "/总结");
    }

    #[test]
    fn ignores_non_whitelisted_room() {
        let matcher = TriggerMatcher::new(config()).unwrap();
        let mut message = msg("/总结");
        message.room_id = "other@chatroom".into();
        assert!(matcher.match_message(&message).is_none());
    }

    #[test]
    fn ignores_self_messages() {
        let matcher = TriggerMatcher::new(config()).unwrap();
        let mut message = msg("/总结");
        message.is_self = true;
        assert!(matcher.match_message(&message).is_none());
    }
}
