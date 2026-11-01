//! What a client may reach in the keyspace and on channels.
//!
//! Administrators, and local agents without a token, reach everything. An agent
//! token carries a scope: glob patterns for the keys it may change, the keys it
//! may only read, and the channels it may use. By default an agent reaches its
//! own namespace (`<agent>:...`) and the shared one (`shared:...`).

use std::sync::{Arc, OnceLock};

use crate::commands::{Name, key_args};
use crate::db::literal_prefix;
use crate::glob::Glob;

/// Channels the server publishes key changes on. No client may publish to them.
pub const NOTIFICATION_PREFIXES: [&str; 2] = ["__keyspace__", "__keyevent__"];

#[derive(Clone, Debug, PartialEq)]
pub struct Scope {
    /// Keys the client may read and change.
    pub keys: Vec<String>,
    /// Keys the client may only read.
    pub read: Vec<String>,
    /// Channels the client may publish to and receive from.
    pub channels: Vec<String>,
    all_keys: bool,
    all_channels: bool,
    /// The patterns above, compiled.
    key_globs: Vec<Glob>,
    read_globs: Vec<Glob>,
    channel_globs: Vec<Glob>,
    /// Where every reachable key starts, when the patterns say: sorted, none
    /// starting with another. Listing keys then visits only those runs.
    prefixes: Option<Vec<String>>,
}

impl Scope {
    pub fn new(keys: Vec<String>, read: Vec<String>, channels: Vec<String>) -> Scope {
        let all_keys = keys.iter().any(|p| p == "*");
        let mut starts: Vec<&str> = keys
            .iter()
            .chain(&read)
            .map(|p| literal_prefix(p))
            .collect();
        starts.sort_unstable();
        let mut prefixes: Vec<String> = Vec::new();
        for start in starts {
            if !prefixes
                .last()
                .is_some_and(|kept| start.starts_with(kept.as_str()))
            {
                prefixes.push(start.to_string());
            }
        }
        let compile = |patterns: &[String]| patterns.iter().map(|p| Glob::new(p)).collect();
        Scope {
            all_keys,
            all_channels: channels.iter().any(|p| p == "*"),
            key_globs: compile(&keys),
            read_globs: compile(&read),
            channel_globs: compile(&channels),
            prefixes: (!all_keys && prefixes.iter().all(|p| !p.is_empty())).then_some(prefixes),
            keys,
            read,
            channels,
        }
    }

    /// The runs of keys worth visiting to find this scope's keys that start
    /// with `prefix`: sorted, none starting with another.
    pub fn search_prefixes(&self, prefix: &str) -> Vec<String> {
        let Some(own) = &self.prefixes else {
            return vec![prefix.to_string()];
        };
        let mut runs = Vec::new();
        for start in own {
            if prefix.starts_with(start.as_str()) {
                return vec![prefix.to_string()];
            }
            if start.starts_with(prefix) {
                runs.push(start.clone());
            }
        }
        runs
    }

    /// Every key and channel.
    pub fn all() -> Scope {
        Scope::new(vec!["*".into()], Vec::new(), vec!["*".into()])
    }

    /// [`Scope::all`], shared, for the many callers that reach everything.
    pub fn everything() -> Arc<Scope> {
        static ALL: OnceLock<Arc<Scope>> = OnceLock::new();
        Arc::clone(ALL.get_or_init(|| Arc::new(Scope::all())))
    }

    /// The default for an agent token: the agent's own namespace and the shared one.
    pub fn agent(name: &str) -> Scope {
        let own = vec![format!("{name}:*"), "shared:*".to_string()];
        Scope::new(own.clone(), Vec::new(), own)
    }

    pub fn is_all(&self) -> bool {
        self.all_keys && self.all_channels
    }

    pub fn can_write(&self, key: &str) -> bool {
        self.all_keys || self.key_globs.iter().any(|g| g.matches(key))
    }

    pub fn can_read(&self, key: &str) -> bool {
        self.can_write(key) || self.read_globs.iter().any(|g| g.matches(key))
    }

    pub fn can_use_channel(&self, channel: &str) -> bool {
        self.all_channels || self.channel_globs.iter().any(|g| g.matches(channel))
    }

    /// Whether a subscription to exactly `channel` can ever deliver anything:
    /// a key's change channel needs the key, event channels are filtered per key.
    pub fn can_subscribe(&self, channel: &str) -> bool {
        if let Some(key) = channel.strip_prefix("__keyspace__:") {
            self.can_read(key)
        } else {
            channel.starts_with("__keyevent__:") || self.can_use_channel(channel)
        }
    }

    /// Whether a published message may reach this scope. Key-change
    /// notifications follow the key, not the channel name.
    pub fn can_receive(&self, channel: &str, message: &str) -> bool {
        if let Some(key) = channel.strip_prefix("__keyspace__:") {
            self.can_read(key)
        } else if channel.starts_with("__keyevent__:") {
            self.can_read(message)
        } else {
            self.can_use_channel(channel)
        }
    }

    fn describe(patterns: &[String]) -> String {
        if patterns.is_empty() {
            "none".into()
        } else {
            patterns.join(", ")
        }
    }

    fn denied_key(&self, key: &str, write: bool) -> String {
        let reach = if write || self.read.is_empty() {
            Scope::describe(&self.keys)
        } else {
            format!(
                "{} (and {} read-only)",
                Scope::describe(&self.keys),
                Scope::describe(&self.read)
            )
        };
        format!(
            "NOPERM this token cannot {} key {key:?}; it reaches {reach}",
            if write { "change" } else { "read" }
        )
    }

    pub fn denied_channel(&self, channel: &str) -> String {
        format!(
            "NOPERM this token cannot use channel {channel:?}; it reaches {}",
            Scope::describe(&self.channels)
        )
    }
}

/// Who runs a command: whether it may flush the keyspace, and what it reaches.
#[derive(Clone, Debug)]
pub struct Access {
    pub admin: bool,
    pub scope: Arc<Scope>,
}

impl Access {
    pub fn admin() -> Access {
        Access {
            admin: true,
            scope: Scope::everything(),
        }
    }

    /// Checks every key a command names, and the channels it publishes to or
    /// asks about, before the command runs.
    pub fn permit(&self, argv: &[String]) -> Result<(), String> {
        let name = Name::of(argv);
        if name == "PUBLISH"
            && let Some(channel) = argv.get(1)
        {
            if NOTIFICATION_PREFIXES.iter().any(|p| channel.starts_with(p)) {
                return Err(format!(
                    "ERR channels starting with {} or {} carry key-change notifications; only the server publishes to them",
                    NOTIFICATION_PREFIXES[0], NOTIFICATION_PREFIXES[1]
                ));
            }
            if !self.scope.can_use_channel(channel) {
                return Err(self.scope.denied_channel(channel));
            }
        }
        if name == "PUBSUB"
            && argv
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case("NUMSUB"))
            && let Some(channel) = argv[2..].iter().find(|c| !self.scope.can_use_channel(c))
        {
            return Err(self.scope.denied_channel(channel));
        }
        if self.scope.all_keys {
            return Ok(());
        }
        // A command the keyspace does not know names no keys we could check.
        let keys = key_args(argv).ok_or_else(|| {
            let word = argv.first().map_or("", String::as_str);
            format!("ERR unknown command '{}'", word.to_ascii_lowercase())
        })?;
        for (index, write) in keys {
            let key = &argv[index];
            let allowed = if write {
                self.scope.can_write(key)
            } else {
                self.scope.can_read(key)
            };
            if !allowed {
                return Err(self.scope.denied_key(key, write));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    fn agent(name: &str) -> Access {
        Access {
            admin: false,
            scope: Arc::new(Scope::agent(name)),
        }
    }

    #[test]
    fn agents_reach_their_own_and_shared_keys() {
        let alice = agent("alice");
        assert!(alice.permit(&argv(&["SET", "alice:plan", "v1"])).is_ok());
        assert!(alice.permit(&argv(&["GET", "shared:config"])).is_ok());
        let denied = alice.permit(&argv(&["GET", "bob:plan"])).unwrap_err();
        assert!(denied.starts_with("NOPERM"), "{denied}");
        assert!(
            denied.contains("alice:*"),
            "the error says what the token reaches"
        );
        assert!(
            alice.permit(&argv(&["GET", "alice"])).is_err(),
            "the namespace needs its colon"
        );
    }

    #[test]
    fn every_key_of_a_command_is_checked() {
        let alice = agent("alice");
        assert!(alice.permit(&argv(&["MGET", "alice:a", "bob:b"])).is_err());
        assert!(
            alice
                .permit(&argv(&["MSET", "alice:a", "1", "bob:b", "2"]))
                .is_err()
        );
        assert!(
            alice.permit(&argv(&["MSET", "alice:a", "bob:b"])).is_ok(),
            "values are not keys"
        );
        assert!(
            alice
                .permit(&argv(&["RENAME", "alice:a", "bob:a"]))
                .is_err()
        );
        assert!(
            alice
                .permit(&argv(&["LMOVE", "alice:q", "bob:q", "LEFT", "RIGHT"]))
                .is_err()
        );
        assert!(
            alice
                .permit(&argv(&["BLPOP", "alice:q", "bob:q", "0"]))
                .is_err()
        );
        assert!(
            alice.permit(&argv(&["BLPOP", "alice:q", "0"])).is_ok(),
            "the timeout is not a key"
        );
        assert!(alice.permit(&argv(&["DEL", "alice:a", "bob:b"])).is_err());
        // A name in any case is the same command, and checked as it.
        assert!(alice.permit(&argv(&["mGeT", "alice:a", "bob:b"])).is_err());
        assert!(alice.permit(&argv(&["pUbLiSh", "builds", "hi"])).is_err());
        // A name no command has names no keys to check, and is refused in
        // its own words, however long.
        let long = "Get".repeat(20);
        assert_eq!(
            alice.permit(&argv(&[&long, "bob:b"])).unwrap_err(),
            format!("ERR unknown command '{}'", long.to_ascii_lowercase())
        );
        assert!(alice.permit(&argv(&[])).is_err());
    }

    #[test]
    fn stream_reads_check_every_stream() {
        let alice = agent("alice");
        assert!(
            alice
                .permit(&argv(&[
                    "XREAD", "COUNT", "5", "STREAMS", "alice:s", "bob:s", "0", "0"
                ]))
                .is_err()
        );
        assert!(
            alice
                .permit(&argv(&["XREAD", "BLOCK", "0", "STREAMS", "alice:s", "$"]))
                .is_ok()
        );
        // A group called STREAMS does not hide the real stream key.
        let read = argv(&[
            "XREADGROUP",
            "GROUP",
            "STREAMS",
            "c",
            "STREAMS",
            "bob:s",
            ">",
        ]);
        assert!(alice.permit(&read).is_err());
        assert!(
            alice
                .permit(&argv(&["XGROUP", "CREATE", "bob:s", "g", "$"]))
                .is_err()
        );
        assert!(alice.permit(&argv(&["XINFO", "STREAM", "bob:s"])).is_err());
    }

    #[test]
    fn read_only_patterns_allow_reads_only() {
        let reader = Access {
            admin: false,
            scope: Arc::new(Scope::new(
                vec!["mine:*".into()],
                vec!["config:*".into()],
                Vec::new(),
            )),
        };
        assert!(reader.permit(&argv(&["GET", "config:region"])).is_ok());
        assert!(
            reader
                .permit(&argv(&["SET", "config:region", "eu"]))
                .is_err()
        );
        assert!(
            reader
                .permit(&argv(&["SMOVE", "config:a", "mine:b", "x"]))
                .is_err()
        );
    }

    #[test]
    fn listing_visits_only_the_runs_a_scope_reaches() {
        let scope = Scope::new(
            vec!["alice:*".into(), "alice:tmp:*".into(), "shared:*".into()],
            vec!["config:?".into()],
            Vec::new(),
        );
        assert_eq!(scope.search_prefixes(""), ["alice:", "config:", "shared:"]);
        assert_eq!(scope.search_prefixes("alice:t"), ["alice:t"]);
        assert_eq!(scope.search_prefixes("sh"), ["shared:"]);
        assert!(scope.search_prefixes("bob:").is_empty());
        let wide = Scope::new(vec!["*:alice".into()], Vec::new(), Vec::new());
        assert_eq!(
            wide.search_prefixes("x"),
            ["x"],
            "no literal start: walk everything"
        );
        assert_eq!(Scope::all().search_prefixes(""), [""]);
    }

    #[test]
    fn channels_are_scoped_and_notifications_are_reserved() {
        let alice = agent("alice");
        assert!(
            alice
                .permit(&argv(&["PUBLISH", "alice:events", "hi"]))
                .is_ok()
        );
        assert!(alice.permit(&argv(&["PUBLISH", "builds", "hi"])).is_err());
        assert!(
            Access::admin()
                .permit(&argv(&["PUBLISH", "__keyspace__:x", "del"]))
                .is_err()
        );
        assert!(
            Access::admin()
                .permit(&argv(&["PUBLISH", "builds", "hi"]))
                .is_ok()
        );
        let scope = Scope::agent("alice");
        assert!(scope.can_receive("__keyspace__:alice:plan", "set"));
        assert!(!scope.can_receive("__keyspace__:bob:plan", "set"));
        assert!(scope.can_receive("__keyevent__:set", "shared:x"));
        assert!(!scope.can_receive("__keyevent__:set", "bob:x"));
    }
}
