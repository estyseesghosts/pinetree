use crate::application::commands::AppServices;

use crate::application::ports::PortError;

impl AppServices {
    /// Apply one antenna import file (Batch N WS4; upstream
    /// `ImportAntennasProcessorService` parity): skip empty-keyword rows
    /// (`keywords` empty or first group all blank — verbatim upstream
    /// rule), skip schema-invalid rows, rewrite `src=list`+`userListAccts`
    /// to `src=users` (upstream parity; `src=list` without accts is
    /// skipped — pinetree has no lists to reference), and create the rest
    /// through `create_antenna`. Returns `(applied, skipped)`; only storage
    /// faults abort the job.
    pub(crate) fn apply_antenna_import(
        &self,
        user_id: &str,
        bytes: &[u8],
    ) -> Result<(usize, usize), PortError> {
        use crate::domain::antenna::AntennaSrc;
        let text = String::from_utf8_lossy(bytes);
        let parsed: serde_json::Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(_) => return Ok((0, 1)),
        };
        let entries = match parsed.as_array() {
            Some(entries) => entries,
            None => return Ok((0, 1)),
        };
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for entry in entries {
            let Some(map) = entry.as_object() else {
                skipped += 1;
                continue;
            };
            // Required keys (upstream `required` parity).
            if ![
                "name",
                "src",
                "keywords",
                "excludeKeywords",
                "users",
                "caseSensitive",
                "withReplies",
                "withFile",
            ]
            .iter()
            .all(|key| map.contains_key(*key))
            {
                skipped += 1;
                continue;
            }
            let Some(name) = map.get("name").and_then(|v| v.as_str()) else {
                skipped += 1;
                continue;
            };
            let name = name.to_string();
            if name.chars().count() == 0 || name.chars().count() > 100 {
                skipped += 1;
                continue;
            }
            let Some(src_raw) = map.get("src").and_then(|v| v.as_str()) else {
                skipped += 1;
                continue;
            };
            let mut src = match AntennaSrc::parse(src_raw) {
                Some(s) => s,
                None => {
                    skipped += 1;
                    continue;
                }
            };
            let str_array = |key: &str| -> Option<Vec<Vec<String>>> {
                let arr = map.get(key)?.as_array()?;
                arr.iter()
                    .map(|group| {
                        group.as_array().map(|items| {
                            items
                                .iter()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect()
                        })
                    })
                    .collect()
            };
            let flat_array = |key: &str| -> Option<Vec<String>> {
                map.get(key)?.as_array().map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
            };
            let Some(keywords) = str_array("keywords") else {
                skipped += 1;
                continue;
            };
            let Some(exclude_keywords) = str_array("excludeKeywords") else {
                skipped += 1;
                continue;
            };
            let Some(mut users) = flat_array("users") else {
                skipped += 1;
                continue;
            };
            let (case_sensitive, with_replies, with_file) = match (
                map.get("caseSensitive").and_then(|v| v.as_bool()),
                map.get("withReplies").and_then(|v| v.as_bool()),
                map.get("withFile").and_then(|v| v.as_bool()),
            ) {
                (Some(a), Some(b), Some(c)) => (a, b, c),
                _ => {
                    skipped += 1;
                    continue;
                }
            };
            let opt_bool = |key: &str, default: bool| -> Option<bool> {
                match map.get(key) {
                    None => Some(default),
                    Some(v) => v.as_bool(),
                }
            };
            let (Some(local_only), Some(exclude_bots)) =
                (opt_bool("localOnly", false), opt_bool("excludeBots", false))
            else {
                skipped += 1;
                continue;
            };
            // Upstream empty-keyword skip (verbatim rule).
            if keywords.is_empty()
                || keywords
                    .first()
                    .map(|g| g.iter().all(|x| x.is_empty()))
                    .unwrap_or(true)
            {
                skipped += 1;
                continue;
            }
            // `src=list` rewrite (upstream parity): accts become `users`.
            if src == AntennaSrc::List {
                match map.get("userListAccts") {
                    Some(serde_json::Value::Array(accts)) => {
                        users = accts
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .filter(|s| !s.is_empty())
                            .collect();
                        src = AntennaSrc::Users;
                    }
                    _ => {
                        skipped += 1;
                        continue;
                    }
                }
            }
            match self.create_antenna(
                user_id,
                name,
                src,
                None,
                keywords,
                exclude_keywords,
                users,
                case_sensitive,
                with_replies,
                with_file,
                local_only,
                exclude_bots,
            ) {
                Ok(_) => applied += 1,
                Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                Err(_) => skipped += 1,
            }
        }
        Ok((applied, skipped))
    }
}

fn validate_antenna_import(bytes: &[u8]) -> (usize, usize) {
    let text = String::from_utf8_lossy(bytes);
    let parsed: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(_) => return (0, 1),
    };
    let entries = match parsed.as_array() {
        Some(entries) => entries,
        None => return (0, 1),
    };
    let mut valid = 0usize;
    let mut invalid = 0usize;
    for entry in entries {
        let obj = entry.as_object();
        let ok = obj.is_some_and(|map| {
            [
                "name",
                "src",
                "keywords",
                "excludeKeywords",
                "users",
                "caseSensitive",
                "withReplies",
                "withFile",
            ]
            .iter()
            .all(|key| map.contains_key(*key))
        });
        if ok {
            valid += 1;
        } else {
            invalid += 1;
        }
    }
    (valid, invalid)
}
