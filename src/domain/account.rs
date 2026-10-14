//! Local/remote account entity: pure data + validation (no I/O).

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Local usernames: `^[A-Za-z0-9_]{1,20}$`.
pub fn validate_local_username(username: &str) -> Result<(), DomainError> {
    let ok = !username.is_empty()
        && username.len() <= 20
        && username
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(invalid(format!("invalid local username: {username}")))
    }
}

/// Remote usernames: 1..=128 chars matching
/// `^[A-Za-z0-9_]([A-Za-z0-9_.-]*[A-Za-z0-9_])?$`.
pub fn validate_remote_username(username: &str) -> Result<(), DomainError> {
    let len = username.chars().count();
    if len == 0 || len > 128 {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    if !username.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-'
    }) {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    // All bytes are ASCII here, so byte indexing is character indexing.
    let bytes = username.as_bytes();
    let edge_ok = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    if !edge_ok(bytes[0]) {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    if len > 1 && !edge_ok(bytes[bytes.len() - 1]) {
        return Err(invalid(format!("invalid remote username: {username}")));
    }
    Ok(())
}

/// Birthdays are `YYYY-MM-DD` calendar dates.
pub fn validate_birthday(b: &str) -> Result<(), DomainError> {
    let bytes = b.as_bytes();
    let shape_ok = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
    if !shape_ok {
        return Err(invalid(format!(
            "invalid birthday (expected YYYY-MM-DD): {b}"
        )));
    }
    chrono::NaiveDate::parse_from_str(b, "%Y-%m-%d")
        .map(|_| ())
        .map_err(|_| invalid(format!("invalid birthday (not a calendar date): {b}")))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub id: String,
    pub username: String,
    pub username_lower: String,
    pub host: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub birthday: Option<String>,
    pub uri: Option<String>,
    pub inbox: Option<String>,
    pub shared_inbox: Option<String>,
    pub followers_count: u64,
    pub following_count: u64,
    pub notes_count: u64,
    pub avatar_id: Option<String>,
    pub banner_id: Option<String>,
    pub is_locked: bool,
    pub is_bot: bool,
    pub is_suspended: bool,
    pub is_explorable: bool,
    pub is_deleted: bool,
    pub is_cat: bool,
}

impl Account {
    pub fn new_local(id: String, username: String) -> Result<Self, DomainError> {
        validate_local_username(&username)?;
        Ok(Self {
            id,
            username_lower: username.to_ascii_lowercase(),
            username,
            host: None,
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: None,
            inbox: None,
            shared_inbox: None,
            followers_count: 0,
            following_count: 0,
            notes_count: 0,
            avatar_id: None,
            banner_id: None,
            is_locked: false,
            is_bot: false,
            is_suspended: false,
            is_explorable: true,
            is_deleted: false,
            is_cat: false,
        })
    }

    pub fn is_local(&self) -> bool {
        self.host.is_none()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        match &self.host {
            None => {
                validate_local_username(&self.username)?;
                for (label, value) in [
                    ("uri", &self.uri),
                    ("inbox", &self.inbox),
                    ("shared_inbox", &self.shared_inbox),
                ] {
                    if value.is_some() {
                        return Err(invalid(format!(
                            "local account must have null {label}"
                        )));
                    }
                }
            }
            Some(host) => {
                if host.chars().count() > 128 {
                    return Err(invalid("host exceeds 128 chars".to_string()));
                }
                validate_remote_username(&self.username)?;
                match &self.uri {
                    Some(uri) if !uri.is_empty() && uri.chars().count() <= 512 => {}
                    _ => {
                        return Err(invalid(
                            "remote account requires uri of 1..=512 chars".to_string(),
                        ))
                    }
                }
                match &self.inbox {
                    Some(inbox) if !inbox.is_empty() && inbox.chars().count() <= 512 => {}
                    _ => {
                        return Err(invalid(
                            "remote account requires inbox of 1..=512 chars".to_string(),
                        ))
                    }
                }
                if let Some(shared_inbox) = &self.shared_inbox {
                    if shared_inbox.chars().count() > 512 {
                        return Err(invalid(
                            "shared_inbox exceeds 512 chars".to_string(),
                        ));
                    }
                }
            }
        }
        if let Some(display_name) = &self.display_name {
            let n = display_name.chars().count();
            if n == 0 || n > 128 {
                return Err(invalid(
                    "display_name must be 1..=128 chars".to_string(),
                ));
            }
        }
        if let Some(description) = &self.description {
            if description.chars().count() > 1500 {
                return Err(invalid("description exceeds 1500 chars".to_string()));
            }
        }
        if let Some(location) = &self.location {
            if location.chars().count() > 50 {
                return Err(invalid("location exceeds 50 chars".to_string()));
            }
        }
        if let Some(birthday) = &self.birthday {
            validate_birthday(birthday)?;
        }
        Ok(())
    }

    /// Local accounts resolve to `{public_url}/users/{id}`; remote accounts
    /// resolve to their stored `uri`. Public URLs never expose node hostnames.
    pub fn actor_url(&self, public_url: &str) -> String {
        if self.is_local() {
            format!("{}/users/{}", public_url.trim_end_matches('/'), self.id)
        } else {
            self.uri.clone().unwrap_or_default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_account() -> Account {
        Account::new_local("aid00000000000001".to_string(), "alice".to_string()).unwrap()
    }

    fn remote_account() -> Account {
        Account {
            id: "aid00000000000002".to_string(),
            username: "bob".to_string(),
            username_lower: "bob".to_string(),
            host: Some("remote.example".to_string()),
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: Some("https://remote.example/users/bob".to_string()),
            inbox: Some("https://remote.example/users/bob/inbox".to_string()),
            shared_inbox: None,
            followers_count: 0,
            following_count: 0,
            notes_count: 0,
            avatar_id: None,
            banner_id: None,
            is_locked: false,
            is_bot: false,
            is_suspended: false,
            is_explorable: false,
            is_deleted: false,
            is_cat: false,
        }
    }

    #[test]
    fn new_local_sets_defaults() {
        let a = local_account();
        assert_eq!(a.username_lower, "alice");
        assert_eq!(a.host, None);
        assert!(a.is_explorable);
        assert!(!a.is_locked && !a.is_bot && !a.is_suspended && !a.is_deleted && !a.is_cat);
        assert_eq!((a.followers_count, a.following_count, a.notes_count), (0, 0, 0));
        assert!(a.validate().is_ok());
    }

    #[test]
    fn new_local_lowercases_username() {
        let a = Account::new_local("aid00000000000003".to_string(), "Alice_1".to_string())
            .unwrap();
        assert_eq!(a.username, "Alice_1");
        assert_eq!(a.username_lower, "alice_1");
        assert!(a.validate().is_ok());
    }

    #[test]
    fn local_username_bounds() {
        for good in ["a".to_string(), "A_09".to_string(), "x".repeat(20)] {
            assert!(validate_local_username(&good).is_ok(), "expected ok: {good}");
        }
        for bad in [
            "".to_string(),
            "x".repeat(21),
            "has space".to_string(),
            "hy-phen".to_string(),
            "dot.name".to_string(),
            "caf\u{e9}".to_string(),
        ] {
            assert!(validate_local_username(&bad).is_err(), "expected err: {bad}");
        }
    }

    #[test]
    fn remote_username_bounds() {
        for good in [
            "a".to_string(),
            "user.name-1_x".to_string(),
            "x".repeat(128),
            "0".to_string(),
        ] {
            assert!(validate_remote_username(&good).is_ok(), "expected ok: {good}");
        }
        for bad in [
            "".to_string(),
            "x".repeat(129),
            ".leading".to_string(),
            "trailing.".to_string(),
            "-leading".to_string(),
            "trailing-".to_string(),
            "has space".to_string(),
        ] {
            assert!(validate_remote_username(&bad).is_err(), "expected err: {bad}");
        }
        assert!(validate_remote_username("_").is_ok());
        assert!(validate_remote_username("a_b").is_ok());
    }

    #[test]
    fn description_and_location_bounds() {
        let mut a = local_account();
        a.description = Some("d".repeat(1500));
        assert!(a.validate().is_ok());
        a.description = Some("d".repeat(1501));
        assert!(a.validate().is_err());

        let mut a = local_account();
        a.location = Some("l".repeat(50));
        assert!(a.validate().is_ok());
        a.location = Some("l".repeat(51));
        assert!(a.validate().is_err());
    }

    #[test]
    fn birthday_bounds() {
        assert!(validate_birthday("1990-02-28").is_ok());
        assert!(validate_birthday("2000-02-29").is_ok()); // leap day
        assert!(validate_birthday("1990-02-30").is_err()); // no such date
        assert!(validate_birthday("1990-13-01").is_err());
        assert!(validate_birthday("not-a-date").is_err());
        assert!(validate_birthday("1990-2-8").is_err()); // not zero-padded
        assert!(validate_birthday("").is_err());

        let mut a = local_account();
        a.birthday = Some("1990-02-30".to_string());
        assert!(a.validate().is_err());
        a.birthday = Some("1990-02-28".to_string());
        assert!(a.validate().is_ok());
    }

    #[test]
    fn remote_uri_requirement() {
        assert!(remote_account().validate().is_ok());

        let mut no_uri = remote_account();
        no_uri.uri = None;
        assert!(no_uri.validate().is_err());

        let mut no_inbox = remote_account();
        no_inbox.inbox = None;
        assert!(no_inbox.validate().is_err());

        let mut local_with_uri = local_account();
        local_with_uri.uri = Some("https://example.local/users/x".to_string());
        assert!(local_with_uri.validate().is_err());
    }

    #[test]
    fn actor_url_forms() {
        let local = local_account();
        assert_eq!(
            local.actor_url("https://example.local/"),
            "https://example.local/users/aid00000000000001"
        );
        let remote = remote_account();
        assert_eq!(
            remote.actor_url("https://example.local"),
            "https://remote.example/users/bob"
        );
    }
}
