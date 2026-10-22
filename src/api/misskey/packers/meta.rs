//! Pack meta (split from `packers.rs`). Pure move.
//!
#![allow(unused_imports)]
//! Misskey response packers (Slice D4).
//!
//! Render domain types into Misskey 2026.10 wire shapes
//! (`misskey/packages/backend/src/models/json-schema/`:
//! `note.ts`, `user.ts`, `drive-file.ts`, `meta.ts`). Field sets are
//! pinned by snapshot tests at the bottom of this file that read the
//! actual checkout schemas from disk. The tests skip (pass vacuously) when
//! the git-ignored `./misskey` checkout is absent, e.g. in clean clones.

use serde_json::{Map, Value};

use crate::activitypub::PACKER_VERSION;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::drive_folder::DriveFolder;
use crate::domain::follow::Follow;
use crate::domain::instance::InviteCode;
use crate::domain::media::MediaMeta;
use crate::domain::moderation::{AbuseReport, ModerationLogEntry};
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::reaction::Reaction;
use crate::domain::visibility::{ViewerContext, is_visible};
use crate::domain::webhook::SystemWebhook;
use crate::domain::webhook::UserWebhook;

/// Misskey server version we claim compatibility with.

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).
use super::user::default_policies;

/// Instance metadata: `MetaLite` always, plus `MetaDetailedOnly` when
/// `detail` is true (`meta.ts`). `disableRegistration` and
/// `maxNoteTextLength` read [`InstanceSettings`] (WP2); everything else
/// keeps the pre-batch constants.
pub fn pack_meta(services: &AppServices, detail: bool) -> Result<Value, PortError> {
    let settings = services.instance_settings.get()?;
    let uri = services.public_url.trim_end_matches('/').to_string();
    let mut obj = Map::new();
    obj.insert("maintainerName".to_string(), Value::Null);
    obj.insert("maintainerEmail".to_string(), Value::Null);
    obj.insert(
        "version".to_string(),
        Value::String(PACKER_VERSION.to_string()),
    );
    obj.insert("providesTarball".to_string(), Value::Bool(false));
    obj.insert("name".to_string(), Value::String("Pinetree".to_string()));
    obj.insert(
        "shortName".to_string(),
        Value::String("Pinetree".to_string()),
    );
    obj.insert("uri".to_string(), Value::String(uri.clone()));
    obj.insert("description".to_string(), Value::Null);
    obj.insert(
        "langs".to_string(),
        Value::Array(vec![Value::String("en".to_string())]),
    );
    obj.insert("tosUrl".to_string(), Value::Null);
    obj.insert("repositoryUrl".to_string(), Value::Null);
    obj.insert("feedbackUrl".to_string(), Value::Null);
    obj.insert("defaultDarkTheme".to_string(), Value::Null);
    obj.insert("defaultLightTheme".to_string(), Value::Null);
    obj.insert(
        "clientOptions".to_string(),
        serde_json::json!({
            "entrancePageStyle": "simple",
            "showTimelineForVisitor": true,
            "showActivitiesForVisitor": true,
        }),
    );
    obj.insert(
        "disableRegistration".to_string(),
        Value::Bool(settings.registration.disable_registration()),
    );
    obj.insert("emailRequiredForSignup".to_string(), Value::Bool(false));
    obj.insert("enableHcaptcha".to_string(), Value::Bool(false));
    obj.insert("hcaptchaSiteKey".to_string(), Value::Null);
    obj.insert("enableMcaptcha".to_string(), Value::Bool(false));
    obj.insert("mcaptchaSiteKey".to_string(), Value::Null);
    obj.insert("mcaptchaInstanceUrl".to_string(), Value::Null);
    obj.insert("enableRecaptcha".to_string(), Value::Bool(false));
    obj.insert("recaptchaSiteKey".to_string(), Value::Null);
    obj.insert("enableTurnstile".to_string(), Value::Bool(false));
    obj.insert("turnstileSiteKey".to_string(), Value::Null);
    obj.insert("enableTestcaptcha".to_string(), Value::Bool(false));
    obj.insert("googleAnalyticsMeasurementId".to_string(), Value::Null);
    obj.insert("swPublickey".to_string(), Value::Null);
    obj.insert(
        "mascotImageUrl".to_string(),
        Value::String("/assets/ai.png".to_string()),
    );
    obj.insert("bannerUrl".to_string(), Value::Null);
    obj.insert("serverErrorImageUrl".to_string(), Value::Null);
    obj.insert("infoImageUrl".to_string(), Value::Null);
    obj.insert("notFoundImageUrl".to_string(), Value::Null);
    obj.insert("iconUrl".to_string(), Value::Null);
    obj.insert(
        "maxNoteTextLength".to_string(),
        Value::Number(settings.max_note_text_length.into()),
    );
    obj.insert("ads".to_string(), Value::Array(Vec::new()));
    obj.insert("notesPerOneAd".to_string(), Value::Number(0.into()));
    obj.insert("enableEmail".to_string(), Value::Bool(false));
    obj.insert("enableServiceWorker".to_string(), Value::Bool(true));
    obj.insert("translatorAvailable".to_string(), Value::Bool(false));
    obj.insert("sentryForFrontend".to_string(), Value::Null);
    obj.insert(
        "mediaProxy".to_string(),
        Value::String(format!("{uri}/proxy")),
    );
    obj.insert("enableUrlPreview".to_string(), Value::Bool(false));
    obj.insert("backgroundImageUrl".to_string(), Value::Null);
    obj.insert("impressumUrl".to_string(), Value::Null);
    obj.insert("logoImageUrl".to_string(), Value::Null);
    obj.insert("privacyPolicyUrl".to_string(), Value::Null);
    obj.insert("inquiryUrl".to_string(), Value::Null);
    obj.insert("serverRules".to_string(), Value::Array(Vec::new()));
    obj.insert("themeColor".to_string(), Value::Null);
    obj.insert("policies".to_string(), default_policies(services));
    obj.insert(
        "noteSearchableScope".to_string(),
        Value::String("local".to_string()),
    );
    obj.insert(
        "maxFileSize".to_string(),
        Value::Number(services.max_file_size.into()),
    );
    obj.insert("federation".to_string(), Value::String("all".to_string()));
    if detail {
        obj.insert(
            "features".to_string(),
            serde_json::json!({
                "registration": true,
                "emailRequiredForSignup": false,
                "localTimeline": true,
                "globalTimeline": true,
                "hcaptcha": false,
                "turnstile": false,
                "recaptcha": false,
                "objectStorage": false,
                "serviceWorker": true,
                "miauth": true,
            }),
        );
        obj.insert("proxyAccountName".to_string(), Value::Null);
        obj.insert("requireSetup".to_string(), Value::Bool(false));
        obj.insert("cacheRemoteFiles".to_string(), Value::Bool(false));
        obj.insert("cacheRemoteSensitiveFiles".to_string(), Value::Bool(false));
    }
    Ok(Value::Object(obj))
}
