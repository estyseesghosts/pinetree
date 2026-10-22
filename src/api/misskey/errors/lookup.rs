//! Error code lookup (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use crate::application::ports::PortError;

/// Known Misskey codes that answer `400 Bad Request` with `kind: "client"`.
pub(crate) fn is_known_client_code(code: &str) -> bool {
    matches!(
        code,
        "NO_SUCH_NOTE"
            | "NO_SUCH_USER"
            | "CONTENT_RESTRICTED_BY_USER"
            | "CONTENT_RESTRICTED_BY_SERVER"
            | "CANNOT_REPORT_YOURSELF"
            | "CANNOT_REPORT_THE_ADMIN"
            | "NO_SUCH_ABUSE_REPORT"
            | "NO_SUCH_INVITE_CODE"
            | "NO_SUCH_REPLY_TARGET"
            | "NO_SUCH_RENOTE_TARGET"
            | "NO_SUCH_FILE"
            | "NO_SUCH_CHANNEL"
            | "CONTAINS_PROHIBITED_WORDS"
            | "CONTAINS_TOO_MANY_MENTIONS"
            | "FOLLOWEE_IS_YOURSELF"
            | "FOLLOWER_IS_YOURSELF"
            | "ALREADY_FOLLOWING"
            | "BLOCKEE_IS_YOURSELF"
            | "ALREADY_BLOCKING"
            | "NOT_BLOCKING"
            | "MUTEE_IS_YOURSELF"
            | "ALREADY_MUTING"
            | "NOT_MUTING"
            | "NO_FOLLOW_REQUEST"
            | "NOT_FOLLOWING"
            | "ALREADY_REACTED"
            | "NOT_REACTED"
            | "ALREADY_FAVORITED"
            | "NOT_FAVORITED"
            | "CANNOT_REACT_TO_RENOTE"
            | "CANNOT_RENOTE"
            | "CANNOT_RENOTE_TO_A_PURE_RENOTE"
            | "CANNOT_RENOTE_DUE_TO_VISIBILITY"
            | "BOTH_WITH_REPLIES_AND_WITH_FILES"
            | "CANNOT_REPLY_TO_AN_INVISIBLE_NOTE"
            | "CANNOT_REPLY_TO_A_PURE_RENOTE"
            | "YOU_HAVE_BEEN_BLOCKED"
            | "BLOCKING"
            | "BLOCKED"
            | "NO_SUCH_AVATAR"
            | "NO_SUCH_BANNER"
            | "AVATAR_NOT_AN_IMAGE"
            | "BANNER_NOT_AN_IMAGE"
            | "NO_SUCH_PAGE"
            | "INVALID_REGEXP"
            | "TOO_MANY_MUTED_WORDS"
            | "URI_NULL"
            | "FORBIDDEN_TO_SET_YOURSELF"
            | "RESTRICTED_BY_ROLE"
            | "YOUR_NAME_CONTAINS_PROHIBITED_WORDS"
            | "YOUR_ACCOUNT_MOVED"
            | "UNSUPPORTED_FIELD"
            | "INVALID_PARAM"
            | "FORBIDDEN"
            | "REACTIONS_NOT_PUBLIC"
            | "IS_REMOTE_USER"
            | "NO_POLL"
            | "INVALID_CHOICE"
            | "ALREADY_VOTED"
            | "ALREADY_EXPIRED"
            | "CANNOT_CREATE_ALREADY_EXPIRED_POLL"
            | "TOO_MANY_DRAFTS"
            | "TOO_MANY_SCHEDULED_NOTES"
            | "SCHEDULED_AT_REQUIRED"
            | "SCHEDULED_AT_MUST_BE_IN_FUTURE"
            | "NO_SUCH_NOTE_DRAFT"
            | "PIN_LIMIT_EXCEEDED"
            | "ALREADY_PINNED"
            | "NO_SUCH_EMOJI"
            | "DUPLICATE_NAME"
            | "SAME_NAME_EMOJI_EXISTS"
            | "UNSUPPORTED_FILE_TYPE"
            | "NO_SUCH_ANTENNA"
            | "EMPTY_KEYWORD"
            | "TOO_MANY_ANTENNAS"
            | "NO_SUCH_USER_LIST"
            | "NO_SUCH_LIST"
            | "TOO_MANY_USERLISTS"
            | "TOO_MANY_USERS"
            | "ALREADY_ADDED"
            | "REMOTE_USER_NOT_ALLOWED"
            | "SCHEDULE_NOTE_MAX"
            | "CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE"
            | "FILE_REQUIRED"
            | "INVALID_FILE_NAME"
            | "UNALLOWED_FILE_TYPE"
            // `notes/edit` (Sharkey `edit.ts` verbatim) + §3 quote-disable.
            | "NEEDS_EDIT_ID"
            | "YOU_ARE_NOT_THE_AUTHOR"
            | "NOT_LOCAL_USER"
            | "CANNOT_QUOTE_THE_CURRENT_NOTE"
            | "CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE"
            | "QUOTE_DISABLED_FOR_USER"
            | "CANNOT_PRIVATE_RENOTE"
            | "CANNOT_RENOTE_OUTSIDE_OF_CHANNEL"
            | "CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY"
            | "MAX_LENGTH"
            | "MAX_CW_LENGTH"
            // Gaps final wiring (WP2a/WP2b/WP2c/WP3 arms): 2fa, apps,
            // push subscriptions, admin families, gallery/flash,
            // hashtags, announcements, drive file extras.
            | "INCORRECT_PASSWORD"
            | "INVALID_TOKEN"
            | "NO_SUCH_2FA"
            | "NO_SUCH_APP"
            | "NO_SUCH_SUBSCRIPTION"
            | "NO_SUCH_ROLE"
            | "NO_SUCH_AD"
            | "NO_SUCH_ANNOUNCEMENT"
            | "NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT"
            | "NO_SUCH_ENDPOINT"
            | "NO_SUCH_HASHTAG"
            | "NO_SUCH_POST"
            | "NO_SUCH_FLASH"
    )
}
/// (message, id, http_status, kind) for every mapped Misskey error code.
pub(crate) fn lookup_known_code(
    code: &str,
) -> Option<(&'static str, &'static str, u16, &'static str)> {
    Some(match code {
        "UNKNOWN_API_ENDPOINT" => (
            "Unknown API endpoint.",
            UNKNOWN_API_ENDPOINT_ID,
            404,
            "client",
        ),
        "AUTHENTICATION_FAILED" => (
            "Authentication failed. Please ensure your token is correct.",
            AUTHENTICATION_FAILED_ID,
            401,
            "client",
        ),
        "CREDENTIAL_REQUIRED" => (
            "Credential required.",
            CREDENTIAL_REQUIRED_ID,
            401,
            "client",
        ),
        "invalid_token" => (
            "Authentication failed. Please ensure your token is correct.",
            AUTHENTICATION_FAILED_ID,
            401,
            "client",
        ),
        "PERMISSION_DENIED" => (
            "Your app does not have the necessary permissions to use this endpoint.",
            PERMISSION_DENIED_ID,
            403,
            "permission",
        ),
        "ACCESS_DENIED" => ("Access denied.", ACCESS_DENIED_ID, 403, "permission"),
        "USER_SUSPENDED" | "YOUR_ACCOUNT_SUSPENDED" => (
            "Your account has been suspended.",
            USER_SUSPENDED_ID,
            403,
            "permission",
        ),
        "RATE_LIMIT_EXCEEDED" => (
            "Rate limit exceeded. Please try again later.",
            RATE_LIMIT_EXCEEDED_ID,
            429,
            "client",
        ),
        "MAX_FILE_SIZE_EXCEEDED" => (
            "Max file size exceeded.",
            MAX_FILE_SIZE_EXCEEDED_ID,
            413,
            "client",
        ),
        "NO_SUCH_NOTE" => ("No such note.", NO_SUCH_NOTE_ID, 400, "client"),
        "NO_SUCH_USER" => ("No such user.", NO_SUCH_USER_ID, 400, "client"),
        // Batch Q Phase 1 item 3: Misskey `notes/show.ts:32-42` verbatim
        // (message + id; Misskey default client status 400).
        "CONTENT_RESTRICTED_BY_USER" => (
            "Content restricted by user. Please sign in to view.",
            CONTENT_RESTRICTED_BY_USER_ID,
            400,
            "client",
        ),
        "CONTENT_RESTRICTED_BY_SERVER" => (
            "Content restricted by server settings. Please sign in to view.",
            CONTENT_RESTRICTED_BY_SERVER_ID,
            400,
            "client",
        ),
        "CANNOT_REPORT_YOURSELF" => (
            "Cannot report yourself.",
            CANNOT_REPORT_YOURSELF_ID,
            400,
            "client",
        ),
        "CANNOT_REPORT_THE_ADMIN" => (
            "Cannot report the admin.",
            CANNOT_REPORT_THE_ADMIN_ID,
            400,
            "client",
        ),
        "NO_SUCH_ABUSE_REPORT" => (
            "No such abuse report.",
            NO_SUCH_ABUSE_REPORT_ID,
            404,
            "server",
        ),
        "NO_SUCH_INVITE_CODE" => (
            "No such invite code.",
            NO_SUCH_INVITE_CODE_ID,
            400,
            "client",
        ),
        "CAN_NOT_DELETE_INVITE_CODE" => (
            "You can't delete this invite code.",
            CAN_NOT_DELETE_INVITE_CODE_ID,
            400,
            "client",
        ),
        "INVALID_DATE_TIME" => (
            "Invalid date-time format",
            INVALID_DATE_TIME_ID,
            400,
            "client",
        ),
        "NO_SUCH_AVATAR" => ("No such avatar file.", NO_SUCH_AVATAR_ID, 400, "client"),
        "NO_SUCH_BANNER" => ("No such banner file.", NO_SUCH_BANNER_ID, 400, "client"),
        "AVATAR_NOT_AN_IMAGE" => (
            "The file specified as an avatar is not an image.",
            AVATAR_NOT_AN_IMAGE_ID,
            400,
            "client",
        ),
        "BANNER_NOT_AN_IMAGE" => (
            "The file specified as a banner is not an image.",
            BANNER_NOT_AN_IMAGE_ID,
            400,
            "client",
        ),
        "NO_SUCH_PAGE" => ("No such page.", NO_SUCH_PAGE_ID, 400, "client"),
        "INVALID_REGEXP" => (
            "Invalid Regular Expression.",
            INVALID_REGEXP_ID,
            400,
            "client",
        ),
        "TOO_MANY_MUTED_WORDS" => (
            "Too many muted words.",
            TOO_MANY_MUTED_WORDS_ID,
            400,
            "client",
        ),
        "URI_NULL" => ("User ActivityPup URI is null.", URI_NULL_ID, 400, "client"),
        "FORBIDDEN_TO_SET_YOURSELF" => (
            "You can't set yourself as your own alias.",
            FORBIDDEN_TO_SET_YOURSELF_ID,
            400,
            "client",
        ),
        "RESTRICTED_BY_ROLE" => (
            "This feature is restricted by your role.",
            RESTRICTED_BY_ROLE_ID,
            400,
            "client",
        ),
        "YOUR_NAME_CONTAINS_PROHIBITED_WORDS" => (
            "Your new name contains prohibited words.",
            YOUR_NAME_CONTAINS_PROHIBITED_WORDS_ID,
            422,
            "client",
        ),
        "YOUR_ACCOUNT_MOVED" => (
            "You have moved your account.",
            YOUR_ACCOUNT_MOVED_ID,
            403,
            "permission",
        ),
        "NO_FOLLOW_REQUEST" => ("No follow request.", NO_FOLLOW_REQUEST_ID, 400, "client"),
        "NO_SUCH_REPLY_TARGET" => (
            "No such reply target.",
            NO_SUCH_REPLY_TARGET_ID,
            400,
            "client",
        ),
        "NO_SUCH_RENOTE_TARGET" => (
            "No such renote target.",
            NO_SUCH_RENOTE_TARGET_ID,
            400,
            "client",
        ),
        "NO_SUCH_FILE" => ("Some files are not found.", NO_SUCH_FILE_ID, 400, "client"),
        "CONTAINS_PROHIBITED_WORDS" => (
            "Cannot post because it contains prohibited words.",
            CONTAINS_PROHIBITED_WORDS_ID,
            400,
            "client",
        ),
        "FOLLOWEE_IS_YOURSELF" => (
            "Followee is yourself.",
            FOLLOWEE_IS_YOURSELF_ID,
            400,
            "client",
        ),
        "FOLLOWER_IS_YOURSELF" => (
            "Follower is yourself.",
            FOLLOWER_IS_YOURSELF_ID,
            400,
            "client",
        ),
        "ALREADY_FOLLOWING" => (
            "You are already following that user.",
            ALREADY_FOLLOWING_ID,
            400,
            "client",
        ),
        "NOT_FOLLOWING" => (
            "You are not following that user.",
            NOT_FOLLOWING_ID,
            400,
            "client",
        ),
        "BLOCKEE_IS_YOURSELF" => (
            "Blockee is yourself.",
            BLOCKEE_IS_YOURSELF_ID,
            400,
            "client",
        ),
        "ALREADY_BLOCKING" => (
            "You are already blocking that user.",
            ALREADY_BLOCKING_ID,
            400,
            "client",
        ),
        "NOT_BLOCKING" => (
            "You are not blocking that user.",
            NOT_BLOCKING_ID,
            400,
            "client",
        ),
        "MUTEE_IS_YOURSELF" => ("Mutee is yourself.", MUTEE_IS_YOURSELF_ID, 400, "client"),
        "ALREADY_MUTING" => (
            "You are already muting that user.",
            ALREADY_MUTING_ID,
            400,
            "client",
        ),
        "NOT_MUTING" => (
            "You are not muting that user.",
            NOT_MUTING_ID,
            400,
            "client",
        ),
        "BLOCKING" => ("You are blocking that user.", BLOCKING_ID, 400, "client"),
        "BLOCKED" => ("You are blocked by that user.", BLOCKED_ID, 400, "client"),
        "ALREADY_REACTED" => (
            "You are already reacting to that note.",
            ALREADY_REACTED_ID,
            400,
            "client",
        ),
        "NOT_REACTED" => (
            "You are not reacting to that note.",
            NOT_REACTED_ID,
            400,
            "client",
        ),
        "ALREADY_FAVORITED" => (
            "You have already favorited that note.",
            ALREADY_FAVORITED_ID,
            400,
            "client",
        ),
        "NOT_FAVORITED" => (
            "You have not favorited that note.",
            NOT_FAVORITED_ID,
            400,
            "client",
        ),
        "CANNOT_REACT_TO_RENOTE" => (
            "You cannot react to Renote.",
            CANNOT_REACT_TO_RENOTE_ID,
            400,
            "client",
        ),
        "CANNOT_RENOTE" | "CANNOT_RENOTE_TO_A_PURE_RENOTE" | "CANNOT_RENOTE_DUE_TO_VISIBILITY" => (
            "You can not Renote due to target visibility.",
            CANNOT_RENOTE_ID,
            400,
            "client",
        ),
        "BOTH_WITH_REPLIES_AND_WITH_FILES" => (
            "Specifying both withReplies and withFiles is not supported.",
            BOTH_WITH_REPLIES_AND_WITH_FILES_ID,
            400,
            "client",
        ),
        "YOU_HAVE_BEEN_BLOCKED" => (
            "You have been blocked by this user.",
            YOU_HAVE_BEEN_BLOCKED_ID,
            400,
            "client",
        ),
        "UNSUPPORTED_FIELD" => ("Unsupported field.", UNSUPPORTED_FIELD_ID, 400, "client"),
        "INVALID_PARAM" => ("Invalid param.", INVALID_PARAM_ID, 400, "client"),
        // `users/followers` + `users/following` visibility gate (Misskey
        // `FORBIDDEN`, kind `client` → HTTP 400 per `ApiCallService`
        // mapping; the two upstream endpoints carry different UUIDs for
        // the same code — pinetree emits the `users/followers` one, see
        // `FORBIDDEN_ID`).
        "FORBIDDEN" => ("Forbidden.", FORBIDDEN_ID, 400, "client"),
        // `users/reactions` gates (Misskey `reactions.ts` verbatim).
        "REACTIONS_NOT_PUBLIC" => (
            "Reactions of the user is not public.",
            REACTIONS_NOT_PUBLIC_ID,
            400,
            "client",
        ),
        "IS_REMOTE_USER" => (
            "Currently unavailable to display reactions of remote users. See https://github.com/misskey-dev/misskey/issues/12964",
            IS_REMOTE_USER_ID,
            400,
            "client",
        ),
        // `notes/polls/vote` gates (Misskey `polls/vote.ts` verbatim).
        "NO_POLL" => (
            "The note does not attach a poll.",
            NO_POLL_ID,
            400,
            "client",
        ),
        "INVALID_CHOICE" => ("Choice ID is invalid.", INVALID_CHOICE_ID, 400, "client"),
        "ALREADY_VOTED" => ("You have already voted.", ALREADY_VOTED_ID, 400, "client"),
        "ALREADY_EXPIRED" => (
            "The poll is already expired.",
            ALREADY_EXPIRED_ID,
            400,
            "client",
        ),
        // `notes/create` poll gate (Misskey `create.ts` verbatim).
        "CANNOT_CREATE_ALREADY_EXPIRED_POLL" => (
            "Poll is already expired.",
            CANNOT_CREATE_ALREADY_EXPIRED_POLL_ID,
            400,
            "client",
        ),
        // Sharkey `notes/schedule/*` gates (verbatim code/message/id).
        // `CANNOT_REPLY_TO_A_PURE_RENOTE` is also emitted by
        // `notes/create` (same code, now with the verbatim UUID).
        "CANNOT_REPLY_TO_A_PURE_RENOTE" => (
            "You can not reply to a pure Renote.",
            CANNOT_REPLY_TO_A_PURE_RENOTE_ID,
            400,
            "client",
        ),
        "CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE" => (
            "Schedule is already expired.",
            CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE_ID,
            400,
            "client",
        ),
        "SCHEDULE_NOTE_MAX" => ("Schedule note max.", SCHEDULE_NOTE_MAX_ID, 400, "client"),
        // `notes/drafts/*` gates (Misskey `drafts/create|update.ts`
        // verbatim code/message; the create-endpoint UUID is canonical).
        "TOO_MANY_DRAFTS" => (
            "You cannot create drafts any more.",
            TOO_MANY_DRAFTS_ID,
            400,
            "client",
        ),
        "TOO_MANY_SCHEDULED_NOTES" => (
            "You cannot create scheduled notes any more.",
            TOO_MANY_SCHEDULED_NOTES_ID,
            400,
            "client",
        ),
        "SCHEDULED_AT_REQUIRED" => (
            "scheduledAt is required when isActuallyScheduled is true.",
            SCHEDULED_AT_REQUIRED_ID,
            400,
            "client",
        ),
        "SCHEDULED_AT_MUST_BE_IN_FUTURE" => (
            "scheduledAt must be in the future.",
            SCHEDULED_AT_MUST_BE_IN_FUTURE_ID,
            400,
            "client",
        ),
        "NO_SUCH_NOTE_DRAFT" => ("No such note draft.", NO_SUCH_NOTE_DRAFT_ID, 400, "client"),
        // `i/pin.ts` gates (verbatim code/message/id).
        "PIN_LIMIT_EXCEEDED" => (
            "You can not pin notes any more.",
            PIN_LIMIT_EXCEEDED_ID,
            400,
            "client",
        ),
        "ALREADY_PINNED" => (
            "That note has already been pinned.",
            ALREADY_PINNED_ID,
            400,
            "client",
        ),
        // `admin/emoji/*` gates (verbatim upstream code/message/id).
        "NO_SUCH_EMOJI" => ("No such emoji.", NO_SUCH_EMOJI_ID, 400, "client"),
        "DUPLICATE_NAME" => ("Duplicate name.", DUPLICATE_NAME_ID, 400, "client"),
        "SAME_NAME_EMOJI_EXISTS" => (
            "Emoji that have same name already exists.",
            SAME_NAME_EMOJI_EXISTS_ID,
            400,
            "client",
        ),
        "UNSUPPORTED_FILE_TYPE" => (
            "Unsupported file type.",
            UNSUPPORTED_FILE_TYPE_ID,
            400,
            "client",
        ),
        // `antennas/*` gates (verbatim upstream code/message/id).
        "NO_SUCH_ANTENNA" => ("No such antenna.", NO_SUCH_ANTENNA_ID, 400, "client"),
        "EMPTY_KEYWORD" => (
            "Either keywords or excludeKeywords is required.",
            EMPTY_KEYWORD_ID,
            400,
            "client",
        ),
        "TOO_MANY_ANTENNAS" => (
            "You cannot create antenna any more.",
            TOO_MANY_ANTENNAS_ID,
            400,
            "client",
        ),
        "NO_SUCH_USER_LIST" => ("No such user list.", NO_SUCH_USER_LIST_ID, 400, "client"),
        "NO_SUCH_LIST" => ("No such list.", LIST_NO_SUCH_LIST_SHOW_ID, 400, "client"),
        "TOO_MANY_USERLISTS" => (
            "You cannot create user list any more.",
            LIST_TOO_MANY_USERLISTS_ID,
            400,
            "client",
        ),
        "TOO_MANY_USERS" => (
            "You can not push users any more.",
            LIST_TOO_MANY_USERS_ID,
            400,
            "client",
        ),
        "ALREADY_ADDED" => (
            "That user has already been added to that list.",
            LIST_ALREADY_ADDED_ID,
            400,
            "client",
        ),
        "REMOTE_USER_NOT_ALLOWED" => (
            "Not allowed to load the remote user's list",
            LIST_REMOTE_USER_NOT_ALLOWED_ID,
            400,
            "client",
        ),
        // `notes/edit` gates (Sharkey `edit.ts` verbatim code/message/id).
        "NEEDS_EDIT_ID" => (
            "You need to specify `editId`.",
            NEEDS_EDIT_ID,
            400,
            "client",
        ),
        "YOU_ARE_NOT_THE_AUTHOR" => (
            "You are not the author of this note.",
            YOU_ARE_NOT_THE_AUTHOR_ID,
            400,
            "client",
        ),
        "NOT_LOCAL_USER" => (
            "You are not a local user.",
            NOT_LOCAL_USER_ID,
            400,
            "client",
        ),
        "CANNOT_QUOTE_THE_CURRENT_NOTE" => (
            "Cannot quote the current note.",
            CANNOT_QUOTE_THE_CURRENT_NOTE_ID,
            400,
            "client",
        ),
        "CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE" => (
            "Cannot quote a quote of edited note.",
            CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE_ID,
            400,
            "client",
        ),
        // §3 quote-disable (Sharkey `notes/create.ts` + `notes/edit.ts`
        // verbatim — same code/message/id on both endpoints).
        "QUOTE_DISABLED_FOR_USER" => (
            "You do not have permission to create quote posts.",
            QUOTE_DISABLED_FOR_USER_ID,
            400,
            "client",
        ),
        "CANNOT_PRIVATE_RENOTE" => (
            "You can not perform a private renote.",
            CANNOT_PRIVATE_RENOTE_ID,
            400,
            "client",
        ),
        "CANNOT_RENOTE_OUTSIDE_OF_CHANNEL" => (
            "Cannot renote outside of channel.",
            CANNOT_RENOTE_OUTSIDE_OF_CHANNEL_ID,
            400,
            "client",
        ),
        "CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY" => (
            "You cannot reply to a specified visibility note with extended visibility.",
            CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY_ID,
            400,
            "client",
        ),
        "MAX_LENGTH" => (
            "You tried posting a note which is too long.",
            MAX_LENGTH_ID,
            400,
            "client",
        ),
        "MAX_CW_LENGTH" => (
            "You tried posting a content warning which is too long.",
            MAX_CW_LENGTH_ID,
            400,
            "client",
        ),
        "INTERNAL_ERROR" => (
            "Internal error occurred. Please contact us if the error persists.",
            INTERNAL_ERROR_ID,
            500,
            "server",
        ),
        // Gaps final wiring (WP2a/WP2b/WP2c/WP3 arms; pinetree-minted
        // UUIDs — `// TODO(final): verify against misskey` on each const).
        "INCORRECT_PASSWORD" => ("Incorrect password.", INCORRECT_PASSWORD_ID, 400, "client"),
        "INVALID_TOKEN" => ("Invalid token.", INVALID_TOKEN_ID, 400, "client"),
        "NO_SUCH_2FA" => (
            "No such two-factor authentication.",
            NO_SUCH_2FA_ID,
            400,
            "client",
        ),
        "NO_SUCH_APP" => ("No such app.", NO_SUCH_APP_ID, 400, "client"),
        "NO_SUCH_SUBSCRIPTION" => (
            "No such subscription.",
            NO_SUCH_SUBSCRIPTION_ID,
            400,
            "client",
        ),
        "NO_SUCH_ROLE" => ("No such role.", NO_SUCH_ROLE_ID, 400, "client"),
        "NO_SUCH_AD" => ("No such ad.", NO_SUCH_AD_ID, 400, "client"),
        "NO_SUCH_ANNOUNCEMENT" => (
            "No such announcement.",
            NO_SUCH_ANNOUNCEMENT_ID,
            400,
            "client",
        ),
        "NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT" => (
            "No such abuse report notification recipient.",
            NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT_ID,
            400,
            "client",
        ),
        "NO_SUCH_ENDPOINT" => ("No such endpoint.", NO_SUCH_ENDPOINT_ID, 404, "client"),
        "NO_SUCH_HASHTAG" => ("No such hashtag.", NO_SUCH_HASHTAG_ID, 400, "client"),
        "NO_SUCH_POST" => ("No such post.", NO_SUCH_POST_ID, 400, "client"),
        "NO_SUCH_FLASH" => ("No such flash.", NO_SUCH_FLASH_ID, 400, "client"),
        _ => return None,
    })
}
/// Leading `LOUD_SNAKE` token of a domain message, if any.
pub(crate) fn code_token(msg: &str) -> Option<String> {
    let token: String = msg
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if token.is_empty() {
        return None;
    }
    // Accept `SCREAMING_SNAKE` idents and lowercase auth shorthands.
    if token == "invalid_token"
        || (token
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
    {
        Some(token)
    } else {
        None
    }
}
/// Deterministic placeholder id for codes without a Misskey-issued UUID.
pub(crate) fn new_id_for(code: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"pinetree-api-error:");
    hasher.update(code.as_bytes());
    let digest = hasher.finalize();
    let bytes: [u8; 16] = digest[..16].try_into().unwrap_or([0u8; 16]);
    // Render as UUID-shaped text (version nibble fixed to 4).
    let mut b = bytes;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}
