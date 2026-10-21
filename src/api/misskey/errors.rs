//! Misskey API error envelope (Slice D1).
//!
//! Matches Misskey 2026.10 (`ApiCallService.ts:306-325`, `error.ts:6-32`)
//! and `docs/endpoint-manifest.md` §1: `{ error: { message, code, id, kind,
//! info? } }` over HTTP with the status mapped from the error.
//!
//! The serialized body carries **both** top-level `id`/`code`/`message`
//! and the nested `error` object so Misskey SDK clients reading
//! `body.error.{id,code,message,kind}` and callers reading top-level
//! `res["code"]` both work.

use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::application::ports::PortError;

// Stable Misskey error idents. UUIDs are taken from the Misskey 2026.10
// checkout where the endpoint defines them; the rest are fixed pinetree
// UUIDs that never change once shipped.
pub const UNKNOWN_API_ENDPOINT_ID: &str = "2ca3b769-b6d7-4b65-96c9-302f3620a591";
pub const AUTHENTICATION_FAILED_ID: &str = "b0a7f5f8-dc2f-4171-b91f-de88ad238e14";
pub const CREDENTIAL_REQUIRED_ID: &str = "1384574d-a912-4b81-8601-c7b1c4085df1";
pub const PERMISSION_DENIED_ID: &str = "1370e5b7-d4eb-4566-bb1d-7748ee6a1838";
pub const ACCESS_DENIED_ID: &str = "56f35758-7dd5-468b-8439-5d6fb8ec9b8e";
pub const USER_SUSPENDED_ID: &str = "a8c724b3-6e9c-4b46-b1a8-bc3ed6258370";
/// Misskey `SigninApiService.ts` idents (verbatim upstream UUIDs; the web
/// `signin-flow` password login, `HANDOFF_auth-signin.md`). Upstream mints
/// these inline (no `code`/`message`), so the codes/messages below are
/// pinetree-chosen; clients match on `id`.
pub const SIGNIN_NO_SUCH_USER_ID: &str = "6cc579cc-885d-43d8-95c2-b8c7fc963280";
pub const SIGNIN_SUSPENDED_ID: &str = "e03a5f46-d309-4865-9b69-56282d94e1eb";
pub const SIGNIN_INCORRECT_PASSWORD_ID: &str = "932c904e-9460-45b7-9ce6-7ed33be7eb2c";
pub const SIGNIN_RATE_LIMITED_ID: &str = "22d05606-fbcf-421a-a2db-b32610dcfd1b";
pub const RATE_LIMIT_EXCEEDED_ID: &str = "d5826d14-3982-4d2e-8011-b9e9f02499ef";
pub const INVALID_PARAM_ID: &str = "3d81ceae-475f-4600-b2a8-2bc116157532";
pub const UNSUPPORTED_FIELD_ID: &str = "9307f30c-4400-421c-bf2f-1e6a37d0f978";
pub const NO_SUCH_NOTE_ID: &str = "24fcbfc6-2e37-42b6-8388-c29b3861a08d";
pub const NO_SUCH_USER_ID: &str = "fcd2eef9-a9b2-4c4f-8624-038099e90aa5";
pub const CANNOT_REPORT_YOURSELF_ID: &str = "1e13149e-b1e8-43cf-902e-c01dbfcb202f";
pub const CANNOT_REPORT_THE_ADMIN_ID: &str = "35e166f5-05fb-4f87-a2d5-adb42676d48f";
pub const NO_SUCH_ABUSE_REPORT_ID: &str = "ac3794dd-2ce4-d878-e546-73c60c06b398";
pub const NO_SUCH_REPLY_TARGET_ID: &str = "749ee0f6-d3da-459a-bf02-282e2da4292c";
pub const NO_SUCH_RENOTE_TARGET_ID: &str = "b5c90186-4ab0-49c8-9bba-a1f76c282ba4";
pub const NO_SUCH_FILE_ID: &str = "b6992544-63e7-67f0-fa7f-32444b1b5306";
pub const CONTAINS_PROHIBITED_WORDS_ID: &str = "aa6e01d3-a85c-669d-758a-76aab43af334";
pub const FOLLOWEE_IS_YOURSELF_ID: &str = "26fbe7bb-a331-4857-af17-205b426669a9";
pub const FOLLOWER_IS_YOURSELF_ID: &str = "07dc03b9-03da-422d-885b-438313707662";
pub const INVALIDATE_NO_SUCH_USER_ID: &str = "b77e6ae6-a3e5-40da-9cc8-c240115479cc";
pub const INVALIDATE_NOT_FOLLOWING_ID: &str = "918faac3-074f-41ae-9c43-ed5d2946770d";
/// Batch P verbatim error UUIDs (`docs/handoff-batch-p.md`): email
/// identity loop, account move, registry, translate, versions.
pub const UPDATE_EMAIL_INCORRECT_PASSWORD_ID: &str = "e54c1d7e-e7d6-4103-86b6-0a95069b4ad3";
pub const UPDATE_EMAIL_UNAVAILABLE_ID: &str = "a2defefb-f220-8849-0af6-17f816099323";
pub const UPDATE_EMAIL_REQUIRED_ID: &str = "324c7a88-59f2-492f-903f-89134f93e47e";
pub const VERIFY_EMAIL_NO_SUCH_CODE_ID: &str = "97c1f576-e4b8-4b8a-a6dc-9cb65e7f6f85";
pub const MOVE_DESTINATION_FORBIDS_ID: &str = "b5c90186-4ab0-49c8-9bba-a1f766282ba4";
pub const MOVE_ROOT_FORBIDDEN_ID: &str = "4362e8dc-731f-4ad8-a694-be2a88922a24";
pub const MOVE_URI_NULL_ID: &str = "bf326f31-d430-4f97-9933-5d61e4d48a23";
pub const MOVE_LOCAL_URI_NULL_ID: &str = "95ba11b9-90e8-43a5-ba16-7acc1ab32e71";
pub const MOVE_ALREADY_MOVED_ID: &str = "b234a14e-9ebe-4581-8000-074b3c215962";
pub const REGISTRY_NO_SUCH_KEY_ID: &str = "ac3ed68a-62f0-422b-a7bc-d5e09e8f6a6a";
pub const EXT_RESOURCE_INVALID_SCHEMA_ID: &str = "bb774091-7a15-4a70-9dc5-6ac8cf125856";
pub const EXT_RESOURCE_HASH_MISMATCH_ID: &str = "693ba8ba-b486-40df-a174-72f8279b56a4";
pub const TRANSLATE_UNAVAILABLE_ID: &str = "50a70314-2d8a-431b-b433-efa5cc56444c";
pub const TRANSLATE_INVISIBLE_NOTE_ID: &str = "ea29f2ca-c368-43b3-aaf1-5ac3e74bbe5d";
/// First-party loud-failure UUIDs (dormant transports — no SMTP / no
/// translation backend configured; Batch P owner decision: fail loudly,
/// never silently drop).
pub const EMAIL_NOT_CONFIGURED_ID: &str = "c41d9a2e-6b3f-4e5a-9d8c-3f2a1b0c4d01";
pub const TRANSLATE_NOT_CONFIGURED_ID: &str = "d52ea1f3-7c4e-4b5d-8e9d-4a3b2c1d5e02";
/// Sharkey `notes/versions.ts` `SIGNIN_REQUIRED` (verbatim;
/// `NO_SUCH_NOTE` reuses the shared id).
pub const VERSIONS_SIGNIN_REQUIRED_ID: &str = "8e75455b-738c-471d-9f80-62693f33372e";
/// Misskey `notes/show.ts:32-42` verbatim errors (Batch Q Phase 1 item 3).
pub const CONTENT_RESTRICTED_BY_USER_ID: &str = "fbcc002d-37d9-4944-a6b0-d9e29f2d33ab";
pub const CONTENT_RESTRICTED_BY_SERVER_ID: &str = "145f88d2-b03d-4087-8143-a78928883c4b";
/// Misskey `users/show.ts:52-57` verbatim `noSuchUser` (Batch Q Phase 1
/// item 3; per-endpoint UUID + 404, upstream convention).
pub const USERS_SHOW_NO_SUCH_USER_ID: &str = "4362f8dc-731f-4ad8-a694-be5a88922a24";
/// Misskey `fetch-rss.ts` verbatim errors.
pub const FETCH_RSS_INVALID_URL_ID: &str = "89b7ee05-ccfc-4bdd-9b13-61172fd1e06c";
pub const FETCH_RSS_FAILED_ID: &str = "8db5d3d8-31d7-452f-b0cc-ca3b8925de12";
pub const FETCH_RSS_UNAVAILABLE_ID: &str = "91e6ff44-c63f-4725-9ad0-b7a40d7f7655";
/// Misskey `users/lists/*` + `notes/user-list-timeline` verbatim errors
/// (Batch O WP16; one UUID per endpoint per code, upstream convention).
/// `NO_SUCH_LIST` is a different code from pinetree's `NO_SUCH_USER_LIST`;
/// both exist here.
pub const LIST_NO_SUCH_LIST_UPDATE_ID: &str = "796666fe-3dff-4d39-becb-8a5932c1d5b7";
pub const LIST_NO_SUCH_LIST_DELETE_ID: &str = "78436795-db79-42f5-b1e2-55ea2cf19166";
pub const LIST_NO_SUCH_LIST_SHOW_ID: &str = "7bc05c21-1d7a-41ae-88f1-66820f4dc686";
pub const LIST_NO_SUCH_LIST_MEMBER_ID: &str = "7f44670e-ab16-43b8-b4c1-ccd2ee89cc02";
pub const LIST_TIMELINE_NO_SUCH_LIST_ID: &str = "8fb1fbd5-e476-4c37-9fb0-43d55b63a2ff";
pub const LIST_NO_SUCH_USER_LIST_FAVORITE_ID: &str = "7dbaf3cf-7b42-4b8f-b431-b3919e580dbe";
pub const LIST_NO_SUCH_USER_LIST_UNFAVORITE_ID: &str = "baedb33e-76b8-4b0c-86a8-9375c0a7b94b";
pub const LIST_NO_SUCH_USER_LIST_QUERY_ID: &str = "a8af4a82-0980-4cc4-a6af-8b0ffd54465e";
pub const LIST_NO_SUCH_USER_PUSH_ID: &str = "a89abd3d-f0bc-4cce-beb1-2f446f4f1e6a";
pub const LIST_NO_SUCH_USER_PULL_ID: &str = "588e7f72-c744-4a61-b180-d354e912bda2";
pub const LIST_TOO_MANY_USERLISTS_ID: &str = "0cf21a28-7715-4f39-a20d-777bfdb8d138";
pub const LIST_ALREADY_ADDED_ID: &str = "1de7c884-1595-49e9-857e-61f12f4d4fc5";
pub const LIST_YOU_HAVE_BEEN_BLOCKED_ID: &str = "990232c5-3f9d-4d83-9f3f-ef27b6332a4b";
pub const LIST_TOO_MANY_USERS_ID: &str = "2dd9752e-a338-413d-8eec-41814430989b";
pub const LIST_ALREADY_FAVORITED_ID: &str = "6425bba0-985b-461e-af1b-518070e72081";
pub const LIST_NOT_FAVORITED_ID: &str = "835c4b27-463d-4cfa-969b-a9058678d465";
pub const LIST_REMOTE_USER_NOT_ALLOWED_ID: &str = "53858f1b-3315-4a01-81b7-db9b48d4b79a";
pub const LIST_QUERY_INVALID_PARAM_ID: &str = "ab36de0e-29e9-48cb-9732-d82f1281620d";
pub const ALREADY_FOLLOWING_ID: &str = "35387507-38c7-4cb9-9197-300b93783fa0";pub const NO_FOLLOW_REQUEST_ID: &str = "bcde4f8b-0913-4614-8736-2e2d5fa29d5b";
pub const NOT_FOLLOWING_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b01";
pub const BLOCKEE_IS_YOURSELF_ID: &str = "88b19138-f28d-42c0-8499-6a31bbd0fdc6";
pub const ALREADY_BLOCKING_ID: &str = "787fed64-acb9-464a-82eb-afbd745b9614";
pub const NOT_BLOCKING_ID: &str = "291b2efa-60c6-45c0-9f6a-045c8f9b02cd";
pub const MUTEE_IS_YOURSELF_ID: &str = "a4619cb2-5f23-484b-9301-94c903074e10";
pub const ALREADY_MUTING_ID: &str = "7e7359cb-160c-4956-b08f-4d1c653cd007";
pub const NOT_MUTING_ID: &str = "5467d020-daa9-4553-81e1-135c0c35a96d4";
pub const NO_SUCH_AVATAR_ID: &str = "539f3a45-f215-4f81-a9a8-31293640207f";
pub const NO_SUCH_BANNER_ID: &str = "0d8f5629-f210-41c2-9433-735831a58595";
pub const AVATAR_NOT_AN_IMAGE_ID: &str = "f419f9f8-2f4d-46b1-9fb4-49d3a2fd7191";
pub const BANNER_NOT_AN_IMAGE_ID: &str = "75aedb19-2afd-4e6d-87fc-67941256fa60";
pub const NO_SUCH_PAGE_ID: &str = "8e01b590-7eb9-431b-a239-860e086c408e";
pub const INVALID_REGEXP_ID: &str = "0d786918-10df-41cd-8f33-8dec7d9a89a5";
pub const TOO_MANY_MUTED_WORDS_ID: &str = "010665b1-a211-42d2-bc64-8f6609d79785";
pub const URI_NULL_ID: &str = "bf326f31-d430-4f97-9933-5d61e4d48a23";
pub const FORBIDDEN_TO_SET_YOURSELF_ID: &str = "25c90186-4ab0-49c8-9bba-a1fa6c202ba4";
pub const RESTRICTED_BY_ROLE_ID: &str = "8feff0ba-5ab5-585b-31f4-4df816663fad";
pub const YOUR_NAME_CONTAINS_PROHIBITED_WORDS_ID: &str = "0b3f9f6a-2f4d-4b1f-9fb4-49d3a2fd7191";
pub const YOUR_ACCOUNT_MOVED_ID: &str = "56f20ec9-fd06-4fa5-841b-edd6d7d4fa31";
pub const BLOCKING_ID: &str = "4e2206ec-aa4f-4960-b865-6c23ac38e2d9";
pub const BLOCKED_ID: &str = "c4ab57cc-4e41-45e9-bfd9-584f61e35ce0";
pub const ALREADY_REACTED_ID: &str = "71efcf98-86d6-4e2b-b2ad-9d032369366b";
pub const NOT_REACTED_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b02";
/// Misskey `notes/favorites/create.ts` + `delete.ts` idents (verbatim upstream UUIDs).
pub const ALREADY_FAVORITED_ID: &str = "a402c12b-34dd-41d2-9d8d-4d2ffd96a1a6";
pub const NOT_FAVORITED_ID: &str = "b625fc69-635e-4e89-baa6-dbefbef35af5";
pub const CANNOT_REACT_TO_RENOTE_ID: &str = "eaccdc08-ddef-43fe-908f-d108faad57f5";
pub const CANNOT_RENOTE_ID: &str = "be9529e9-fe72-4de0-ae43-0b363c4938af";
pub const BOTH_WITH_REPLIES_AND_WITH_FILES_ID: &str = "91c8cb9f-36ed-46e7-9ca2-7df96ed6e222";
pub const YOU_HAVE_BEEN_BLOCKED_ID: &str = "b390d7e1-8a5e-46ed-b625-06271cafd3d3";
pub const MAX_FILE_SIZE_EXCEEDED_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b03";
/// Gaps final wiring: drive folders + folder-aware file legs (WP2a; one
/// UUID per endpoint per code, upstream convention; pinetree-minted —
/// `// TODO(final): verify against misskey`).
pub const FOLDER_CREATE_NO_SUCH_FOLDER_ID: &str = "17ebfd4a-6d3b-4318-bc8f-b605f418ee3f";
pub const FOLDER_SHOW_NO_SUCH_FOLDER_ID: &str = "4586e998-fca0-4962-af84-b6fa2d55390b";
pub const FOLDER_UPDATE_NO_SUCH_FOLDER_ID: &str = "4d52caa2-22de-4100-869e-4d366873955f";
pub const FOLDER_UPDATE_NO_SUCH_PARENT_FOLDER_ID: &str = "9309f3a8-d6a2-4a3a-841f-68a82eda4fe3";
pub const FOLDER_UPDATE_RECURSIVE_NESTING_ID: &str = "d651a096-ae0e-4024-86b6-68731935b8a8";
pub const FOLDER_DELETE_NO_SUCH_FOLDER_ID: &str = "157697f5-e645-44b2-b1bc-5ed4a86b3732";
pub const FOLDER_DELETE_HAS_CHILD_ID: &str = "07408c65-a7bb-4f27-a3a8-a200d34fdc37";
pub const MOVE_BULK_NO_SUCH_FOLDER_ID: &str = "f76a1131-8b94-4bd3-9b62-b81f9e1ef1e9";
pub const FILES_UPDATE_NO_SUCH_FOLDER_ID: &str = "efef2146-7ea9-44d2-90c5-9557983bf275";
/// Gaps final wiring: system/user webhooks (WP3), update-memo + thread
/// muting (WP2a). Same per-endpoint-UUID convention; pinetree-minted.
pub const SYSTEM_WEBHOOK_SHOW_NO_SUCH_ID: &str = "722a824a-ba81-4d2e-8f84-bbc358bfdb42";
pub const SYSTEM_WEBHOOK_UPDATE_NO_SUCH_ID: &str = "ae385176-36c5-4fa6-9ddb-8ae015a80a68";
pub const SYSTEM_WEBHOOK_DELETE_NO_SUCH_ID: &str = "1a43e12f-a743-46ca-b77e-0d173209da69";
pub const WEBHOOK_SHOW_NO_SUCH_ID: &str = "7213d0fd-c19d-47ac-b6b6-0317fb0ff85c";
pub const WEBHOOK_UPDATE_NO_SUCH_ID: &str = "b2598135-6132-4378-ba05-75c8ddcb25fc";
pub const WEBHOOK_DELETE_NO_SUCH_ID: &str = "68d3e74a-c19d-46ac-a585-6225a5d8fcce";
pub const WEBHOOK_CREATE_TOO_MANY_ID: &str = "adadc6d4-090b-4f02-a74a-fdb5d5a1f4c7";
pub const UPDATE_MEMO_NO_SUCH_USER_ID: &str = "52fc7755-ae05-435b-9d4b-7d20f4da926e";
pub const THREAD_MUTE_CREATE_NO_SUCH_NOTE_ID: &str = "3f694d2b-ff60-456f-b901-e72753194958";
pub const THREAD_MUTE_DELETE_NO_SUCH_NOTE_ID: &str = "f92bec2b-2f38-460e-8f68-b3e47fb0986d";
pub const INTERNAL_ERROR_ID: &str = "5d37dbcb-891e-41ca-a3d6-e690c97775ac";
pub const FORBIDDEN_ID: &str = "3c6a84db-d619-26af-ca14-06232a21df8a";
pub const REACTIONS_NOT_PUBLIC_ID: &str = "673a7dd2-6924-1093-e0c0-e68456ceae5c";
pub const IS_REMOTE_USER_ID: &str = "6b95fa98-8cf9-2350-e284-f0ffdb54a805";
/// Misskey `notes/polls/vote.ts` idents (verbatim upstream UUIDs).
pub const NO_POLL_ID: &str = "5f979967-52d9-4314-a911-1c673727f92f";
pub const INVALID_CHOICE_ID: &str = "e0cc9a04-f2e8-41e4-a5f1-4127293260cc";
pub const ALREADY_VOTED_ID: &str = "0963fc77-efac-419b-9424-b391608dc6d8";
pub const ALREADY_EXPIRED_ID: &str = "1022a357-b085-4054-9083-8f8de358337e";
/// Misskey `notes/create.ts` ident (verbatim upstream UUID).
pub const CANNOT_CREATE_ALREADY_EXPIRED_POLL_ID: &str = "04da457d-b083-4055-9082-955525eda5a5";
/// Misskey `notes/drafts/*` idents (verbatim upstream UUIDs; the
/// create-endpoint UUID is canonical — the update endpoint reuses the same
/// `code` with its own UUID, see the endpoint manifest §§16–17).
pub const TOO_MANY_DRAFTS_ID: &str = "9ee33bbe-fde3-4c71-9b51-e50492c6b9c8";
pub const TOO_MANY_SCHEDULED_NOTES_ID: &str = "22ae69eb-09e3-4541-a850-773cfa45e693";
pub const SCHEDULED_AT_REQUIRED_ID: &str = "15e28a55-e74c-4d65-89b7-8880cdaaa87d";
pub const SCHEDULED_AT_MUST_BE_IN_FUTURE_ID: &str = "e4bed6c9-017e-4934-aed0-01c22cc60ec1";
pub const NO_SUCH_NOTE_DRAFT_ID: &str = "49cd6b9d-848e-41ee-b0b9-adaca711a6b1";
/// Misskey `admin/emoji/*` idents (verbatim upstream UUIDs; Batch N WS1).
/// One UUID per code (§16 convention): `NO_SUCH_EMOJI` takes the update-endpoint
/// UUID, `NO_SUCH_FILE` on the add/update path takes the add-endpoint UUID.
pub const NO_SUCH_EMOJI_ID: &str = "684dec9d-a8c2-4364-9aa8-456c49cb1dc8";
pub const DUPLICATE_NAME_ID: &str = "f7a3462c-4e6e-4069-8421-b9bd4f4c3975";
pub const SAME_NAME_EMOJI_EXISTS_ID: &str = "7180fe9d-1ee3-bff9-647d-fe9896d2ffb8";
pub const UNSUPPORTED_FILE_TYPE_ID: &str = "f7599d96-8750-af68-1633-9575d625c1a7";
pub const EMOJI_ADD_NO_SUCH_FILE_ID: &str = "fc46b5a4-6b92-4c33-ac66-b806659bb5cf";
/// Misskey `antennas/*` + `i/import-antennas` idents (verbatim upstream
/// UUIDs; Batch N WS4). One UUID per code (§16 convention): `NO_SUCH_ANTENNA`
/// takes the update-endpoint UUID; the import endpoint mints its own
/// `TOO_MANY_ANTENNAS` UUID (`import_too_many_antennas`).
pub const NO_SUCH_ANTENNA_ID: &str = "10c673ac-8852-48eb-aa1f-f5b67f069290";
pub const EMPTY_KEYWORD_ID: &str = "53ee222e-1ddd-4f9a-92e5-9fb82ddb463a";
pub const TOO_MANY_ANTENNAS_ID: &str = "faf47050-e8b5-438c-913c-db2b1576fde4";
pub const NO_SUCH_USER_LIST_ID: &str = "95063e93-a283-4b8b-9aa5-bcdb8df69a7f";
pub const IMPORT_ANTENNAS_TOO_MANY_ID: &str = "600917d4-a4cb-4cc5-8ba8-7ac8ea3c7779";
pub const DRIVE_SHOW_FILE_NO_SUCH_FILE_ID: &str = "caf3ca38-c6e5-472e-a30c-b05377dcc240";
/// Sharkey `notes/schedule/*` idents (verbatim upstream UUIDs; Batch N
/// scheduling shim). `CANNOT_REPLY_TO_A_PURE_RENOTE` takes the Sharkey UUID
/// here — this also fixes the `notes/create` path, which emitted the code
/// with a generated UUID (the code was already in the known list).
pub const CANNOT_REPLY_TO_A_PURE_RENOTE_ID: &str = "3ac74a84-8fd5-4bb0-870f-01804f82ce15";
pub const CANNOT_CREATE_ALREADY_EXPIRED_SCHEDULE_ID: &str = "8a9bfb90-fc7e-4878-a3e8-d97faaf5fb07";
pub const SCHEDULE_NOTE_MAX_ID: &str = "168707c3-e7da-4031-989e-f42aa3a274b2";
/// Misskey `clips/*` idents (verbatim upstream UUIDs; federated clips +
/// pages handoff, Phase 1). One UUID per endpoint per code (upstream
/// convention): `NO_SUCH_CLIP` has a different UUID on every endpoint.
pub const CLIP_CREATE_TOO_MANY_ID: &str = "920f7c2d-6208-4b76-8082-e632020f5883";
pub const CLIP_UPDATE_NO_SUCH_CLIP_ID: &str = "b4d92d70-b216-46fa-9a3f-a8c811699257";
pub const CLIP_DELETE_NO_SUCH_CLIP_ID: &str = "70ca08ba-6865-4630-b6fb-8494759aa754";
pub const CLIP_SHOW_NO_SUCH_CLIP_ID: &str = "c3c5fe33-d62c-44d2-9ea5-d997703f5c20";
pub const CLIP_NOTES_NO_SUCH_CLIP_ID: &str = "1d7645e6-2b6d-4635-b0fe-fe22b0e72e00";
pub const CLIP_ADD_NOTE_NO_SUCH_CLIP_ID: &str = "d6e76cc0-a1b5-4c7c-a287-73fa9c716dcf";
pub const CLIP_ADD_NOTE_NO_SUCH_NOTE_ID: &str = "fc8c0b49-c7a3-4664-a0a6-b418d386bb8b";
pub const CLIP_ALREADY_CLIPPED_ID: &str = "734806c4-542c-463a-9311-15c512803965";
pub const CLIP_TOO_MANY_CLIP_NOTES_ID: &str = "f0dba960-ff73-4615-8df4-d6ac5d9dc118";
pub const CLIP_REMOVE_NOTE_NO_SUCH_CLIP_ID: &str = "b80525c6-97f7-49d7-a42d-ebccd49cfd52";
pub const CLIP_REMOVE_NOTE_NO_SUCH_NOTE_ID: &str = "aff017de-190e-434b-893e-33a9ff5049d8";
pub const CLIP_FAVORITE_NO_SUCH_CLIP_ID: &str = "4c2aaeae-80d8-4250-9606-26cb1fdb77a5";
pub const CLIP_ALREADY_FAVORITED_ID: &str = "92658936-c625-4273-8326-2d790129256e";
pub const CLIP_UNFAVORITE_NO_SUCH_CLIP_ID: &str = "2603966e-b865-426c-94a7-af4a01241dc1";
pub const CLIP_NOT_FAVORITED_ID: &str = "90c3a9e8-b321-4dae-bf57-2bf79bbcc187";
/// Pinetree-first `clips/follow|unfollow|followers` idents (DIVERGENCE —
/// Misskey has no clip-follow; pinetree-fixed UUIDs, documented as such).
pub const CLIP_FOLLOW_NO_SUCH_CLIP_ID: &str = "ac078150-884c-475f-84d6-22747a204b7a";
pub const CLIP_FOLLOWERS_FORBIDDEN_ID: &str = "3edf8e54-c859-4654-bad8-39dbacbff7d3";
/// Misskey `pages/*` idents (verbatim upstream UUIDs; federated clips +
/// pages handoff, Phase 3 local). One UUID per endpoint per code.
pub const PAGE_CREATE_NO_SUCH_FILE_ID: &str = "b7b97489-0f66-4b12-a5ff-b21bd63f6e1c";
pub const PAGE_CREATE_NAME_ALREADY_EXISTS_ID: &str = "4650348e-301c-499a-83c9-6aa988c66bc1";
pub const PAGE_UPDATE_NO_SUCH_PAGE_ID: &str = "21149b9e-3616-4778-9592-c4ce89f5a864";
pub const PAGE_UPDATE_ACCESS_DENIED_ID: &str = "3c15cd52-3b4b-4274-967d-6456fc4f792b";
pub const PAGE_UPDATE_NO_SUCH_FILE_ID: &str = "cfc23c7c-3887-490e-af30-0ed576703c82";
pub const PAGE_UPDATE_NAME_ALREADY_EXISTS_ID: &str = "2298a392-d4a1-44c5-9ebb-ac1aeaa5a9ab";
pub const PAGE_DELETE_NO_SUCH_PAGE_ID: &str = "eb0c6e1d-d519-4764-9486-52a7e1c6392a";
pub const PAGE_DELETE_ACCESS_DENIED_ID: &str = "8b741b3e-2c22-44b3-a15f-29949aa1601e";
pub const PAGE_SHOW_NO_SUCH_PAGE_ID: &str = "222120c0-3ead-4528-811b-b96f233388d7";
pub const PAGE_LIKE_NO_SUCH_PAGE_ID: &str = "cc98a8a2-0dc3-4123-b198-62c71df18ed3";
pub const PAGE_YOUR_PAGE_ID: &str = "28800466-e6db-40f2-8fae-bf9e82aa92b8";
pub const PAGE_ALREADY_LIKED_ID: &str = "d4c1edbe-7da2-4eae-8714-1acfd2d63941";
pub const PAGE_UNLIKE_NO_SUCH_PAGE_ID: &str = "a0d41e20-1993-40bd-890e-f6e560ae648e";
pub const PAGE_NOT_LIKED_ID: &str = "f5e586b0-ce93-4050-b0e3-7f31af5259ee";
/// pinetree `i/import-clips` idents (no upstream endpoint exists; the
/// import consumes the pinetree export format, so these UUIDs are
/// pinetree-fixed and never change).
pub const IMPORT_CLIPS_NO_SUCH_FILE_ID: &str = "7c1d9e02-4b5a-4f6e-9d1a-2c3e5f6a7b81";
pub const IMPORT_CLIPS_EMPTY_FILE_ID: &str = "8d2e0f13-5c6b-4a7f-8e2b-3d4f6a7b8c92";
pub const IMPORT_CLIPS_TOO_BIG_FILE_ID: &str = "9e3f1a24-6d7c-4b8a-9f3c-4e5a6b7c9da3";
/// Sharkey per-endpoint UUIDs (same wire code as a pinetree code, different
/// endpoint UUID — verbatim per the handoff).
pub const SHARKEY_CANNOT_RERENOTE_ID: &str = "fd4cc33e-2a37-48dd-99cc-9b806eb2031a";
pub const SCHEDULE_NO_SUCH_NOTE_ID: &str = "a58056ba-8ba1-4323-8ebf-e0b585bc244f";
pub const SCHEDULE_PERMISSION_DENIED_ID: &str = "c0da2fed-8f61-4c47-a41d-431992607b5c";
/// Misskey `i/pin.ts` idents (verbatim upstream UUIDs).
pub const PIN_LIMIT_EXCEEDED_ID: &str = "72dab508-c64d-498f-8740-a8eec1ba385a";
pub const ALREADY_PINNED_ID: &str = "8b18c2b7-68fe-4edb-9892-c0cbaeb6c913";
/// Misskey invite-code idents (verbatim upstream UUIDs; Batch M WP2).
pub const NO_SUCH_INVITE_CODE_ID: &str = "cd4f9ae4-7854-4e3e-8df9-c296f051e634";
pub const CAN_NOT_DELETE_INVITE_CODE_ID: &str = "ff17af39-000c-4d4e-abdf-848fa30fc1ce";
pub const INVALID_DATE_TIME_ID: &str = "f1380b15-3760-4c6c-a1db-5c3aaf1cbd49";
/// Sharkey `notes/edit.ts` idents (verbatim upstream UUIDs). Codes already
/// known to pinetree (`NO_SUCH_NOTE`, `NO_SUCH_REPLY_TARGET`, …) keep the
/// existing pinetree UUID (one UUID per code, §16 convention); only new
/// codes take their Sharkey UUID here.
pub const NEEDS_EDIT_ID: &str = "d697edc8-8c73-4de8-bded-35fd198b79e5";
pub const YOU_ARE_NOT_THE_AUTHOR_ID: &str = "c6e61685-411d-43d0-b90a-a448d2539001";
pub const NOT_LOCAL_USER_ID: &str = "b907f407-2aa0-4283-800b-a2c56290b822";
pub const CANNOT_QUOTE_THE_CURRENT_NOTE_ID: &str = "33510210-8452-094c-6227-4a6c05d99f02";
pub const CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE_ID: &str = "33510210-8452-094c-6227-4a6c05d99f01";
pub const QUOTE_DISABLED_FOR_USER_ID: &str = "1c0ea108-d1e3-4e8e-aa3f-4d2487626153";
pub const CANNOT_PRIVATE_RENOTE_ID: &str = "19a50f1c-84fa-4e33-81d3-17834ccc0ad8";
pub const CANNOT_RENOTE_OUTSIDE_OF_CHANNEL_ID: &str = "33510210-8452-094c-6227-4a6c05d99f00";
pub const CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY_ID: &str =
    "ed940410-535c-4d5e-bfa3-af798671e93c";
pub const MAX_LENGTH_ID: &str = "3ac74a84-8fd5-4bb0-870f-01804f82ce16";
pub const MAX_CW_LENGTH_ID: &str = "7004c478-bda3-4b4f-acb2-4316398c9d52";
/// Misskey `i/import-*.ts` file-guard idents (verbatim upstream UUIDs).
/// Every import endpoint mints its OWN UUID per code (unlike pinetree's
/// usual one-UUID-per-code convention), so these are keyed per endpoint.
pub const IMPORT_BLOCKING_NO_SUCH_FILE_ID: &str = "ebb53e5f-6574-9c0c-0b92-7ca6def56d7e";
pub const IMPORT_BLOCKING_TOO_BIG_FILE_ID: &str = "b7fbf0b1-aeef-3b21-29ef-fadd4cb72ccf";
pub const IMPORT_BLOCKING_EMPTY_FILE_ID: &str = "6f3a4dcc-f060-a707-4950-806fbdbe60d6";
pub const IMPORT_FOLLOWING_NO_SUCH_FILE_ID: &str = "b98644cf-a5ac-4277-a502-0b8054a709a3";
pub const IMPORT_FOLLOWING_TOO_BIG_FILE_ID: &str = "dee9d4ed-ad07-43ed-8b34-b2856398bc60";
pub const IMPORT_FOLLOWING_EMPTY_FILE_ID: &str = "31a1b42c-06f7-42ae-8a38-a661c5c9f691";
pub const IMPORT_MUTING_NO_SUCH_FILE_ID: &str = "e674141e-bd2a-ba85-e616-aefb187c9c2a";
pub const IMPORT_MUTING_TOO_BIG_FILE_ID: &str = "9b4ada6d-d7f7-0472-0713-4f558bd1ec9c";
pub const IMPORT_MUTING_EMPTY_FILE_ID: &str = "d2f12af1-e7b4-feac-86a3-519548f2728e";
pub const IMPORT_USER_LISTS_NO_SUCH_FILE_ID: &str = "ea9cc34f-c415-4bc6-a6fe-28ac40357049";
pub const IMPORT_USER_LISTS_TOO_BIG_FILE_ID: &str = "ae6e7a22-971b-4b52-b2be-fc0b9b121fe9";
pub const IMPORT_USER_LISTS_EMPTY_FILE_ID: &str = "99efe367-ce6e-4d44-93f8-5fae7b040356";
pub const IMPORT_ANTENNAS_NO_SUCH_FILE_ID: &str = "3b71d086-c3fa-431c-b01d-ded65a777172";
pub const IMPORT_ANTENNAS_EMPTY_FILE_ID: &str = "7f60115d-8d93-4b0f-bd0e-3815dcbb389f";
/// Gaps final-wiring error idents (WP2a/WP2b/WP2c/WP3 dispatcher arms).
/// Every UUID below is a pinetree-minted v4: the misskey checkout symlinks
/// are broken in this environment, so none of these could be pinned.
/// `// TODO(final): verify against misskey` marks each one for the parent
/// merge to reconcile against the upstream checkout (one UUID per
/// endpoint per code, upstream convention).
// TODO(final): verify against misskey.
pub const INCORRECT_PASSWORD_ID: &str = "f0e94fca-6eb6-4f7c-9cb3-057c35ff53a6";
// TODO(final): verify against misskey.
pub const INVALID_TOKEN_ID: &str = "ce81e4ed-0ab2-463e-8532-f6bfc31ece9e";
// TODO(final): verify against misskey.
pub const NO_SUCH_2FA_ID: &str = "c133d97b-e0ca-495c-aeb5-9e7b52c5f187";
// TODO(final): verify against misskey.
pub const NO_SUCH_APP_ID: &str = "b2bd7c54-9ca3-41ad-82be-6162e0beda86";
// TODO(final): verify against misskey.
pub const NO_SUCH_SUBSCRIPTION_ID: &str = "ed1f411e-707a-43f7-a1a5-bfd41289df01";
// TODO(final): verify against misskey.
pub const NO_SUCH_ROLE_ID: &str = "339ecdfe-6c78-4aa8-91ba-6432e8ff2866";
// TODO(final): verify against misskey.
pub const NO_SUCH_AD_ID: &str = "5192512d-3d9e-47a4-9b7b-d1996a2aecea";
// TODO(final): verify against misskey.
pub const NO_SUCH_ANNOUNCEMENT_ID: &str = "e13e1b72-8a5b-40af-aa12-704d5627c149";
// TODO(final): verify against misskey.
pub const NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT_ID: &str =
    "dbc5473f-296b-4c85-9e91-447991cbd163";
// TODO(final): verify against misskey.
pub const NO_SUCH_ENDPOINT_ID: &str = "06ccccd9-dda7-4ef8-bf8a-59cc81204b4f";
// TODO(final): verify against misskey.
pub const NO_SUCH_HASHTAG_ID: &str = "f4d91c70-47c8-4011-b2d0-bfbbd440a9fb";
// TODO(final): verify against misskey.
pub const NO_SUCH_POST_ID: &str = "a6170400-7fba-4421-85e2-2f499dd91291";
// TODO(final): verify against misskey.
pub const NO_SUCH_FLASH_ID: &str = "95a44529-6e18-4ae8-bc4f-f84ccc761dae";
// TODO(final): verify against misskey.
pub const GALLERY_ALREADY_LIKED_ID: &str = "74e9d1f1-1d88-4fdd-bd1d-67fb8e25c282";
// TODO(final): verify against misskey.
pub const GALLERY_NOT_LIKED_ID: &str = "67405043-c538-49eb-a5d9-76a49f86975d";
// TODO(final): verify against misskey.
pub const DRIVE_CHECK_EXISTENCE_NO_SUCH_FILE_ID: &str =
    "d0787be9-4b54-434c-9034-ce8dbd974a1f";
/// `drive/files/attached-notes` `NO_SUCH_FILE`: the `c118ece3-…` prefix is
/// the verbatim upstream id recorded in
/// `application::commands::drive_folders` (`attached_notes`); the remaining
/// nibbles are pinetree-minted.
// TODO(final): verify against misskey.
pub const DRIVE_ATTACHED_NOTES_NO_SUCH_FILE_ID: &str =
    "c118ece3-15b0-4596-829c-214fde70db4a";

/// Misskey API error: stable `code`/`id` ident, human `message`,
/// machine `kind` (`client` | `permission` | `server`), optional `info`,
/// and the HTTP status to answer with.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub message: String,
    pub code: String,
    pub id: String,
    pub kind: String,
    pub info: Option<Value>,
    pub http_status: u16,
    pub retry_after_secs: Option<u64>,
}

impl ApiError {
    fn new(
        message: impl Into<String>,
        code: impl Into<String>,
        id: impl Into<String>,
        kind: impl Into<String>,
        http_status: u16,
    ) -> Self {
        Self {
            message: message.into(),
            code: code.into(),
            id: id.into(),
            kind: kind.into(),
            info: None,
            http_status,
            retry_after_secs: None,
        }
    }

    fn client(message: &str, code: &str, id: &str, http_status: u16) -> Self {
        Self::new(message, code, id, "client", http_status)
    }

    fn permission(message: &str, code: &str, id: &str, http_status: u16) -> Self {
        Self::new(message, code, id, "permission", http_status)
    }

    pub fn with_info(mut self, info: Value) -> Self {
        self.info = Some(info);
        self
    }

    /// 404 `UNKNOWN_API_ENDPOINT`.
    pub fn unknown_endpoint() -> Self {
        Self::client(
            "Unknown API endpoint.",
            "UNKNOWN_API_ENDPOINT",
            UNKNOWN_API_ENDPOINT_ID,
            404,
        )
    }

    /// 400 `UNSUPPORTED_FIELD` naming `field` in `info.param`.
    pub fn unsupported_field(field: &str) -> Self {
        Self::client(
            &format!("Unsupported field: {field}."),
            "UNSUPPORTED_FIELD",
            UNSUPPORTED_FIELD_ID,
            400,
        )
        .with_info(Value::Object({
            let mut info = Map::new();
            info.insert("param".to_string(), Value::String(field.to_string()));
            info
        }))
    }

    /// 400 `INVALID_PARAM` naming `field` in `info.param` with `info.reason`.
    pub fn invalid_param(field: &str, reason: &str) -> Self {
        Self::client("Invalid param.", "INVALID_PARAM", INVALID_PARAM_ID, 400).with_info(
            Value::Object({
                let mut info = Map::new();
                info.insert("param".to_string(), Value::String(field.to_string()));
                info.insert("reason".to_string(), Value::String(reason.to_string()));
                info
            }),
        )
    }

    /// 401 `AUTHENTICATION_FAILED`.
    pub fn authentication_failed() -> Self {
        Self::client(
            "Authentication failed. Please ensure your token is correct.",
            "AUTHENTICATION_FAILED",
            AUTHENTICATION_FAILED_ID,
            401,
        )
    }

    /// 401 `CREDENTIAL_REQUIRED`.
    pub fn credential_required() -> Self {
        Self::client(
            "Credential required.",
            "CREDENTIAL_REQUIRED",
            CREDENTIAL_REQUIRED_ID,
            401,
        )
    }

    /// 403 `PERMISSION_DENIED`.
    pub fn permission_denied() -> Self {
        Self::permission(
            "Your app does not have the necessary permissions to use this endpoint.",
            "PERMISSION_DENIED",
            PERMISSION_DENIED_ID,
            403,
        )
    }

    /// 403 `ACCESS_DENIED`.
    pub fn access_denied() -> Self {
        Self::permission("Access denied.", "ACCESS_DENIED", ACCESS_DENIED_ID, 403)
    }

    /// 403 `USER_SUSPENDED`.
    pub fn user_suspended() -> Self {
        Self::permission(
            "Your account has been suspended.",
            "USER_SUSPENDED",
            USER_SUSPENDED_ID,
            403,
        )
    }

    /// Misskey `signin-flow` unknown username (verbatim upstream id,
    /// 404; `SigninApiService.ts` answers `{ error: { id } }` with no
    /// code/message — the code/message here are pinetree-chosen).
    pub fn signin_no_such_user() -> Self {
        Self::client("No such user.", "NO_SUCH_USER", SIGNIN_NO_SUCH_USER_ID, 404)
    }

    /// Misskey `signin-flow` suspended account (verbatim upstream id,
    /// 403). Checked before the password, like upstream (unlike the
    /// legacy `signin` arm, which checks after).
    pub fn signin_suspended() -> Self {
        Self::permission(
            "Your account has been suspended.",
            "USER_SUSPENDED",
            SIGNIN_SUSPENDED_ID,
            403,
        )
    }

    /// Misskey `signin-flow` wrong password / wrong totp (verbatim
    /// upstream id, 403).
    pub fn signin_incorrect_password() -> Self {
        Self::client(
            "Incorrect password.",
            "INCORRECT_PASSWORD",
            SIGNIN_INCORRECT_PASSWORD_ID,
            403,
        )
    }

    /// Misskey `signin-flow` IP rate limit (verbatim upstream
    /// code/message/id, 429 with `Retry-After`).
    pub fn signin_too_many_failures(retry_after_secs: u64) -> Self {
        let mut err = Self::client(
            "Too many failed attempts to sign in. Try again later.",
            "TOO_MANY_AUTHENTICATION_FAILURES",
            SIGNIN_RATE_LIMITED_ID,
            429,
        )
        .with_info(Value::Object({
            let mut info = Map::new();
            info.insert(
                "retryAfter".to_string(),
                Value::Number(retry_after_secs.into()),
            );
            info
        }));
        err.retry_after_secs = Some(retry_after_secs);
        err
    }

    /// 429 `RATE_LIMIT_EXCEEDED` with a `Retry-After` hint.
    pub fn rate_limit_exceeded(retry_after_secs: u64) -> Self {
        let mut err = Self::client(
            "Rate limit exceeded. Please try again later.",
            "RATE_LIMIT_EXCEEDED",
            RATE_LIMIT_EXCEEDED_ID,
            429,
        )
        .with_info(Value::Object({
            let mut info = Map::new();
            info.insert(
                "retryAfter".to_string(),
                Value::Number(retry_after_secs.into()),
            );
            info
        }));
        err.retry_after_secs = Some(retry_after_secs);
        err
    }

    /// 413 `MAX_FILE_SIZE_EXCEEDED`.
    pub fn max_file_size_exceeded() -> Self {
        Self::client(
            "Max file size exceeded.",
            "MAX_FILE_SIZE_EXCEEDED",
            MAX_FILE_SIZE_EXCEEDED_ID,
            413,
        )
    }

    /// Misskey `i/import-*.ts:26-50` file-guard errors (verbatim upstream
    /// code/message/id, keyed per endpoint — upstream mints one UUID per
    /// endpoint per code). `endpoint` is the Misskey endpoint name
    /// (`i/import-blocking`, …); unknown names fall back to a deterministic
    /// pinetree UUID with the same code/message.
    pub fn import_no_such_file(endpoint: &str) -> Self {
        let id = match endpoint {
            "i/import-blocking" => IMPORT_BLOCKING_NO_SUCH_FILE_ID,
            "i/import-following" => IMPORT_FOLLOWING_NO_SUCH_FILE_ID,
            "i/import-muting" => IMPORT_MUTING_NO_SUCH_FILE_ID,
            "i/import-user-lists" => IMPORT_USER_LISTS_NO_SUCH_FILE_ID,
            "i/import-antennas" => IMPORT_ANTENNAS_NO_SUCH_FILE_ID,
            "i/import-clips" => IMPORT_CLIPS_NO_SUCH_FILE_ID,
            _ => NO_SUCH_FILE_ID,
        };
        Self::client("No such file.", "NO_SUCH_FILE", id, 400)
    }

    /// Misskey `i/import-*.ts` `EMPTY_FILE` (`That file is empty.`,
    /// verbatim per-endpoint UUID).
    pub fn import_empty_file(endpoint: &str) -> Self {
        let id: String = match endpoint {
            "i/import-blocking" => IMPORT_BLOCKING_EMPTY_FILE_ID.to_string(),
            "i/import-following" => IMPORT_FOLLOWING_EMPTY_FILE_ID.to_string(),
            "i/import-muting" => IMPORT_MUTING_EMPTY_FILE_ID.to_string(),
            "i/import-user-lists" => IMPORT_USER_LISTS_EMPTY_FILE_ID.to_string(),
            "i/import-antennas" => IMPORT_ANTENNAS_EMPTY_FILE_ID.to_string(),
            "i/import-clips" => IMPORT_CLIPS_EMPTY_FILE_ID.to_string(),
            _ => new_id_for("EMPTY_FILE"),
        };
        Self::client("That file is empty.", "EMPTY_FILE", &id, 400)
    }

    /// Misskey `i/import-*.ts` `TOO_BIG_FILE` (`That file is too big.`,
    /// verbatim per-endpoint UUID). `i/import-antennas` has no such error
    /// upstream (it never size-checks), so it is absent from the match.
    pub fn import_too_big_file(endpoint: &str) -> Self {
        let id: String = match endpoint {
            "i/import-blocking" => IMPORT_BLOCKING_TOO_BIG_FILE_ID.to_string(),
            "i/import-following" => IMPORT_FOLLOWING_TOO_BIG_FILE_ID.to_string(),
            "i/import-muting" => IMPORT_MUTING_TOO_BIG_FILE_ID.to_string(),
            "i/import-user-lists" => IMPORT_USER_LISTS_TOO_BIG_FILE_ID.to_string(),
            "i/import-clips" => IMPORT_CLIPS_TOO_BIG_FILE_ID.to_string(),
            _ => new_id_for("TOO_BIG_FILE"),
        };
        Self::client("That file is too big.", "TOO_BIG_FILE", &id, 400)
    }

    /// Misskey `admin/emoji/add.ts` `NO_SUCH_FILE` (`No such file.`,
    /// verbatim add-endpoint UUID `fc46b5a4-…`). The update endpoint mints
    /// its own UUID (`14fb9fd9-…`); pinetree emits the add UUID for both
    /// (one-UUID-per-code, §16 convention).
    pub fn emoji_no_such_file() -> Self {
        Self::client("No such file.", "NO_SUCH_FILE", EMOJI_ADD_NO_SUCH_FILE_ID, 400)
    }

    /// Misskey `i/import-antennas.ts` `TOO_MANY_ANTENNAS` (`You cannot
    /// create antenna any more.`, verbatim import-endpoint UUID
    /// `600917d4-…`). The create/update path emits the create-endpoint UUID
    /// (`faf47050-…`, one-UUID-per-code).
    pub fn import_too_many_antennas() -> Self {
        Self::client(
            "You cannot create antenna any more.",
            "TOO_MANY_ANTENNAS",
            IMPORT_ANTENNAS_TOO_MANY_ID,
            400,
        )
    }

    /// Sharkey `notes/schedule/create.ts` `SCHEDULE_NOTE_MAX` (`Schedule
    /// note max.`, verbatim UUID).
    pub fn schedule_note_max() -> Self {
        Self::client("Schedule note max.", "SCHEDULE_NOTE_MAX", SCHEDULE_NOTE_MAX_ID, 400)
    }

    /// Sharkey `notes/schedule/create.ts` `CANNOT_RENOTE_TO_A_PURE_RENOTE`
    /// (`You can not Renote a pure Renote.`, verbatim UUID). The
    /// `notes/create` path emits the same code under Misskey's
    /// `CANNOT_RENOTE` UUID (one-UUID-per-code for the shared code
    /// string); the shim needs Sharkey's UUID verbatim.
    pub fn sharkey_cannot_rerenote() -> Self {
        Self::client(
            "You can not Renote a pure Renote.",
            "CANNOT_RENOTE_TO_A_PURE_RENOTE",
            SHARKEY_CANNOT_RERENOTE_ID,
            400,
        )
    }

    /// Sharkey `notes/schedule/delete.ts` `NO_SUCH_NOTE` (`No such note.`,
    /// verbatim UUID `a58056ba-…` — different from pinetree's
    /// `NO_SUCH_NOTE` UUID).
    pub fn schedule_no_such_note() -> Self {
        Self::client("No such note.", "NO_SUCH_NOTE", SCHEDULE_NO_SUCH_NOTE_ID, 400)
    }

    /// Sharkey `notes/schedule/delete.ts` `PERMISSION_DENIED`
    /// (`Permission denied.`, verbatim UUID, HTTP 403).
    pub fn schedule_permission_denied() -> Self {
        Self::permission("Permission denied.", "PERMISSION_DENIED", SCHEDULE_PERMISSION_DENIED_ID, 403)
    }

    /// Misskey `following/invalidate.ts` verbatim errors (per-endpoint
    /// UUIDs, upstream `29-45`). Pinetree's shared `NO_SUCH_USER` /
    /// `NOT_FOLLOWING` UUIDs differ, so invalidate mints its own.
    pub fn invalidate_no_such_user() -> Self {
        Self::client("No such user.", "NO_SUCH_USER", INVALIDATE_NO_SUCH_USER_ID, 400)
    }

    /// Misskey `following/invalidate.ts` `FOLLOWER_IS_YOURSELF`.
    pub fn follower_is_yourself() -> Self {
        Self::client(
            "Follower is yourself.",
            "FOLLOWER_IS_YOURSELF",
            FOLLOWER_IS_YOURSELF_ID,
            400,
        )
    }

/// Misskey `following/invalidate.ts` `NOT_FOLLOWING`.
    pub fn invalidate_not_following() -> Self {
        Self::client(
            "You are not following that user.",
            "NOT_FOLLOWING",
            INVALIDATE_NOT_FOLLOWING_ID,
            400,
        )
    }

    /// Batch P email/move/registry/translate verbatim errors (upstream
    /// `code` + UUID per endpoint file).
    /// Misskey `i/update-email.ts` `INCORRECT_PASSWORD`.
    pub fn update_email_incorrect_password() -> Self {
        Self::client(
            "Incorrect password.",
            "INCORRECT_PASSWORD",
            UPDATE_EMAIL_INCORRECT_PASSWORD_ID,
            400,
        )
    }

    /// Misskey `i/update-email.ts` `UNAVAILABLE`.
    pub fn update_email_unavailable() -> Self {
        Self::client(
            "Unavailable email address.",
            "UNAVAILABLE",
            UPDATE_EMAIL_UNAVAILABLE_ID,
            400,
        )
    }

    /// Misskey `i/update-email.ts` `EMAIL_REQUIRED`.
    pub fn update_email_required() -> Self {
        Self::client(
            "Email address is required.",
            "EMAIL_REQUIRED",
            UPDATE_EMAIL_REQUIRED_ID,
            400,
        )
    }

    /// Misskey `verify-email.ts` `NO_SUCH_CODE`.
    pub fn verify_email_no_such_code() -> Self {
        Self::client("No such code.", "NO_SUCH_CODE", VERIFY_EMAIL_NO_SUCH_CODE_ID, 404)
    }

    /// Misskey `i/move.ts` `DESTINATION_ACCOUNT_FORBIDS`.
    pub fn move_destination_forbids() -> Self {
        Self::client(
            "Destination account doesn't have proper 'Known As' alias, or has already moved.",
            "DESTINATION_ACCOUNT_FORBIDS",
            MOVE_DESTINATION_FORBIDS_ID,
            400,
        )
    }

    /// Misskey `i/move.ts` `NOT_ROOT_FORBIDDEN` (upstream message/code:
    /// "The root can't migrate." / `NOT_ROOT_FORBIDDEN`).
    pub fn move_root_forbidden() -> Self {
        Self::client(
            "The root can't migrate.",
            "NOT_ROOT_FORBIDDEN",
            MOVE_ROOT_FORBIDDEN_ID,
            400,
        )
    }

    /// Misskey `i/move.ts` `NO_SUCH_USER` (shared `NO_SUCH_USER` UUID).
    pub fn move_no_such_user() -> Self {
        Self::client("No such user.", "NO_SUCH_USER", NO_SUCH_USER_ID, 404)
    }

    /// Misskey `i/move.ts` `URI_NULL`.
    pub fn move_uri_null() -> Self {
        Self::client(
            "User ActivityPup URI is null.",
            "URI_NULL",
            MOVE_URI_NULL_ID,
            400,
        )
    }

    /// Misskey `i/move.ts` `ALREADY_MOVED`.
    pub fn move_already_moved() -> Self {
        Self::client(
            "Account was already moved to another account.",
            "ALREADY_MOVED",
            MOVE_ALREADY_MOVED_ID,
            400,
        )
    }

    /// Misskey `i/registry/get|get-detail.ts` `NO_SUCH_KEY`.
    pub fn registry_no_such_key() -> Self {
        Self::client("No such key.", "NO_SUCH_KEY", REGISTRY_NO_SUCH_KEY_ID, 404)
    }

    /// Misskey `fetch-external-resources.ts`
    /// `EXT_RESOURCE_RETURNED_INVALID_SCHEMA`.
    pub fn ext_resource_invalid_schema() -> Self {
        Self::client(
            "External resource returned invalid schema.",
            "EXT_RESOURCE_RETURNED_INVALID_SCHEMA",
            EXT_RESOURCE_INVALID_SCHEMA_ID,
            400,
        )
    }

    /// Misskey `fetch-external-resources.ts`
    /// `EXT_RESOURCE_HASH_DIDNT_MATCH`.
    pub fn ext_resource_hash_mismatch() -> Self {
        Self::client(
            "Hash did not match.",
            "EXT_RESOURCE_HASH_DIDNT_MATCH",
            EXT_RESOURCE_HASH_MISMATCH_ID,
            400,
        )
    }

    /// Misskey `notes/translate.ts` `UNAVAILABLE` (no backend / no
    /// permission). The dormant-transport posture reuses this code with a
    /// first-party UUID (`TRANSLATE_NOT_CONFIGURED_ID`).
    pub fn translate_unavailable() -> Self {
        Self::client(
            "Translate of notes unavailable.",
            "UNAVAILABLE",
            TRANSLATE_UNAVAILABLE_ID,
            400,
        )
    }

    /// Batch P dormant-transport posture: no translation backend is
    /// configured, so the send path fails loudly (never silently drops).
    /// Same `UNAVAILABLE` code as upstream, first-party UUID.
    pub fn translate_not_configured() -> Self {
        Self::client(
            "Translate of notes unavailable.",
            "UNAVAILABLE",
            TRANSLATE_NOT_CONFIGURED_ID,
            400,
        )
    }

    /// Misskey `notes/translate.ts` `CANNOT_TRANSLATE_INVISIBLE_NOTE`.
    pub fn translate_invisible_note() -> Self {
        Self::client(
            "Cannot translate invisible note.",
            "CANNOT_TRANSLATE_INVISIBLE_NOTE",
            TRANSLATE_INVISIBLE_NOTE_ID,
            400,
        )
    }

    /// Batch P dormant-transport posture for the email loop: no SMTP is
    /// configured, so send paths fail loudly (never silently drop a
    /// password reset). First-party UUID.
    pub fn email_not_configured() -> Self {
        Self::client(
            "Email sending is not configured on this server.",
            "EMAIL_NOT_CONFIGURED",
            EMAIL_NOT_CONFIGURED_ID,
            400,
        )
    }

    /// Sharkey `notes/versions.ts` `SIGNIN_REQUIRED` (private-note history
    /// without a viewer).
    pub fn versions_signin_required() -> Self {
        Self::client("Signin required.", "SIGNIN_REQUIRED", VERSIONS_SIGNIN_REQUIRED_ID, 401)
    }

    /// Misskey `users/show.ts:52-57` `noSuchUser` (Batch Q Phase 1 item 3):
    /// verbatim id + 404, covering both missing and suspended users for
    /// non-moderators.
    pub fn users_show_no_such_user() -> Self {
        Self::client("No such user.", "NO_SUCH_USER", USERS_SHOW_NO_SUCH_USER_ID, 404)
    }

    /// Misskey `fetch-rss.ts` `INVALID_URL` (HTTP 400 upstream).
    pub fn fetch_rss_invalid_url() -> Self {
        Self::client("Invalid URL.", "INVALID_URL", FETCH_RSS_INVALID_URL_ID, 400)
    }

    /// Misskey `fetch-rss.ts` `FETCH_RSS_FAILED` (HTTP 422 upstream).
    pub fn fetch_rss_failed() -> Self {
        Self::client(
            "Failed to fetch RSS.",
            "FETCH_RSS_FAILED",
            FETCH_RSS_FAILED_ID,
            422,
        )
    }

    /// Misskey `fetch-rss.ts` `FETCH_RSS_UNAVAILABLE` (HTTP 503 upstream).
    pub fn fetch_rss_unavailable() -> Self {
        Self::client(
            "RSS fetching is temporarily unavailable.",
            "FETCH_RSS_UNAVAILABLE",
            FETCH_RSS_UNAVAILABLE_ID,
            503,
        )
    }

    /// Misskey `users/lists/*` `NO_SUCH_LIST` (`No such list.`, verbatim
    /// per-endpoint UUID): `endpoint` is the Misskey endpoint name
    /// (`users/lists/update|delete|show|get-memberships|pull|
    /// update-membership`, `notes/user-list-timeline`).
    pub fn user_list_no_such_list(endpoint: &str) -> Self {
        let id = match endpoint {
            "users/lists/update" => LIST_NO_SUCH_LIST_UPDATE_ID,
            "users/lists/delete" => LIST_NO_SUCH_LIST_DELETE_ID,
            "users/lists/show" | "users/lists/get-memberships" => LIST_NO_SUCH_LIST_SHOW_ID,
            "users/lists/pull" | "users/lists/update-membership" | "users/lists/push" => {
                LIST_NO_SUCH_LIST_MEMBER_ID
            }
            "notes/user-list-timeline" => LIST_TIMELINE_NO_SUCH_LIST_ID,
            _ => LIST_NO_SUCH_LIST_SHOW_ID,
        };
        Self::client("No such list.", "NO_SUCH_LIST", id, 400)
    }

    /// Misskey `users/lists/*` `NO_SUCH_USER` (verbatim per-endpoint UUID).
    pub fn user_list_no_such_user(endpoint: &str) -> Self {
        let id = match endpoint {
            "users/lists/list" => LIST_NO_SUCH_USER_LIST_QUERY_ID,
            "users/lists/push" => LIST_NO_SUCH_USER_PUSH_ID,
            _ => LIST_NO_SUCH_USER_PULL_ID,
        };
        Self::client("No such user.", "NO_SUCH_USER", id, 400)
    }

    /// Misskey `users/lists/list` `REMOTE_USER_NOT_ALLOWED`.
    pub fn user_list_remote_not_allowed() -> Self {
        Self::client(
            "Not allowed to load the remote user's list",
            "REMOTE_USER_NOT_ALLOWED",
            LIST_REMOTE_USER_NOT_ALLOWED_ID,
            400,
        )
    }

    /// Misskey `users/lists/list` `INVALID_PARAM` (anonymous without
    /// `userId`; verbatim id).
    pub fn user_list_query_invalid_param() -> Self {
        Self::client("Invalid param.", "INVALID_PARAM", LIST_QUERY_INVALID_PARAM_ID, 400)
    }

    /// Misskey `users/lists/create` `TOO_MANY_USERLISTS`.
    pub fn user_list_too_many() -> Self {
        Self::client(
            "You cannot create user list any more.",
            "TOO_MANY_USERLISTS",
            LIST_TOO_MANY_USERLISTS_ID,
            400,
        )
    }

    /// Misskey `users/lists/push` `ALREADY_ADDED`.
    pub fn user_list_already_added() -> Self {
        Self::client(
            "That user has already been added to that list.",
            "ALREADY_ADDED",
            LIST_ALREADY_ADDED_ID,
            400,
        )
    }

    /// Misskey `users/lists/push` `YOU_HAVE_BEEN_BLOCKED`.
    pub fn user_list_blocked() -> Self {
        Self::client(
            "You cannot push this user because you have been blocked by this user.",
            "YOU_HAVE_BEEN_BLOCKED",
            LIST_YOU_HAVE_BEEN_BLOCKED_ID,
            400,
        )
    }

    /// Misskey `users/lists/push` `TOO_MANY_USERS`.
    pub fn user_list_too_many_users() -> Self {
        Self::client(
            "You can not push users any more.",
            "TOO_MANY_USERS",
            LIST_TOO_MANY_USERS_ID,
            400,
        )
    }

    /// Misskey `users/lists/favorite|unfavorite` `NO_SUCH_USER_LIST`
    /// (verbatim per-endpoint UUID).
    pub fn user_list_no_such_user_list(endpoint: &str) -> Self {
        let id = match endpoint {
            "users/lists/favorite" => LIST_NO_SUCH_USER_LIST_FAVORITE_ID,
            _ => LIST_NO_SUCH_USER_LIST_UNFAVORITE_ID,
        };
        Self::client("No such user list.", "NO_SUCH_USER_LIST", id, 400)
    }

    /// Misskey `users/lists/favorite` `ALREADY_FAVORITED`.
    pub fn user_list_already_favorited() -> Self {
        Self::client(
            "The list has already been favorited.",
            "ALREADY_FAVORITED",
            LIST_ALREADY_FAVORITED_ID,
            400,
        )
    }

    /// Misskey `users/lists/unfavorite` `notFavorited` quirk: the wire code
    /// stays `ALREADY_FAVORITED` with its own UUID (verbatim).
    pub fn user_list_not_favorited() -> Self {
        Self::client(
            "You have not favorited the list.",
            "ALREADY_FAVORITED",
            LIST_NOT_FAVORITED_ID,
            400,
        )
    }

    /// Misskey `admin/drive/show-file.ts` `NO_SUCH_FILE` (`No such file.`,
    /// verbatim UUID `caf3ca38-…`).
    pub fn drive_show_file_no_such_file() -> Self {
        Self::client("No such file.", "NO_SUCH_FILE", DRIVE_SHOW_FILE_NO_SUCH_FILE_ID, 400)
    }

    /// Misskey `clips/*` `NO_SUCH_CLIP` (`No such clip.`, verbatim
    /// per-endpoint UUID — upstream assigns one UUID per endpoint).
    pub fn clip_no_such_clip(endpoint: &str) -> Self {
        let id = match endpoint {
            "clips/create" => CLIP_CREATE_TOO_MANY_ID,
            "clips/update" => CLIP_UPDATE_NO_SUCH_CLIP_ID,
            "clips/delete" => CLIP_DELETE_NO_SUCH_CLIP_ID,
            "clips/show" => CLIP_SHOW_NO_SUCH_CLIP_ID,
            "clips/notes" => CLIP_NOTES_NO_SUCH_CLIP_ID,
            "clips/add-note" => CLIP_ADD_NOTE_NO_SUCH_CLIP_ID,
            "clips/remove-note" => CLIP_REMOVE_NOTE_NO_SUCH_CLIP_ID,
            "clips/favorite" => CLIP_FAVORITE_NO_SUCH_CLIP_ID,
            "clips/unfavorite" => CLIP_UNFAVORITE_NO_SUCH_CLIP_ID,
            "clips/follow" => CLIP_FOLLOW_NO_SUCH_CLIP_ID,
            "clips/unfollow" => CLIP_FOLLOW_NO_SUCH_CLIP_ID,
            "clips/followers" => CLIP_FOLLOW_NO_SUCH_CLIP_ID,
            _ => CLIP_SHOW_NO_SUCH_CLIP_ID,
        };
        // `clips/create` never emits `NO_SUCH_CLIP` (it emits
        // `TOO_MANY_CLIPS`); the arm above is unreachable defensively.
        Self::client("No such clip.", "NO_SUCH_CLIP", id, 400)
    }

    /// Misskey `clips/create` `TOO_MANY_CLIPS` (verbatim).
    pub fn clip_too_many_clips() -> Self {
        Self::client(
            "You cannot create clip any more.",
            "TOO_MANY_CLIPS",
            CLIP_CREATE_TOO_MANY_ID,
            400,
        )
    }

    /// Misskey `clips/{add-note,remove-note}` `NO_SUCH_NOTE` (`No such
    /// note.`, verbatim per-endpoint UUID).
    pub fn clip_no_such_note(endpoint: &str) -> Self {
        let id = match endpoint {
            "clips/remove-note" => CLIP_REMOVE_NOTE_NO_SUCH_NOTE_ID,
            _ => CLIP_ADD_NOTE_NO_SUCH_NOTE_ID,
        };
        Self::client("No such note.", "NO_SUCH_NOTE", id, 400)
    }

    /// Gaps final wiring: `drive/files/{check-existence,attached-notes}`
    /// `NO_SUCH_FILE` (one UUID per endpoint, upstream convention;
    /// pinetree-minted — `// TODO(final): verify against misskey`).
    pub fn drive_no_such_file(endpoint: &str) -> Self {
        let id = match endpoint {
            "drive/files/attached-notes" => DRIVE_ATTACHED_NOTES_NO_SUCH_FILE_ID,
            _ => DRIVE_CHECK_EXISTENCE_NO_SUCH_FILE_ID,
        };
        Self::client("No such file.", "NO_SUCH_FILE", id, 400)
    }

    /// Gaps final wiring: `drive/folders/{create,show,update,delete}` +
    /// `drive/files/{move-bulk,update-folder-leg}` `NO_SUCH_FOLDER` (one
    /// UUID per endpoint, upstream convention; pinetree-minted —
    /// `// TODO(final): verify against misskey`).
    pub fn folder_no_such_folder(endpoint: &str) -> Self {
        let id = match endpoint {
            "drive/folders/create" => FOLDER_CREATE_NO_SUCH_FOLDER_ID,
            "drive/folders/show" => FOLDER_SHOW_NO_SUCH_FOLDER_ID,
            "drive/folders/update" => FOLDER_UPDATE_NO_SUCH_FOLDER_ID,
            "drive/folders/delete" => FOLDER_DELETE_NO_SUCH_FOLDER_ID,
            "drive/files/move-bulk" => MOVE_BULK_NO_SUCH_FOLDER_ID,
            "drive/files/update" => FILES_UPDATE_NO_SUCH_FOLDER_ID,
            _ => FOLDER_SHOW_NO_SUCH_FOLDER_ID,
        };
        Self::client("No such folder.", "NO_SUCH_FOLDER", id, 400)
    }

    /// Gaps final wiring: `drive/folders/update` `NO_SUCH_PARENT_FOLDER` /
    /// `RECURSIVE_NESTING` (verbatim codes; pinetree-minted UUIDs).
    pub fn folder_update_parent_error(code: &str) -> Self {
        match code {
            "NO_SUCH_PARENT_FOLDER" => Self::client(
                "No such parent folder.",
                "NO_SUCH_PARENT_FOLDER",
                FOLDER_UPDATE_NO_SUCH_PARENT_FOLDER_ID,
                400,
            ),
            _ => Self::client(
                "Recursive nesting.",
                "RECURSIVE_NESTING",
                FOLDER_UPDATE_RECURSIVE_NESTING_ID,
                400,
            ),
        }
    }

    /// Gaps final wiring: `drive/folders/delete`
    /// `HAS_CHILD_FILES_OR_FOLDERS` (verbatim code; pinetree-minted UUID).
    pub fn folder_delete_has_child() -> Self {
        Self::client(
            "The folder has child files or folders.",
            "HAS_CHILD_FILES_OR_FOLDERS",
            FOLDER_DELETE_HAS_CHILD_ID,
            400,
        )
    }

    /// Gaps final wiring: `system-webhook/{show,update,delete}` +
    /// `webhooks/{show,update,delete}` `NO_SUCH_WEBHOOK` (one UUID per
    /// endpoint, upstream `i/webhooks/*` convention; pinetree-minted).
    /// The store layer reports lowercase `no such webhook`; this maps it
    /// with the arm's endpoint identity.
    pub fn webhook_no_such_webhook(endpoint: &str) -> Self {
        let id = match endpoint {
            "system-webhook/show" => SYSTEM_WEBHOOK_SHOW_NO_SUCH_ID,
            "system-webhook/update" => SYSTEM_WEBHOOK_UPDATE_NO_SUCH_ID,
            "system-webhook/delete" => SYSTEM_WEBHOOK_DELETE_NO_SUCH_ID,
            "webhooks/show" => WEBHOOK_SHOW_NO_SUCH_ID,
            "webhooks/update" => WEBHOOK_UPDATE_NO_SUCH_ID,
            "webhooks/delete" => WEBHOOK_DELETE_NO_SUCH_ID,
            _ => WEBHOOK_SHOW_NO_SUCH_ID,
        };
        Self::client("No such webhook.", "NO_SUCH_WEBHOOK", id, 400)
    }

    /// Gaps final wiring: `webhooks/create` over the per-user cap.
    /// Upstream has no limit error; pinetree-defined `TOO_MANY_WEBHOOKS`.
    pub fn webhook_too_many() -> Self {
        Self::client(
            "You cannot create webhooks any more.",
            "TOO_MANY_WEBHOOKS",
            WEBHOOK_CREATE_TOO_MANY_ID,
            400,
        )
    }

    /// Gaps final wiring: `users/update-memo` `NO_SUCH_USER` (target
    /// missing; per-endpoint UUID like `users/show`).
    pub fn update_memo_no_such_user() -> Self {
        Self::client(
            "No such user.",
            "NO_SUCH_USER",
            UPDATE_MEMO_NO_SUCH_USER_ID,
            400,
        )
    }

    /// Gaps final wiring: `notes/thread-muting/{create,delete}`
    /// `NO_SUCH_NOTE` (per-endpoint UUID, upstream convention).
    pub fn thread_mute_no_such_note(endpoint: &str) -> Self {
        let id = match endpoint {
            "notes/thread-muting/delete" => THREAD_MUTE_DELETE_NO_SUCH_NOTE_ID,
            _ => THREAD_MUTE_CREATE_NO_SUCH_NOTE_ID,
        };
        Self::client("No such note.", "NO_SUCH_NOTE", id, 400)
    }

    /// Gaps final wiring: `gallery/posts/like` + `flash/like`
    /// `ALREADY_LIKED` (kept out of the shared lookup so the pages
    /// `ALREADY_LIKED` UUID is untouched; pinetree-minted —
    /// `// TODO(final): verify against misskey`).
    pub fn gallery_already_liked() -> Self {
        Self::client(
            "The item has already been liked.",
            "ALREADY_LIKED",
            GALLERY_ALREADY_LIKED_ID,
            400,
        )
    }

    /// Gaps final wiring: `gallery/posts/unlike` + `flash/unlike`
    /// `NOT_LIKED` (same non-shared treatment as the like twin).
    pub fn gallery_not_liked() -> Self {
        Self::client(
            "You have not liked that item.",
            "NOT_LIKED",
            GALLERY_NOT_LIKED_ID,
            400,
        )
    }

    /// Misskey `clips/add-note` `ALREADY_CLIPPED` (verbatim).
    pub fn clip_already_clipped() -> Self {
        Self::client(
            "The note has already been clipped.",
            "ALREADY_CLIPPED",
            CLIP_ALREADY_CLIPPED_ID,
            400,
        )
    }

    /// Misskey `clips/add-note` `TOO_MANY_CLIP_NOTES` (verbatim).
    pub fn clip_too_many_clip_notes() -> Self {
        Self::client(
            "You cannot add notes to the clip any more.",
            "TOO_MANY_CLIP_NOTES",
            CLIP_TOO_MANY_CLIP_NOTES_ID,
            400,
        )
    }

    /// Misskey `clips/favorite` `ALREADY_FAVORITED` (verbatim).
    pub fn clip_already_favorited() -> Self {
        Self::client(
            "The clip has already been favorited.",
            "ALREADY_FAVORITED",
            CLIP_ALREADY_FAVORITED_ID,
            400,
        )
    }

    /// Misskey `clips/unfavorite` `NOT_FAVORITED` (verbatim).
    pub fn clip_not_favorited() -> Self {
        Self::client(
            "You have not favorited the clip.",
            "NOT_FAVORITED",
            CLIP_NOT_FAVORITED_ID,
            400,
        )
    }

    /// Pinetree `clips/followers` non-owner read (`Access denied.`,
    /// pinetree-fixed UUID — follower sets never advertise to non-owners).
    pub fn clip_followers_forbidden() -> Self {
        Self::client(
            "Access denied.",
            "ACCESS_DENIED",
            CLIP_FOLLOWERS_FORBIDDEN_ID,
            403,
        )
    }

    /// Misskey `pages/*` `NO_SUCH_PAGE` (`No such page.`, verbatim
    /// per-endpoint UUID).
    pub fn page_no_such_page(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/create" => PAGE_CREATE_NO_SUCH_FILE_ID,
            "pages/update" => PAGE_UPDATE_NO_SUCH_PAGE_ID,
            "pages/delete" => PAGE_DELETE_NO_SUCH_PAGE_ID,
            "pages/show" => PAGE_SHOW_NO_SUCH_PAGE_ID,
            "pages/like" => PAGE_LIKE_NO_SUCH_PAGE_ID,
            "pages/unlike" => PAGE_UNLIKE_NO_SUCH_PAGE_ID,
            _ => PAGE_SHOW_NO_SUCH_PAGE_ID,
        };
        // `pages/create` never emits `NO_SUCH_PAGE`; defensive arm only.
        Self::client("No such page.", "NO_SUCH_PAGE", id, 400)
    }

    /// Misskey `pages/{update,delete}` `ACCESS_DENIED` (`Access denied.`,
    /// verbatim per-endpoint UUID).
    pub fn page_access_denied(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/delete" => PAGE_DELETE_ACCESS_DENIED_ID,
            _ => PAGE_UPDATE_ACCESS_DENIED_ID,
        };
        Self::permission("Access denied.", "ACCESS_DENIED", id, 403)
    }

    /// Misskey `pages/{create,update}` `NO_SUCH_FILE` (`No such file.`,
    /// verbatim per-endpoint UUID — the eye-catch image must be owned).
    pub fn page_no_such_file(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/update" => PAGE_UPDATE_NO_SUCH_FILE_ID,
            _ => PAGE_CREATE_NO_SUCH_FILE_ID,
        };
        Self::client("No such file.", "NO_SUCH_FILE", id, 400)
    }

    /// Misskey `pages/{create,update}` `NAME_ALREADY_EXISTS` (`Specified
    /// name already exists.`, verbatim per-endpoint UUID).
    pub fn page_name_already_exists(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/update" => PAGE_UPDATE_NAME_ALREADY_EXISTS_ID,
            _ => PAGE_CREATE_NAME_ALREADY_EXISTS_ID,
        };
        Self::client(
            "Specified name already exists.",
            "NAME_ALREADY_EXISTS",
            id,
            400,
        )
    }

    /// Misskey `pages/like` `YOUR_PAGE` (verbatim).
    pub fn page_your_page() -> Self {
        Self::client(
            "You cannot like your page.",
            "YOUR_PAGE",
            PAGE_YOUR_PAGE_ID,
            400,
        )
    }

    /// Misskey `pages/like` `ALREADY_LIKED` (verbatim).
    pub fn page_already_liked() -> Self {
        Self::client(
            "The page has already been liked.",
            "ALREADY_LIKED",
            PAGE_ALREADY_LIKED_ID,
            400,
        )
    }

    /// Misskey `pages/unlike` `NOT_LIKED` (verbatim).
    pub fn page_not_liked() -> Self {
        Self::client(
            "You have not liked that page.",
            "NOT_LIKED",
            PAGE_NOT_LIKED_ID,
            400,
        )
    }

    /// 500 `INTERNAL_ERROR`.
    pub fn internal() -> Self {
        Self::new(
            "Internal error occurred. Please contact us if the error persists.",
            "INTERNAL_ERROR",
            INTERNAL_ERROR_ID,
            "server",
            500,
        )
    }

    /// Map a domain/port error carrying a Misskey error ident
    /// (`NO_SUCH_NOTE`, `ALREADY_FOLLOWING`, …) to its exact Misskey
    /// error code, message, id, and HTTP status.
    pub fn from_port_error(err: PortError) -> Self {
        match err {
            PortError::StorageUnavailable(msg) => {
                Self::internal().with_info(Value::String(msg))
            }
            PortError::Validation(msg) => Self::from_code_string(&msg, 400),
            PortError::Conflict(msg) => Self::from_code_string(&msg, 400),
            PortError::NotFound(msg) => {
                let mapped = Self::from_code_string(&msg, 400);
                if is_known_client_code(&mapped.code) {
                    mapped
                } else {
                    // Genuinely unknown resource: 404.
                    let code = code_token(&msg).unwrap_or_else(|| "NO_SUCH_RESOURCE".to_string());
                    Self::client(&format!("No such resource: {code}."), &code, &new_id_for(&code), 404)
                }
            }
        }
    }

    /// Build an error from a free-form domain message whose leading token
    /// is the Misskey error code. Falls back to a generic client error
    /// with `fallback_status`.
    fn from_code_string(msg: &str, fallback_status: u16) -> Self {
        if let Some(code) = code_token(msg) {
            if let Some((message, id, status, kind)) = lookup_known_code(&code) {
                let mut err = Self::new(message, &code, id, kind, status);
                // Keep any trailing detail after the code token as info.
                let rest = msg[code.len()..].trim();
                if !rest.is_empty() {
                    err.info = Some(Value::String(rest.to_string()));
                }
                return err;
            }
            // Unknown code token: preserve it verbatim.
            let id = new_id_for(&code);
            return Self::new(msg, code, &id, "client", fallback_status);
        }
        Self::client(msg, "INVALID_PARAM", INVALID_PARAM_ID, fallback_status)
            .with_info(Value::Object({
                let mut info = Map::new();
                info.insert("reason".to_string(), Value::String(msg.to_string()));
                info
            }))
    }

    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// Full response body: top-level `id`/`code`/`message` plus the nested
    /// Misskey `error` object.
    pub fn body(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            serde_json::json!({
                "id": self.id,
                "code": self.code,
                "message": self.message,
                "error": {
                    "message": self.message,
                    "code": self.code,
                    "id": self.id,
                    "kind": self.kind,
                },
            })
        })
    }

    fn from_value(v: &Value) -> Self {
        let obj = v
            .get("error")
            .and_then(Value::as_object)
            .or_else(|| v.as_object());
        let message = obj
            .and_then(|o| o.get("message"))
            .and_then(Value::as_str);
        let code = obj
            .and_then(|o| o.get("code"))
            .and_then(Value::as_str);
        let id = obj.and_then(|o| o.get("id")).and_then(Value::as_str);
        let kind = obj
            .and_then(|o| o.get("kind"))
            .and_then(Value::as_str);
        let info = obj.and_then(|o| o.get("info")).cloned();
        let code = code.unwrap_or("INTERNAL_ERROR").to_string();
        let (_, _, status, _) = lookup_known_code(&code)
            .map(|(m, i, s, k)| (m, i, s, k))
            .unwrap_or((
                "Internal error occurred.",
                INTERNAL_ERROR_ID,
                500u16,
                "server",
            ));
        Self {
            message: message.unwrap_or("Internal error occurred.").to_string(),
            code,
            id: id.unwrap_or(INTERNAL_ERROR_ID).to_string(),
            kind: kind.unwrap_or("server").to_string(),
            info,
            http_status: status,
            retry_after_secs: None,
        }
    }
}

/// Known Misskey codes that answer `400 Bad Request` with `kind: "client"`.
fn is_known_client_code(code: &str) -> bool {
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
fn lookup_known_code(code: &str) -> Option<(&'static str, &'static str, u16, &'static str)> {
    Some(match code {
        "UNKNOWN_API_ENDPOINT" => ("Unknown API endpoint.", UNKNOWN_API_ENDPOINT_ID, 404, "client"),
        "AUTHENTICATION_FAILED" => (
            "Authentication failed. Please ensure your token is correct.",
            AUTHENTICATION_FAILED_ID,
            401,
            "client",
        ),
        "CREDENTIAL_REQUIRED" => ("Credential required.", CREDENTIAL_REQUIRED_ID, 401, "client"),
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
        "CANNOT_REPORT_YOURSELF" => ("Cannot report yourself.", CANNOT_REPORT_YOURSELF_ID, 400, "client"),
        "CANNOT_REPORT_THE_ADMIN" => ("Cannot report the admin.", CANNOT_REPORT_THE_ADMIN_ID, 400, "client"),
        "NO_SUCH_ABUSE_REPORT" => ("No such abuse report.", NO_SUCH_ABUSE_REPORT_ID, 404, "server"),
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
        "URI_NULL" => (
            "User ActivityPup URI is null.",
            URI_NULL_ID,
            400,
            "client",
        ),
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
        "NOT_FOLLOWING" => ("You are not following that user.", NOT_FOLLOWING_ID, 400, "client"),
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
        "NOT_MUTING" => ("You are not muting that user.", NOT_MUTING_ID, 400, "client"),
        "BLOCKING" => (
            "You are blocking that user.",
            BLOCKING_ID,
            400,
            "client",
        ),
        "BLOCKED" => (
            "You are blocked by that user.",
            BLOCKED_ID,
            400,
            "client",
        ),
        "ALREADY_REACTED" => (
            "You are already reacting to that note.",
            ALREADY_REACTED_ID,
            400,
            "client",
        ),
        "NOT_REACTED" => ("You are not reacting to that note.", NOT_REACTED_ID, 400, "client"),
        "ALREADY_FAVORITED" => (
            "You have already favorited that note.",
            ALREADY_FAVORITED_ID,
            400,
            "client",
        ),
        "NOT_FAVORITED" => ("You have not favorited that note.", NOT_FAVORITED_ID, 400, "client"),
        "CANNOT_REACT_TO_RENOTE" => (
            "You cannot react to Renote.",
            CANNOT_REACT_TO_RENOTE_ID,
            400,
            "client",
        ),
        "CANNOT_RENOTE" | "CANNOT_RENOTE_TO_A_PURE_RENOTE" | "CANNOT_RENOTE_DUE_TO_VISIBILITY" => {
            (
                "You can not Renote due to target visibility.",
                CANNOT_RENOTE_ID,
                400,
                "client",
            )
        }
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
        "ALREADY_VOTED" => (
            "You have already voted.",
            ALREADY_VOTED_ID,
            400,
            "client",
        ),
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
        "SCHEDULE_NOTE_MAX" => (
            "Schedule note max.",
            SCHEDULE_NOTE_MAX_ID,
            400,
            "client",
        ),
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
        "NO_SUCH_NOTE_DRAFT" => (
            "No such note draft.",
            NO_SUCH_NOTE_DRAFT_ID,
            400,
            "client",
        ),
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
        "NO_SUCH_EMOJI" => (
            "No such emoji.",
            NO_SUCH_EMOJI_ID,
            400,
            "client",
        ),
        "DUPLICATE_NAME" => (
            "Duplicate name.",
            DUPLICATE_NAME_ID,
            400,
            "client",
        ),
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
        "NO_SUCH_ANTENNA" => (
            "No such antenna.",
            NO_SUCH_ANTENNA_ID,
            400,
            "client",
        ),
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
        "NO_SUCH_USER_LIST" => (
            "No such user list.",
            NO_SUCH_USER_LIST_ID,
            400,
            "client",
        ),
        "NO_SUCH_LIST" => (
            "No such list.",
            LIST_NO_SUCH_LIST_SHOW_ID,
            400,
            "client",
        ),
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
        "INCORRECT_PASSWORD" => (
            "Incorrect password.",
            INCORRECT_PASSWORD_ID,
            400,
            "client",
        ),
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
        "NO_SUCH_ENDPOINT" => (
            "No such endpoint.",
            NO_SUCH_ENDPOINT_ID,
            404,
            "client",
        ),
        "NO_SUCH_HASHTAG" => (
            "No such hashtag.",
            NO_SUCH_HASHTAG_ID,
            400,
            "client",
        ),
        "NO_SUCH_POST" => ("No such post.", NO_SUCH_POST_ID, 400, "client"),
        "NO_SUCH_FLASH" => ("No such flash.", NO_SUCH_FLASH_ID, 400, "client"),
        _ => return None,
    })
}

/// Leading `LOUD_SNAKE` token of a domain message, if any.
fn code_token(msg: &str) -> Option<String> {
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
fn new_id_for(code: &str) -> String {
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
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
        b[12], b[13], b[14], b[15]
    )
}

impl Serialize for ApiError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut nested = Map::new();
        nested.insert("message".to_string(), Value::String(self.message.clone()));
        nested.insert("code".to_string(), Value::String(self.code.clone()));
        nested.insert("id".to_string(), Value::String(self.id.clone()));
        nested.insert("kind".to_string(), Value::String(self.kind.clone()));
        if let Some(info) = &self.info {
            nested.insert("info".to_string(), info.clone());
        }
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("code", &self.code)?;
        map.serialize_entry("message", &self.message)?;
        map.serialize_entry("error", &Value::Object(nested))?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for ApiError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer)?;
        Ok(Self::from_value(&v))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut response = (status, Json(self.body())).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                "WWW-Authenticate",
                HeaderValue::from_static("Bearer realm=\"Misskey\""),
            );
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            if let Some(secs) = self.retry_after_secs {
                if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
                    response.headers_mut().insert("Retry-After", value);
                }
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_has_top_level_and_nested_fields() {
        let err = ApiError::unsupported_field("channelId");
        let body = err.body();
        assert_eq!(body["code"], "UNSUPPORTED_FIELD");
        assert_eq!(body["error"]["code"], "UNSUPPORTED_FIELD");
        assert_eq!(body["error"]["kind"], "client");
        assert_eq!(body["error"]["info"]["param"], "channelId");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn status_mapping() {
        assert_eq!(ApiError::unknown_endpoint().status(), StatusCode::NOT_FOUND);
        assert_eq!(
            ApiError::authentication_failed().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(ApiError::permission_denied().status(), StatusCode::FORBIDDEN);
        assert_eq!(ApiError::access_denied().status(), StatusCode::FORBIDDEN);
        assert_eq!(ApiError::user_suspended().status(), StatusCode::FORBIDDEN);
        assert_eq!(
            ApiError::rate_limit_exceeded(42).status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            ApiError::max_file_size_exceeded().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn port_error_mapping_keeps_misskey_codes() {
        let err = ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        assert_eq!(err.code, "NO_SUCH_NOTE");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.kind, "client");

        let err = ApiError::from_port_error(PortError::Validation(
            "ALREADY_FOLLOWING".to_string(),
        ));
        assert_eq!(err.code, "ALREADY_FOLLOWING");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);

        let err = ApiError::from_port_error(PortError::Validation(
            "MAX_FILE_SIZE_EXCEEDED".to_string(),
        ));
        assert_eq!(err.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let err = ApiError::from_port_error(PortError::NotFound("NO_SUCH_WIDGET".to_string()));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn rate_limit_body_carries_retry_after() {
        let err = ApiError::rate_limit_exceeded(30);
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get("Retry-After").unwrap(),
            "30"
        );
    }
}
