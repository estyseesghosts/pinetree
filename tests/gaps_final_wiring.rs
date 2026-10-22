//! Gaps final-wiring dispatcher tests: every arm added by the final-wiring
//! pass, exercised over HTTP against a live server (same framing as
//! `tests/gaps_streaming.rs`).
//!
//! Covered: `ping`, `endpoint(s)`, `retention`, `reset-db`, `sw/*`,
//! `v2/admin/emoji/list`, `i/2fa/*`, `my/apps`, `app/*`,
//! `following/{update,update-all,list}`, `following/requests/sent`,
//! `drive/files/{check-existence,attached-notes}`, `drive/stream`,
//! `users/pages`, `users/lists/create-from-public`, `gallery/*`,
//! `flash/*`, `hashtags/*`, `announcements*`, `i/read-announcement`,
//! `promo/read`, the `i` announcement badge, the admin families,
//! `federation/*`, `queue/*`, `abuse-report-notification-recipient/*`,
//! and the live-mail hookups (`i/update-email`, `request-reset-password`).
//! Plus the final-wiring 5/5 arms: `drive/folders/*`,
//! `drive/files/{find,move-bulk}` + the folder-aware `drive/files` and
//! `drive/files/update` legs, `notes/thread-muting/*`,
//! `users/update-memo`, `system-webhook/*`, `webhooks/*`,
//! `admin/send-email`. Channels/chat stay 404 by owner SKIP order
//! (`docs/gaps.md:5`; the code was removed from the tree).

#[path = "gaps_final_wiring/admin.rs"]
mod admin;
#[path = "gaps_final_wiring/announcements.rs"]
mod announcements;
#[path = "gaps_final_wiring/channels_chat.rs"]
mod channels_chat;
#[path = "gaps_final_wiring/common.rs"]
mod common;
#[path = "gaps_final_wiring/core.rs"]
mod core;
#[path = "gaps_final_wiring/drive.rs"]
mod drive;
#[path = "gaps_final_wiring/drive_files.rs"]
mod drive_files;
#[path = "gaps_final_wiring/drive_folders.rs"]
mod drive_folders;
#[path = "gaps_final_wiring/emoji_list.rs"]
mod emoji_list;
#[path = "gaps_final_wiring/following.rs"]
mod following;
#[path = "gaps_final_wiring/gallery_flash.rs"]
mod gallery_flash;
#[path = "gaps_final_wiring/hashtags.rs"]
mod hashtags;
#[path = "gaps_final_wiring/live_mail.rs"]
mod live_mail;
#[path = "gaps_final_wiring/send_email.rs"]
mod send_email;
#[path = "gaps_final_wiring/service_worker.rs"]
mod service_worker;
#[path = "gaps_final_wiring/social.rs"]
mod social;
#[path = "gaps_final_wiring/thread_muting_memo.rs"]
mod thread_muting_memo;
#[path = "gaps_final_wiring/two_factor_apps.rs"]
mod two_factor_apps;
#[path = "gaps_final_wiring/webhooks.rs"]
mod webhooks;

use common::Live;

#[tokio::test]
async fn gaps_final_wiring_arms() {
    let srv = Live::start().await;
    let (alice_id, alice_token) = core::run(&srv).await;
    service_worker::run(&srv, &alice_token).await;
    emoji_list::run(&srv, &alice_token).await;
    two_factor_apps::run(&srv, &alice_id, &alice_token).await;
    let (bob_id, bob_token) = following::run(&srv, &alice_token).await;
    let file_id = drive::run(&srv, &alice_id, &alice_token, &bob_token).await;
    social::run(&srv, &alice_token).await;
    gallery_flash::run(&srv, &alice_token, &file_id).await;
    hashtags::run(&srv).await;
    announcements::run(&srv, &alice_token).await;
    admin::run(&srv, &alice_id, &alice_token).await;
    live_mail::run(&srv, &alice_id, &alice_token).await;
    channels_chat::run(&srv, &alice_token).await;
    let folder_id = drive_folders::run(&srv, &alice_id, &alice_token, &bob_token).await;
    drive_files::run(&srv, &alice_id, &alice_token, &folder_id).await;
    thread_muting_memo::run(&srv, &alice_token, &bob_id).await;
    webhooks::run(&srv, &alice_token, &bob_token).await;
    send_email::run(&srv, &alice_token, &bob_token).await;
}
