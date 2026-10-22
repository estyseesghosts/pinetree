//! I/notifications/registry/webhook dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]

mod abuse;
mod account;
mod exports;
mod imports;
mod notifications;
mod push;
mod registry;
mod webhooks;

pub(crate) use abuse::abuse_report_notification_recipient_create;
pub(crate) use abuse::abuse_report_notification_recipient_delete;
pub(crate) use abuse::abuse_report_notification_recipient_list;
pub(crate) use abuse::abuse_report_notification_recipient_show;
pub(crate) use abuse::abuse_report_notification_recipient_update;
pub(crate) use abuse::pack_abuse_recipient;
pub(crate) use account::i_apps;
pub(crate) use account::i_authorized_apps;
pub(crate) use account::i_move;
pub(crate) use account::i_signin_history;
pub(crate) use account::i_update_email;
pub(crate) use exports::i_export_antennas;
pub(crate) use exports::i_export_blocking;
pub(crate) use exports::i_export_clips;
pub(crate) use exports::i_export_favorites;
pub(crate) use exports::i_export_following;
pub(crate) use exports::i_export_mute;
pub(crate) use exports::i_export_notes;
pub(crate) use exports::i_export_user_lists;
pub(crate) use imports::i_import_antennas;
pub(crate) use imports::i_import_blocking;
pub(crate) use imports::i_import_clips;
pub(crate) use imports::i_import_following;
pub(crate) use imports::i_import_muting;
pub(crate) use imports::i_import_user_lists;
pub(crate) use notifications::i_notifications;
pub(crate) use notifications::i_notifications_grouped;
pub(crate) use notifications::notifications_flush;
pub(crate) use notifications::notifications_mark_all_as_read;
pub(crate) use notifications::notifications_test_notification;
pub(crate) use push::sw_register;
pub(crate) use push::sw_show_registration;
pub(crate) use push::sw_unregister;
pub(crate) use push::sw_update_registration;
pub(crate) use registry::i_registry_get;
pub(crate) use registry::i_registry_get_all;
pub(crate) use registry::i_registry_get_detail;
pub(crate) use registry::i_registry_keys;
pub(crate) use registry::i_registry_keys_with_type;
pub(crate) use registry::i_registry_remove;
pub(crate) use registry::i_registry_scopes_with_domain;
pub(crate) use registry::i_registry_set;
pub(crate) use webhooks::system_webhook_create;
pub(crate) use webhooks::system_webhook_delete;
pub(crate) use webhooks::system_webhook_list;
pub(crate) use webhooks::system_webhook_show;
pub(crate) use webhooks::system_webhook_update;
pub(crate) use webhooks::webhooks_create;
pub(crate) use webhooks::webhooks_delete;
pub(crate) use webhooks::webhooks_list;
pub(crate) use webhooks::webhooks_show;
pub(crate) use webhooks::webhooks_update;
