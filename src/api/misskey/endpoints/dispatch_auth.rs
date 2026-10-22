//! Auth/session/meta/i dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]

mod apps;
mod me;
mod meta;
mod password;
mod twofa;

pub(crate) use apps::app_create;
pub(crate) use apps::app_show;
pub(crate) use apps::my_apps;
pub(crate) use apps::ping;
pub(crate) use me::i;
pub(crate) use me::i_change_password;
pub(crate) use me::i_regenerate_token;
pub(crate) use me::i_revoke_token;
pub(crate) use me::i_update;
pub(crate) use meta::auth_accept;
pub(crate) use meta::auth_session_generate;
pub(crate) use meta::auth_session_show;
pub(crate) use meta::auth_session_userkey;
pub(crate) use meta::meta;
pub(crate) use meta::miauth_gen_token;
pub(crate) use meta::pack_app_record;
pub(crate) use password::request_reset_password;
pub(crate) use password::reset_password;
pub(crate) use password::signin;
pub(crate) use password::signin_flow;
pub(crate) use password::signin_with_passkey;
pub(crate) use password::signup;
pub(crate) use password::verify_email;
pub(crate) use twofa::i_2fa_authenticate;
pub(crate) use twofa::i_2fa_done;
pub(crate) use twofa::i_2fa_key_done;
pub(crate) use twofa::i_2fa_password_less;
pub(crate) use twofa::i_2fa_register;
pub(crate) use twofa::i_2fa_remove_key;
pub(crate) use twofa::i_2fa_unregister;
pub(crate) use twofa::i_2fa_update_key;
