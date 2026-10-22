//! Signup/signin, MiAuth web flow and caller resolution.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]

mod email;
mod miauth;
mod session;
mod signin_flow;
mod signup;

pub(crate) use email::EMAIL_SENDER;
pub use email::configure_smtp;
pub(crate) use email::send_email_now;
pub use email::set_email_sender;
pub(crate) use miauth::hex_val;
pub(crate) use miauth::is_truthy;
pub(crate) use miauth::json_to_string_map;
pub(crate) use miauth::miauth_check;
pub(crate) use miauth::miauth_error_page;
pub(crate) use miauth::miauth_page;
pub(crate) use miauth::miauth_submit;
pub(crate) use miauth::parse_form;
pub(crate) use miauth::pct_decode;
pub(crate) use session::auth_account_key;
pub(crate) use session::auth_ip_key;
pub(crate) use session::client_ip;
pub(crate) use session::rate_key_for;
pub(crate) use session::record_2fa_auth_failure;
pub(crate) use session::record_signin_attempt;
pub(crate) use session::require_caller;
pub(crate) use session::resolve_caller;
pub(crate) use session::session_cookie_value;
pub(crate) use session::verify_and_upgrade_password;
pub(crate) use signin_flow::SigninFlowError;
pub(crate) use signin_flow::decode_base64;
pub(crate) use signin_flow::do_signin_flow;
pub(crate) use signin_flow::is_truthy_value;
pub(crate) use signin_flow::signin_flow;
pub(crate) use signup::SIGNUP_FIELDS;
pub(crate) use signup::SignupError;
pub(crate) use signup::do_signin;
pub(crate) use signup::do_signup;
pub(crate) use signup::signin;
pub(crate) use signup::signup;
