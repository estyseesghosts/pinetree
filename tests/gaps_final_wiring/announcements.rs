//! Announcement arms: announcements/show, i/read-announcement, promo/read, and the i badge.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str) {
    // --- announcements ----------------------------------------------------------
    let (status, value) = srv.api("announcements", json!(null), None).await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "announcements/show",
            json!({"announcementId": "nope"}),
            None,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_ANNOUNCEMENT");
    let (status, value) = srv
        .api(
            "i/read-announcement",
            json!({"announcementId": "nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_ANNOUNCEMENT");
    let (status, _) = srv.api("promo/read", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 204);
    let (status, value) = srv.api("i", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value["hasUnreadAnnouncement"], json!(false));
    assert_eq!(value["unreadAnnouncements"], json!([]));
}
