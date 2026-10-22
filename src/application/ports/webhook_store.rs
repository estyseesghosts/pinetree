use crate::domain::id::PortError;
use crate::domain::webhook::SystemWebhook;
use crate::domain::webhook::UserWebhook;

/// Durable system-webhook rows + trigger index.
pub trait SystemWebhookStore: Send + Sync {
    fn insert_system_webhook(&self, row: SystemWebhook) -> Result<SystemWebhook, PortError>;
    fn get_system_webhook(&self, id: &str) -> Result<Option<SystemWebhook>, PortError>;
    fn list_system_webhooks(&self) -> Result<Vec<SystemWebhook>, PortError>;
    fn put_system_webhook(&self, row: SystemWebhook) -> Result<SystemWebhook, PortError>;
    fn delete_system_webhook(&self, id: &str) -> Result<bool, PortError>;
    fn active_system_webhooks_for(
        &self,
        trigger: &str,
        limit: usize,
    ) -> Result<Vec<SystemWebhook>, PortError>;
}

/// Durable per-user webhook rows + (owner, id) and trigger indexes.
pub trait UserWebhookStore: Send + Sync {
    fn insert_user_webhook(&self, row: UserWebhook) -> Result<UserWebhook, PortError>;
    fn get_user_webhook(&self, id: &str) -> Result<Option<UserWebhook>, PortError>;
    fn list_user_webhooks(&self, owner_id: &str) -> Result<Vec<UserWebhook>, PortError>;
    fn count_user_webhooks(&self, owner_id: &str) -> Result<usize, PortError>;
    fn put_user_webhook(&self, row: UserWebhook) -> Result<UserWebhook, PortError>;
    fn delete_user_webhook(&self, id: &str) -> Result<bool, PortError>;
    fn active_user_webhooks_for(
        &self,
        owner_id: &str,
        trigger: &str,
        limit: usize,
    ) -> Result<Vec<UserWebhook>, PortError>;
}
