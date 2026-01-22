pub mod authors;
pub mod docs;
pub mod emails;
pub mod health;
pub mod helpers;
pub mod jobs;
pub mod mailing_lists;
pub mod stats;
pub mod threads;
pub mod webhooks;

/// Default list limit for paginated endpoints.
pub fn default_list_limit() -> i64 {
    100
}

/// Default queue name for job enqueues.
pub fn default_queue() -> String {
    "default".to_string()
}

#[cfg(test)]
mod tests {
    use super::{default_list_limit, default_queue};

    #[test]
    fn default_queue_is_default() {
        assert_eq!(default_queue(), "default");
    }

    #[test]
    fn default_list_limit_is_100() {
        assert_eq!(default_list_limit(), 100);
    }
}
