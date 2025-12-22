use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Container {
    pub message_id: String,
    pub email_id: Option<i32>,
    pub parent: Option<String>,
    pub children: Vec<String>,
}

impl Container {
    pub fn new_with_email(message_id: String, email_id: i32) -> Self {
        Self {
            message_id,
            email_id: Some(email_id),
            parent: None,
            children: Vec::new(),
        }
    }

    pub fn new_phantom(message_id: String) -> Self {
        Self {
            message_id,
            email_id: None,
            parent: None,
            children: Vec::new(),
        }
    }

    pub fn add_child(&mut self, child_id: String) {
        if !self.children.contains(&child_id) {
            self.children.push(child_id);
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmailData {
    pub id: i32,
    pub message_id: String,
    pub subject: String,
    pub normalized_subject: String,
    pub in_reply_to: Option<String>,
    pub date: DateTime<Utc>,
}

#[derive(Debug)]
pub struct ThreadInfo {
    pub root_message_id: String,
    pub subject: String,
    pub start_date: DateTime<Utc>,
    pub last_date: DateTime<Utc>,
    pub emails: Vec<(i32, i32)>,
}

impl ThreadInfo {
    pub fn new(
        root_message_id: String,
        subject: String,
        start_date: DateTime<Utc>,
        last_date: DateTime<Utc>,
    ) -> Self {
        Self {
            root_message_id,
            subject,
            start_date,
            last_date,
            emails: Vec::new(),
        }
    }
}
