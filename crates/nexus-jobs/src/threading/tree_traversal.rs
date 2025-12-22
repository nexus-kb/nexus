use std::collections::HashMap;

use super::container::{Container, EmailData};
use chrono::{DateTime, Utc};

pub fn find_first_real_message<'a>(
    root_message_id: &str,
    containers: &HashMap<String, Container>,
    email_data: &'a HashMap<i32, EmailData>,
) -> Option<(String, &'a EmailData)> {
    let mut stack = vec![root_message_id.to_string()];

    while let Some(message_id) = stack.pop() {
        if let Some(container) = containers.get(&message_id) {
            if let Some(email_id) = container.email_id {
                if let Some(data) = email_data.get(&email_id) {
                    return Some((message_id.clone(), data));
                }
            }

            for child in container.children.iter().rev() {
                stack.push(child.clone());
            }
        }
    }

    None
}

pub fn collect_thread_members(
    root_message_id: &str,
    containers: &HashMap<String, Container>,
    email_data: &HashMap<i32, EmailData>,
    starting_depth: i32,
    collected_members: &mut Vec<(i32, i32)>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let mut stack = vec![(root_message_id.to_string(), starting_depth)];
    let mut min_date: Option<DateTime<Utc>> = None;
    let mut max_date: Option<DateTime<Utc>> = None;

    while let Some((message_id, depth)) = stack.pop() {
        if let Some(container) = containers.get(&message_id) {
            if let Some(email_id) = container.email_id {
                collected_members.push((email_id, depth));
                if let Some(data) = email_data.get(&email_id) {
                    match (&min_date, &max_date) {
                        (None, None) => {
                            min_date = Some(data.date);
                            max_date = Some(data.date);
                        }
                        (Some(current_min), Some(current_max)) => {
                            if data.date < *current_min {
                                min_date = Some(data.date);
                            }
                            if data.date > *current_max {
                                max_date = Some(data.date);
                            }
                        }
                        _ => {}
                    }
                }
            }

            for child in container.children.iter().rev() {
                stack.push((child.clone(), depth + 1));
            }
        }
    }

    match (min_date, max_date) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    }
}
