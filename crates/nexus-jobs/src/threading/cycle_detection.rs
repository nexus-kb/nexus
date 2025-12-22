use std::collections::{HashMap, HashSet};

use super::container::Container;

pub fn detect_cycle_in_ancestry(
    containers: &HashMap<String, Container>,
    child_message_id: &str,
    parent_message_id: &str,
) -> bool {
    let mut visited = HashSet::new();
    let mut current = Some(parent_message_id.to_string());

    while let Some(message_id) = current {
        if !visited.insert(message_id.clone()) {
            return true;
        }
        if message_id == child_message_id {
            return true;
        }
        current = containers
            .get(&message_id)
            .and_then(|container| container.parent.clone());
    }

    false
}
