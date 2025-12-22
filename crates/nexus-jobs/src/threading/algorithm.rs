use std::collections::{BTreeMap, HashMap};

use super::container::{Container, EmailData, ThreadInfo};
use super::cycle_detection::detect_cycle_in_ancestry;
use super::tree_traversal::{collect_thread_members, find_first_real_message};

pub fn build_email_threads(
    email_data: HashMap<i32, EmailData>,
    email_references: HashMap<i32, Vec<String>>,
) -> Vec<ThreadInfo> {
    let mut containers = create_message_containers(&email_data, &email_references);

    build_reference_links(&mut containers, &email_data, &email_references);
    apply_in_reply_to_fallbacks(&mut containers, &email_data);
    apply_subject_fallback(&mut containers, &email_data);

    let roots = identify_thread_roots(&containers);

    roots
        .iter()
        .filter_map(|root| assemble_single_thread(root, &containers, &email_data))
        .collect()
}

fn create_message_containers(
    email_data: &HashMap<i32, EmailData>,
    email_references: &HashMap<i32, Vec<String>>,
) -> HashMap<String, Container> {
    let mut containers = HashMap::new();

    for (_email_id, data) in email_data.iter() {
        containers
            .entry(data.message_id.clone())
            .or_insert_with(|| Container::new_with_email(data.message_id.clone(), data.id));
    }

    for refs in email_references.values() {
        for reference in refs {
            containers
                .entry(reference.clone())
                .or_insert_with(|| Container::new_phantom(reference.clone()));
        }
    }

    containers
}

fn build_reference_links(
    containers: &mut HashMap<String, Container>,
    email_data: &HashMap<i32, EmailData>,
    email_references: &HashMap<i32, Vec<String>>,
) {
    for (email_id, data) in email_data.iter() {
        let msg_id = &data.message_id;
        containers
            .entry(msg_id.clone())
            .or_insert_with(|| Container::new_with_email(msg_id.clone(), *email_id));

        if let Some(refs) = email_references.get(email_id) {
            let mut previous_reference: Option<String> = None;

            for reference_id in refs {
                containers
                    .entry(reference_id.clone())
                    .or_insert_with(|| Container::new_phantom(reference_id.clone()));

                if let Some(prev) = &previous_reference {
                    link_child_to_parent(containers, reference_id, prev);
                }

                previous_reference = Some(reference_id.clone());
            }

            if let Some(last_ref) = previous_reference {
                link_child_to_parent(containers, msg_id, &last_ref);
            }
        }
    }
}

fn apply_in_reply_to_fallbacks(
    containers: &mut HashMap<String, Container>,
    email_data: &HashMap<i32, EmailData>,
) {
    for (_email_id, data) in email_data {
        let msg_id = &data.message_id;
        let has_parent = containers
            .get(msg_id)
            .and_then(|container| container.parent.as_ref())
            .is_some();
        if has_parent {
            continue;
        }

        if let Some(in_reply_to) = &data.in_reply_to {
            if containers.contains_key(in_reply_to) {
                link_child_to_parent(containers, msg_id, in_reply_to);
            }
        }
    }
}

fn apply_subject_fallback(
    containers: &mut HashMap<String, Container>,
    email_data: &HashMap<i32, EmailData>,
) {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for container in containers.values() {
        if container.parent.is_some() {
            continue;
        }
        let Some(email_id) = container.email_id else {
            continue;
        };
        let Some(data) = email_data.get(&email_id) else {
            continue;
        };
        if data.normalized_subject.is_empty() {
            continue;
        }
        groups
            .entry(data.normalized_subject.clone())
            .or_default()
            .push(container.message_id.clone());
    }

    for ids in groups.values() {
        if ids.len() <= 1 {
            continue;
        }
        let mut best_id: Option<String> = None;
        let mut best_date = None;
        for id in ids {
            if let Some(container) = containers.get(id) {
                if let Some(email_id) = container.email_id {
                    if let Some(data) = email_data.get(&email_id) {
                        if best_date.map_or(true, |best| data.date < best) {
                            best_date = Some(data.date);
                            best_id = Some(id.clone());
                        }
                    }
                }
            }
        }

        let Some(root_id) = best_id else {
            continue;
        };

        for id in ids {
            if id == &root_id {
                continue;
            }
            if let Some(container) = containers.get(id) {
                if container.parent.is_some() {
                    continue;
                }
            }
            link_child_to_parent(containers, id, &root_id);
        }
    }
}

fn identify_thread_roots(containers: &HashMap<String, Container>) -> Vec<String> {
    containers
        .values()
        .filter(|container| container.parent.is_none())
        .map(|container| container.message_id.clone())
        .collect()
}

fn assemble_single_thread(
    root_message_id: &str,
    containers: &HashMap<String, Container>,
    email_data: &HashMap<i32, EmailData>,
) -> Option<ThreadInfo> {
    let root_container = containers.get(root_message_id)?;

    if let Some(root_email_id) = root_container.email_id {
        if let Some(root_data) = email_data.get(&root_email_id) {
            let mut members = Vec::new();
            let dates =
                collect_thread_members(root_message_id, containers, email_data, 0, &mut members);
            let (start_date, last_date) = dates.unwrap_or((root_data.date, root_data.date));
            let mut thread_info = ThreadInfo::new(
                root_data.message_id.clone(),
                root_data.subject.clone(),
                start_date,
                last_date,
            );
            thread_info.emails = members;
            return Some(thread_info);
        }
    }

    let (_first_message_id, first_data) =
        find_first_real_message(root_message_id, containers, email_data)?;
    let mut members = Vec::new();
    let dates = collect_thread_members(root_message_id, containers, email_data, -1, &mut members);
    let (start_date, last_date) = dates.unwrap_or((first_data.date, first_data.date));
    let mut thread_info = ThreadInfo::new(
        first_data.message_id.clone(),
        first_data.subject.clone(),
        start_date,
        last_date,
    );
    thread_info.emails = members;
    Some(thread_info)
}

fn link_child_to_parent(
    containers: &mut HashMap<String, Container>,
    child_message_id: &str,
    parent_message_id: &str,
) {
    if child_message_id == parent_message_id {
        return;
    }

    if containers
        .get(child_message_id)
        .and_then(|container| container.parent.as_ref())
        .is_some()
    {
        return;
    }

    if detect_cycle_in_ancestry(containers, child_message_id, parent_message_id) {
        return;
    }

    if let Some(child) = containers.get_mut(child_message_id) {
        child.parent = Some(parent_message_id.to_string());
    }
    if let Some(parent) = containers.get_mut(parent_message_id) {
        parent.add_child(child_message_id.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    fn email(id: i32, message_id: &str, subject: &str, date: DateTime<Utc>) -> EmailData {
        EmailData {
            id,
            message_id: message_id.to_string(),
            subject: subject.to_string(),
            normalized_subject: subject.to_lowercase(),
            in_reply_to: None,
            date,
        }
    }

    #[test]
    fn phantom_root_sets_depth_zero_for_first_real() {
        let mut email_data = HashMap::new();
        let mut references = HashMap::new();

        let date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        email_data.insert(1, email(1, "msg2", "Subject", date));
        references.insert(1, vec!["msg1".to_string()]);

        let threads = build_email_threads(email_data, references);
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].emails.len(), 1);
        assert_eq!(threads[0].emails[0].1, 0);
        assert_eq!(threads[0].root_message_id, "msg2");
    }

    #[test]
    fn subject_fallback_merges_roots() {
        let mut email_data = HashMap::new();
        let references = HashMap::new();

        let date1 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let date2 = Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap();

        email_data.insert(1, email(1, "a@x", "Hello", date1));
        email_data.insert(2, email(2, "b@x", "Hello", date2));

        let threads = build_email_threads(email_data, references);
        assert_eq!(threads.len(), 1);
        let members = &threads[0].emails;
        assert_eq!(members.len(), 2);
        assert_eq!(threads[0].root_message_id, "a@x");
    }
}
