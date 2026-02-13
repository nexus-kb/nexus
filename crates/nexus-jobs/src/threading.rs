use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct ThreadingInputMessage {
    pub message_pk: i64,
    pub message_id_primary: String,
    pub subject_raw: String,
    pub subject_norm: String,
    pub date_utc: Option<DateTime<Utc>>,
    pub references_ids: Vec<String>,
    pub in_reply_to_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BuiltThreadNode {
    pub node_key: String,
    pub message_pk: Option<i64>,
    pub parent_node_key: Option<String>,
    pub depth: i32,
    pub sort_key: Vec<u8>,
    pub is_dummy: bool,
}

#[derive(Debug, Clone)]
pub struct BuiltThreadMessage {
    pub message_pk: i64,
    pub parent_message_pk: Option<i64>,
    pub depth: i32,
    pub sort_key: Vec<u8>,
    pub is_dummy: bool,
}

#[derive(Debug, Clone)]
pub struct BuiltThreadSummary {
    pub root_node_key: String,
    pub root_message_pk: Option<i64>,
    pub subject_norm: String,
    pub created_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub message_count: i32,
    pub membership_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BuiltThreadComponent {
    pub summary: BuiltThreadSummary,
    pub nodes: Vec<BuiltThreadNode>,
    pub messages: Vec<BuiltThreadMessage>,
}

#[derive(Debug, Clone, Default)]
pub struct BuildOutcome {
    pub components: Vec<BuiltThreadComponent>,
}

#[derive(Debug, Clone)]
struct Node {
    node_key: String,
    message_pk: Option<i64>,
    message_id_primary: Option<String>,
    subject_raw: String,
    subject_norm: String,
    date_utc: DateTime<Utc>,
    parent_node_key: Option<String>,
    children: BTreeSet<String>,
    is_dummy: bool,
}

impl Node {
    fn sort_tuple(&self) -> (DateTime<Utc>, String, String) {
        (
            self.date_utc,
            self.subject_norm.clone(),
            self.node_key.clone(),
        )
    }
}

pub fn build_threads(messages: Vec<ThreadingInputMessage>) -> BuildOutcome {
    if messages.is_empty() {
        return BuildOutcome::default();
    }

    let mut ordered_messages = messages;
    ordered_messages.sort_by_key(|msg| {
        (
            msg.date_utc.unwrap_or_else(epoch_utc),
            msg.subject_norm.clone(),
            msg.message_pk,
        )
    });

    let mut nodes: HashMap<String, Node> = HashMap::new();
    let mut message_pk_to_node_key: HashMap<i64, String> = HashMap::new();
    let mut message_order: Vec<i64> = Vec::new();
    let mut canonical_key_to_node_key: HashMap<String, String> = HashMap::new();

    for msg in &ordered_messages {
        let canonical_key = canonical_message_key(msg);
        let node_key = if canonical_key.starts_with("synthetic:msgpk:") {
            canonical_key.clone()
        } else if canonical_key_to_node_key.contains_key(&canonical_key) {
            format!("duplicate:{canonical_key}:msgpk:{}", msg.message_pk)
        } else {
            canonical_key.clone()
        };

        if !canonical_key.starts_with("synthetic:msgpk:")
            && !canonical_key_to_node_key.contains_key(&canonical_key)
        {
            canonical_key_to_node_key.insert(canonical_key, node_key.clone());
        }

        nodes.insert(
            node_key.clone(),
            Node {
                node_key: node_key.clone(),
                message_pk: Some(msg.message_pk),
                message_id_primary: Some(msg.message_id_primary.clone()),
                subject_raw: msg.subject_raw.clone(),
                subject_norm: msg.subject_norm.clone(),
                date_utc: msg.date_utc.unwrap_or_else(epoch_utc),
                parent_node_key: None,
                children: BTreeSet::new(),
                is_dummy: false,
            },
        );
        message_pk_to_node_key.insert(msg.message_pk, node_key);
        message_order.push(msg.message_pk);
    }

    for message_pk in &message_order {
        let Some(node_key) = message_pk_to_node_key.get(message_pk).cloned() else {
            continue;
        };
        let Some(message) = ordered_messages.iter().find(|msg| msg.message_pk == *message_pk) else {
            continue;
        };

        let reference_chain = build_reference_chain(message);
        let mut chain_node_keys = Vec::new();

        for reference_id in reference_chain {
            let resolved_key = if let Some(existing) = canonical_key_to_node_key.get(&reference_id) {
                existing.clone()
            } else {
                nodes
                    .entry(reference_id.clone())
                    .or_insert_with(|| Node {
                        node_key: reference_id.clone(),
                        message_pk: None,
                        message_id_primary: None,
                        subject_raw: String::new(),
                        subject_norm: String::new(),
                        date_utc: epoch_utc(),
                        parent_node_key: None,
                        children: BTreeSet::new(),
                        is_dummy: true,
                    });
                reference_id
            };
            chain_node_keys.push(resolved_key);
        }

        let mut prev: Option<String> = None;
        for chain_key in &chain_node_keys {
            if let Some(parent_key) = prev.as_deref() {
                let _ = try_attach_parent(&mut nodes, parent_key, chain_key);
            }
            prev = Some(chain_key.clone());
        }
        if let Some(parent_key) = prev.as_deref() {
            let _ = try_attach_parent(&mut nodes, parent_key, &node_key);
        }
    }

    apply_subject_fallback(&mut nodes);

    let mut roots: Vec<String> = nodes
        .values()
        .filter(|node| node.parent_node_key.is_none())
        .map(|node| node.node_key.clone())
        .collect();
    roots.sort_by_key(|key| {
        nodes
            .get(key)
            .map(|node| node.sort_tuple())
            .unwrap_or_else(|| (epoch_utc(), String::new(), key.clone()))
    });

    let mut components = Vec::new();
    for (root_idx, root_key) in roots.iter().enumerate() {
        let mut ordered_nodes = Vec::new();
        let mut path = vec![(root_idx + 1) as u32];
        walk_preorder(&nodes, root_key, &mut path, &mut ordered_nodes);

        if ordered_nodes.is_empty() {
            continue;
        }

        let mut thread_nodes = Vec::new();
        let mut thread_messages = Vec::new();
        let mut dates = Vec::new();
        let mut membership_tokens = Vec::new();

        for entry in &ordered_nodes {
            let Some(node) = nodes.get(&entry.node_key) else {
                continue;
            };

            thread_nodes.push(BuiltThreadNode {
                node_key: node.node_key.clone(),
                message_pk: node.message_pk,
                parent_node_key: node.parent_node_key.clone(),
                depth: entry.depth,
                sort_key: entry.sort_key.clone(),
                is_dummy: node.is_dummy,
            });

            if let Some(message_pk) = node.message_pk {
                let parent_message_pk = nearest_real_parent_message_pk(&nodes, &node.node_key);
                thread_messages.push(BuiltThreadMessage {
                    message_pk,
                    parent_message_pk,
                    depth: entry.depth,
                    sort_key: entry.sort_key.clone(),
                    is_dummy: false,
                });

                dates.push(node.date_utc);
                let token = node
                    .message_id_primary
                    .as_ref()
                    .map(|v| normalize_message_id_token(v))
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| format!("synthetic:msgpk:{message_pk}"));
                membership_tokens.push(token);
            }
        }

        if thread_messages.is_empty() {
            continue;
        }

        let root_real_message_pk = ordered_nodes.iter().find_map(|entry| {
            nodes.get(&entry.node_key).and_then(|node| {
                if node.message_pk.is_some() {
                    node.message_pk
                } else {
                    None
                }
            })
        });

        let subject_norm = root_real_message_pk
            .and_then(|pk| {
                nodes
                    .values()
                    .find(|node| node.message_pk == Some(pk))
                    .map(|node| node.subject_norm.clone())
            })
            .unwrap_or_default();

        let created_at = dates.iter().min().copied().unwrap_or_else(epoch_utc);
        let last_activity_at = dates.iter().max().copied().unwrap_or_else(epoch_utc);

        let membership_hash = compute_membership_hash(&membership_tokens);
        let message_count = thread_messages.len() as i32;

        components.push(BuiltThreadComponent {
            summary: BuiltThreadSummary {
                root_node_key: root_key.clone(),
                root_message_pk: root_real_message_pk,
                subject_norm,
                created_at,
                last_activity_at,
                message_count,
                membership_hash,
            },
            nodes: thread_nodes,
            messages: thread_messages,
        });
    }

    BuildOutcome { components }
}

#[derive(Debug, Clone)]
struct TraversedNode {
    node_key: String,
    depth: i32,
    sort_key: Vec<u8>,
}

fn walk_preorder(
    nodes: &HashMap<String, Node>,
    node_key: &str,
    path: &mut Vec<u32>,
    out: &mut Vec<TraversedNode>,
) {
    let depth = (path.len().saturating_sub(1)) as i32;
    out.push(TraversedNode {
        node_key: node_key.to_string(),
        depth,
        sort_key: encode_sort_key(path),
    });

    let Some(node) = nodes.get(node_key) else {
        return;
    };

    let mut children: Vec<String> = node.children.iter().cloned().collect();
    children.sort_by_key(|key| {
        nodes
            .get(key)
            .map(|n| n.sort_tuple())
            .unwrap_or_else(|| (epoch_utc(), String::new(), key.clone()))
    });

    for (idx, child_key) in children.iter().enumerate() {
        path.push((idx + 1) as u32);
        walk_preorder(nodes, child_key, path, out);
        let _ = path.pop();
    }
}

fn apply_subject_fallback(nodes: &mut HashMap<String, Node>) {
    let mut roots: Vec<String> = nodes
        .values()
        .filter(|node| node.parent_node_key.is_none() && node.message_pk.is_some())
        .map(|node| node.node_key.clone())
        .collect();
    roots.sort_by_key(|key| {
        nodes
            .get(key)
            .map(|node| node.sort_tuple())
            .unwrap_or_else(|| (epoch_utc(), String::new(), key.clone()))
    });

    let root_snapshot = roots.clone();
    for child_root in root_snapshot {
        let Some(child) = nodes.get(&child_root) else {
            continue;
        };
        if !is_reply_subject(&child.subject_raw) || child.subject_norm.is_empty() {
            continue;
        }

        let mut parent_candidates: Vec<String> = roots
            .iter()
            .filter(|candidate| **candidate != child_root)
            .filter_map(|candidate| {
                nodes.get(candidate).and_then(|node| {
                    if node.parent_node_key.is_none()
                        && node.message_pk.is_some()
                        && node.subject_norm == child.subject_norm
                    {
                        Some(candidate.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        parent_candidates.sort_by_key(|key| {
            let node = nodes.get(key).expect("candidate exists");
            (
                is_reply_subject(&node.subject_raw),
                node.sort_tuple(),
            )
        });

        if let Some(parent_key) = parent_candidates.first() {
            let _ = try_attach_parent(nodes, parent_key, &child_root);
        }
    }
}

fn try_attach_parent(nodes: &mut HashMap<String, Node>, parent_key: &str, child_key: &str) -> bool {
    if parent_key == child_key {
        return false;
    }

    if !nodes.contains_key(parent_key) || !nodes.contains_key(child_key) {
        return false;
    }

    if nodes
        .get(child_key)
        .and_then(|node| node.parent_node_key.as_deref())
        .is_some()
    {
        return false;
    }

    if introduces_cycle(nodes, parent_key, child_key) {
        return false;
    }

    if let Some(child) = nodes.get_mut(child_key) {
        child.parent_node_key = Some(parent_key.to_string());
    }
    if let Some(parent) = nodes.get_mut(parent_key) {
        parent.children.insert(child_key.to_string());
    }
    true
}

fn introduces_cycle(nodes: &HashMap<String, Node>, parent_key: &str, child_key: &str) -> bool {
    let mut current = Some(parent_key.to_string());
    while let Some(key) = current {
        if key == child_key {
            return true;
        }
        current = nodes
            .get(&key)
            .and_then(|node| node.parent_node_key.clone());
    }
    false
}

fn nearest_real_parent_message_pk(nodes: &HashMap<String, Node>, node_key: &str) -> Option<i64> {
    let mut current = nodes
        .get(node_key)
        .and_then(|node| node.parent_node_key.clone());
    while let Some(parent_key) = current {
        let parent = nodes.get(&parent_key)?;
        if let Some(message_pk) = parent.message_pk {
            return Some(message_pk);
        }
        current = parent.parent_node_key.clone();
    }
    None
}

fn build_reference_chain(message: &ThreadingInputMessage) -> Vec<String> {
    if !message.references_ids.is_empty() {
        message
            .references_ids
            .iter()
            .map(|v| normalize_message_id_token(v))
            .filter(|v| !v.is_empty())
            .collect()
    } else {
        message
            .in_reply_to_ids
            .first()
            .map(|v| normalize_message_id_token(v))
            .filter(|v| !v.is_empty())
            .map(|v| vec![v])
            .unwrap_or_default()
    }
}

fn canonical_message_key(message: &ThreadingInputMessage) -> String {
    let normalized = normalize_message_id_token(&message.message_id_primary);
    if normalized.is_empty() {
        format!("synthetic:msgpk:{}", message.message_pk)
    } else {
        normalized
    }
}

fn normalize_message_id_token(raw: &str) -> String {
    let mut value = raw.trim().to_ascii_lowercase();
    loop {
        let before = value.clone();
        value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .trim_matches('<')
            .trim_matches('>')
            .trim()
            .to_string();
        if before == value {
            break;
        }
    }
    value
}

fn is_reply_subject(subject: &str) -> bool {
    let normalized = subject.trim().to_ascii_lowercase();
    ["re:", "aw:", "fwd:", "fw:"]
        .iter()
        .any(|prefix| normalized.starts_with(prefix))
}

fn encode_sort_key(path: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(path.len() * 4);
    for segment in path {
        out.extend(segment.to_be_bytes());
    }
    out
}

fn compute_membership_hash(tokens: &[String]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    for token in tokens {
        hasher.update(token.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_vec()
}

fn epoch_utc() -> DateTime<Utc> {
    Utc.timestamp_opt(0, 0).single().expect("unix epoch valid")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};

    use super::{ThreadingInputMessage, build_threads};

    fn msg(
        message_pk: i64,
        message_id_primary: &str,
        subject_raw: &str,
        subject_norm: &str,
        references_ids: &[&str],
        in_reply_to_ids: &[&str],
        ts: i64,
    ) -> ThreadingInputMessage {
        ThreadingInputMessage {
            message_pk,
            message_id_primary: message_id_primary.to_string(),
            subject_raw: subject_raw.to_string(),
            subject_norm: subject_norm.to_string(),
            date_utc: Utc.timestamp_opt(ts, 0).single(),
            references_ids: references_ids.iter().map(|v| v.to_string()).collect(),
            in_reply_to_ids: in_reply_to_ids.iter().map(|v| v.to_string()).collect(),
        }
    }

    #[test]
    fn missing_references_create_dummy_nodes() {
        let outcome = build_threads(vec![msg(
            10,
            "child@example.com",
            "Re: topic",
            "topic",
            &["missing@example.com"],
            &[],
            10,
        )]);

        assert_eq!(outcome.components.len(), 1);
        let component = &outcome.components[0];
        assert_eq!(component.nodes.len(), 2);
        assert!(
            component
                .nodes
                .iter()
                .any(|node| node.node_key == "missing@example.com" && node.is_dummy)
        );
        let child = component
            .nodes
            .iter()
            .find(|node| node.message_pk == Some(10))
            .expect("child node");
        assert_eq!(child.parent_node_key.as_deref(), Some("missing@example.com"));
    }

    #[test]
    fn duplicate_message_ids_first_wins() {
        let outcome = build_threads(vec![
            msg(
                1,
                "dup@example.com",
                "topic",
                "topic",
                &[],
                &[],
                1,
            ),
            msg(
                2,
                "dup@example.com",
                "topic reply",
                "topic reply",
                &[],
                &[],
                2,
            ),
        ]);

        let nodes: Vec<_> = outcome
            .components
            .iter()
            .flat_map(|component| component.nodes.iter())
            .collect();
        assert!(nodes.iter().any(|n| n.node_key == "dup@example.com"));
        assert!(nodes
            .iter()
            .any(|n| n.node_key == "duplicate:dup@example.com:msgpk:2"));
    }

    #[test]
    fn references_take_precedence_over_in_reply_to() {
        let outcome = build_threads(vec![
            msg(1, "ref@example.com", "root", "root", &[], &[], 1),
            msg(2, "reply@example.com", "other", "other", &[], &[], 2),
            msg(
                3,
                "child@example.com",
                "re: root",
                "root",
                &["ref@example.com"],
                &["reply@example.com"],
                3,
            ),
        ]);

        let component = outcome
            .components
            .iter()
            .find(|c| c.messages.iter().any(|m| m.message_pk == 3))
            .expect("component");
        let child = component
            .messages
            .iter()
            .find(|m| m.message_pk == 3)
            .expect("child message");
        assert_eq!(child.parent_message_pk, Some(1));
    }

    #[test]
    fn sort_keys_are_deterministic() {
        let input_a = vec![
            msg(1, "a@example.com", "topic", "topic", &[], &[], 1),
            msg(2, "b@example.com", "re: topic", "topic", &["a@example.com"], &[], 2),
            msg(3, "c@example.com", "re: topic", "topic", &["a@example.com"], &[], 3),
        ];
        let mut input_b = input_a.clone();
        input_b.reverse();

        let outcome_a = build_threads(input_a);
        let outcome_b = build_threads(input_b);

        let keys_a: BTreeMap<i64, Vec<u8>> = outcome_a
            .components
            .iter()
            .flat_map(|c| c.messages.iter().map(|m| (m.message_pk, m.sort_key.clone())))
            .collect();
        let keys_b: BTreeMap<i64, Vec<u8>> = outcome_b
            .components
            .iter()
            .flat_map(|c| c.messages.iter().map(|m| (m.message_pk, m.sort_key.clone())))
            .collect();

        assert_eq!(keys_a, keys_b);
    }

    #[test]
    fn subject_fallback_links_reply_roots_only() {
        let outcome = build_threads(vec![
            msg(1, "root@example.com", "topic", "topic", &[], &[], 1),
            msg(2, "reply@example.com", "Re: topic", "topic", &[], &[], 2),
            msg(3, "other@example.com", "Re: other", "other", &[], &[], 3),
        ]);

        let topic_component = outcome
            .components
            .iter()
            .find(|c| c.messages.iter().any(|m| m.message_pk == 2))
            .expect("topic component");
        let reply = topic_component
            .messages
            .iter()
            .find(|m| m.message_pk == 2)
            .expect("reply");
        assert_eq!(reply.parent_message_pk, Some(1));

        let other_component = outcome
            .components
            .iter()
            .find(|c| c.messages.iter().any(|m| m.message_pk == 3))
            .expect("other component");
        let other = other_component
            .messages
            .iter()
            .find(|m| m.message_pk == 3)
            .expect("other");
        assert_eq!(other.parent_message_pk, None);
    }
}
