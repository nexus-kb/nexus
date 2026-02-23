use crate::ingest::{
    CopyMessageBodyRow, CopyMessageRow, build_message_bodies_copy_payload,
    build_messages_copy_payload, encode_bytea_hex, encode_text_array,
};

#[test]
fn encode_bytea_hex_uses_postgres_hex_prefix() {
    assert_eq!(encode_bytea_hex(&[0x00, 0x7f, 0xff]), "\\x007fff");
}

#[test]
fn encode_text_array_escapes_quotes_and_backslashes() {
    let values = vec![
        "a/b".to_string(),
        "with\"quote".to_string(),
        "with\\slash".to_string(),
    ];
    assert_eq!(
        encode_text_array(&values),
        "{\"a/b\",\"with\\\"quote\",\"with\\\\slash\"}"
    );
}

#[test]
fn copy_payload_handles_csv_sensitive_content_and_nulls() {
    let payload = build_message_bodies_copy_payload(&[CopyMessageBodyRow {
        id: 42,
        body_text: Some("a,\"b\"\nline".to_string()),
        diff_text: None,
        search_text: "\\N".to_string(),
        has_diff: true,
        has_attachments: false,
    }])
    .expect("payload should be encoded");
    let encoded = String::from_utf8(payload).expect("payload should be utf-8");
    assert!(encoded.contains(",\\N,"));
    assert!(encoded.contains("\"\\N\""));
    assert!(encoded.contains("\"a,\"\"b\"\"\nline\""));
}

#[test]
fn copy_messages_payload_round_trips_arrays_and_bytea() {
    let payload = build_messages_copy_payload(&[CopyMessageRow {
        id: 7,
        content_hash_sha256: vec![0x00, 0x7f, 0xff],
        subject_raw: "[PATCH] csv".to_string(),
        subject_norm: "csv".to_string(),
        from_name: None,
        from_email: "alice@example.com".to_string(),
        date_utc: None,
        to_raw: Some("list@example.com".to_string()),
        cc_raw: None,
        message_ids: vec![
            "id@example.com".to_string(),
            "with\"quote".to_string(),
            "with\\slash".to_string(),
        ],
        message_id_primary: "id@example.com".to_string(),
        in_reply_to_ids: vec!["parent@example.com".to_string()],
        references_ids: vec!["root@example.com".to_string()],
        mime_type: Some("text/plain".to_string()),
        body_id: 11,
    }])
    .expect("payload should be encoded");

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(payload.as_slice());
    let record = reader
        .records()
        .next()
        .expect("one row")
        .expect("valid csv row");
    assert_eq!(record.get(1), Some("\\x007fff"));
    assert_eq!(
        record.get(9),
        Some("{\"id@example.com\",\"with\\\"quote\",\"with\\\\slash\"}")
    );
    assert_eq!(record.get(4), Some("\\N"));
}
