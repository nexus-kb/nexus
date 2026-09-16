#!/usr/bin/env bash
# Requires psql/createdb/dropdb and PG* credentials for a local test server with
# ParadeDB. Creates and removes its own disposable database; never uses the
# caller's PGDATABASE for fixture writes.
set -euo pipefail
root=$(realpath "$(dirname "$0")/..")
database="nexus_timestamp_repair_test_$$"
createdb --maintenance-db=postgres "$database"
export PGDATABASE="$database"
work=$(mktemp -d)
trap 'dropdb --maintenance-db=postgres "$database"; rm -rf "$work"' EXIT
pushd "$work" >/dev/null
sql() { psql -X -v ON_ERROR_STOP=1 "$@"; }
cat "$root"/Sources/NexusKb/Migrations/*.sql | sql >/dev/null

sql <<'SQL'
INSERT INTO threads(id,root_message_id,last_updated_at) OVERRIDING SYSTEM VALUE VALUES
(-1,'cover@test','2085-01-01'),(-2,'other@test',to_timestamp(1600000100)),
(-3,'cross@test','epoch'),(-4,'unknown@test','1988-01-01'),(-5,'early@test','2085-01-01');
INSERT INTO messages(id,message_id,thread_id,sent_at,body) OVERRIDING SYSTEM VALUE VALUES
(-1,'cover@test',-1,to_timestamp(1600000000),'valid cover'),
(-2,'part@test',-1,'2085-01-01','future part'),
(-3,'reply@test',-1,to_timestamp(1600000120),'valid reply'),
(-4,'other@test',-2,to_timestamp(1600000100),'unaffected lineage member'),
(-5,'cross@test',-3,NULL,'cross-post without Date'),
(-6,'unknown@test',-4,'1988-01-01','no usable clocks'),
(-7,'early@test',-5,to_timestamp(1599999940),'valid earlier part'),
(-8,'late@test',-5,'2085-01-01','future later part');
INSERT INTO messages_mailing_lists(message_id,mailing_list_id,archive_blob_oid) VALUES
(-1,1,'cover'),(-2,1,'part'),(-3,1,'reply'),(-4,1,'other'),
(-5,1,'cross-a'),(-5,2,'cross-b'),(-6,1,'unknown'),(-7,1,'early'),(-8,1,'late');
INSERT INTO patchsets(id,thread_id,cover_letter_message_id,sent_at) OVERRIDING SYSTEM VALUE VALUES
(-1,-1,'cover@test','2085-01-01'),(-2,-2,'other@test',to_timestamp(1600000100)),
(-3,-3,'cross@test',NULL),(-4,-4,'unknown@test','1988-01-01'),(-5,-5,NULL,'2085-01-01');
INSERT INTO patches(id,patchset_id,message_id,part_index,diff) OVERRIDING SYSTEM VALUE VALUES
(-1,-1,'part@test',1,'diff'),(-2,-5,'early@test',1,'diff'),(-3,-5,'late@test',2,'diff');
INSERT INTO patch_lineages(id,canonical_subject,first_sent_at,latest_sent_at) OVERRIDING SYSTEM VALUE VALUES
(-1,'stale',to_timestamp(1600000100),'2085-01-01'),
(-2,'cross',NULL,NULL),(-3,'unknown','1988-01-01','1988-01-01');
INSERT INTO patchset_lineage_state(
patchset_id,lineage_id,phase,revision,revision_explicit,is_resend,display_subject,
normalized_subject,author_email,match_source,match_confidence,matcher_version,manual_lock)
VALUES (-1,-1,'PATCH',1,true,false,'older','older','test@example.com','manual',100,1,true),
(-2,-1,'PATCH',2,true,false,'kept','kept','test@example.com','manual',100,1,true),
(-3,-2,'PATCH',1,true,false,'cross','cross','test@example.com','singleton',100,1,false),
(-4,-3,'RFC',1,true,false,'unknown','unknown','test@example.com','singleton',100,1,false);
CREATE TABLE test_archive_times(archive_group text, blob text, received bigint);
INSERT INTO test_archive_times VALUES
('lkml','cover',1600000005),('lkml','part',1600000060),('lkml','reply',1600000125),
('lkml','other',1600000105),('lkml','cross-a',1600000060),('bpf','cross-b',1600000300),
('lkml','unknown',600000000),('lkml','early',1599999945),('lkml','late',1600000030),
('lkml','part',1600000090),('lkml','part',0);
DO $$ DECLARE rejected boolean := false; BEGIN
BEGIN
    PERFORM refresh_message_timestamps(ARRAY[-5]::bigint[]);
EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'Timestamp observations need backfill%' THEN RAISE; END IF;
    rejected := true;
END;
IF NOT rejected THEN RAISE EXCEPTION 'Unbackfilled cross-post evidence was ignored'; END IF;
END $$;
\copy (SELECT * FROM test_archive_times WHERE blob <> 'cross-b') TO 'archive-timestamps.csv' CSV
SQL

# A missing copy must fail even though the same message is covered on lkml.
if sql -v apply=true -f "$root/deploy/fix-malformed-message-timestamps.sql" > incomplete.log 2>&1; then
    echo 'ERROR: incomplete cross-post export was accepted' >&2
    exit 1
fi
grep -q 'Incomplete archive export' incomplete.log
sql <<'SQL'
DO $$ BEGIN
IF EXISTS (SELECT 1 FROM messages_mailing_lists WHERE timestamp_recorded) THEN
    RAISE EXCEPTION 'Incomplete export wrote observations'; END IF;
IF (SELECT sent_at FROM messages WHERE id=-2) <> timestamptz '2085-01-01' THEN
    RAISE EXCEPTION 'Incomplete export changed messages'; END IF;
END $$;
\copy test_archive_times TO 'archive-timestamps.csv' CSV
SQL

sql -f "$root/deploy/fix-malformed-message-timestamps.sql" > preview.log
sql <<'SQL'
DO $$ BEGIN
IF EXISTS (SELECT 1 FROM messages_mailing_lists WHERE timestamp_recorded) THEN
    RAISE EXCEPTION 'Preview wrote observations'; END IF;
IF (SELECT sent_at FROM messages WHERE id=-2) <> timestamptz '2085-01-01' THEN
    RAISE EXCEPTION 'Preview changed messages'; END IF;
END $$;
SQL
sql -v apply=true -f "$root/deploy/fix-malformed-message-timestamps.sql" > apply.log
sql <<'SQL'
DO $$ BEGIN
IF EXISTS (SELECT 1 FROM messages_mailing_lists WHERE NOT timestamp_recorded) THEN
    RAISE EXCEPTION 'Missing backfill'; END IF;
IF (SELECT sent_at FROM messages WHERE id=-5) IS DISTINCT FROM to_timestamp(1600000060) THEN
    RAISE EXCEPTION 'Cross-post did not choose earlier observation'; END IF;
IF (SELECT effective_sent_at FROM messages_mailing_lists WHERE message_id=-5 AND mailing_list_id=2)
    IS DISTINCT FROM to_timestamp(1600000300) THEN
    RAISE EXCEPTION 'Lost later per-list observation'; END IF;
IF (SELECT last_updated_at FROM threads WHERE id=-1) IS DISTINCT FROM to_timestamp(1600000120) THEN
    RAISE EXCEPTION 'Unaffected reply must determine thread activity'; END IF;
IF (SELECT sent_at FROM patchsets WHERE id=-1) IS DISTINCT FROM to_timestamp(1600000000) THEN
    RAISE EXCEPTION 'Unaffected cover must determine patchset date'; END IF;
IF (SELECT sent_at FROM patchsets WHERE id=-5) IS DISTINCT FROM to_timestamp(1599999940) THEN
    RAISE EXCEPTION 'Unaffected early part must determine patchset date'; END IF;
IF NOT EXISTS (SELECT 1 FROM patch_lineages WHERE id=-1 AND canonical_subject='kept'
    AND first_sent_at=to_timestamp(1600000000) AND latest_sent_at=to_timestamp(1600000100)) THEN
    RAISE EXCEPTION 'Lineage summary omitted unaffected member'; END IF;
IF EXISTS (SELECT 1 FROM patchset_lineage_state WHERE patchset_id IN (-1,-2)
    AND (lineage_id <> -1 OR NOT manual_lock OR match_source <> 'manual')) THEN
    RAISE EXCEPTION 'Changed manual lineage membership'; END IF;
IF (SELECT sent_at FROM messages WHERE id=-6) IS NOT NULL
    OR (SELECT sent_at FROM patchsets WHERE id=-4) IS NOT NULL
    OR (SELECT latest_sent_at FROM patch_lineages WHERE id=-3) IS NOT NULL
    OR (SELECT first_sent_at FROM patch_lineages WHERE id=-3) IS NOT NULL THEN
    RAISE EXCEPTION 'Unknown message/patchset/lineage dates must remain NULL'; END IF;
IF (SELECT last_updated_at FROM threads WHERE id=-4) IS DISTINCT FROM 'epoch'::timestamptz THEN
    RAISE EXCEPTION 'Unknown thread must sort last'; END IF;
IF (SELECT count(*) FROM thread_search_documents) <> 5 THEN
    RAISE EXCEPTION 'Search documents lost'; END IF;
END $$;
-- Include audit columns to detect needless writes on rerun, not just dates.
CREATE VIEW test_snapshot AS
SELECT 'messages' AS kind, row_to_json(m)::text AS value FROM messages m UNION ALL
SELECT 'links', row_to_json(l)::text FROM messages_mailing_lists l UNION ALL
SELECT 'threads', row_to_json(t)::text FROM threads t UNION ALL
SELECT 'patchsets', row_to_json(p)::text FROM patchsets p UNION ALL
SELECT 'lineages', row_to_json(l)::text FROM patch_lineages l;
SQL
before=$(sql -Atc "SELECT md5(string_agg(value, ',' ORDER BY kind,value)) FROM test_snapshot")
sql -v apply=true -f "$root/deploy/fix-malformed-message-timestamps.sql" > rerun.log
after=$(sql -Atc "SELECT md5(string_agg(value, ',' ORDER BY kind,value)) FROM test_snapshot")
test "$before" = "$after"

sql <<'SQL'
-- Model replay of the later copy through the shared ingestion functions.
UPDATE messages SET sent_at=to_timestamp(1600000300) WHERE id=-5;
SELECT refresh_message_timestamps(ARRAY[-5]::bigint[]);
SELECT refresh_patchset_timestamps(ARRAY[-3]::bigint[]);
DO $$ BEGIN
IF (SELECT latest_sent_at FROM patch_lineages WHERE id=-2) IS DISTINCT FROM to_timestamp(1600000060) THEN
    RAISE EXCEPTION 'Replay undid the repair'; END IF;
END $$;
-- Removing the earliest copy exposes the later copy's original observation.
DELETE FROM messages_mailing_lists WHERE message_id=-5 AND mailing_list_id=1;
SELECT refresh_message_timestamps(ARRAY[-5]::bigint[]);
SELECT refresh_patchset_timestamps(ARRAY[-3]::bigint[]);
DO $$ BEGIN
IF (SELECT latest_sent_at FROM patch_lineages WHERE id=-2) IS DISTINCT FROM to_timestamp(1600000300) THEN
    RAISE EXCEPTION 'Deleting early copy did not expose later evidence'; END IF;
END $$;
SQL
echo 'Timestamp SQL regression checks passed.'
