#!/usr/bin/env bash
# Uses PG* credentials; all writes are confined to a disposable database.
set -euo pipefail
root=$(realpath "$(dirname "$0")/..")
database="nexus_remove_git_test_$$"
createdb --maintenance-db=postgres "$database"
export PGDATABASE="$database"
trap 'dropdb --maintenance-db=postgres "$database"' EXIT
sql() { psql -X -v ON_ERROR_STOP=1 "$@"; }
for path in "$root"/Sources/NexusKb/Migrations/*.sql; do
    [[ ${path##*/} == 0021_* ]] || sql -f "$path" >/dev/null
done
sql <<'SQL'
INSERT INTO threads(id,root_message_id,subject,last_updated_at) OVERRIDING SYSTEM VALUE VALUES
(101,'git@test','git-only','2026-09-29'),
(102,'parent@test','git parent','2026-09-29'),
(103,'cross@test','cross-post','2026-09-20'),
(104,'linux@test','linux only','2026-09-25');
INSERT INTO messages(id,message_id,thread_id,subject,body,sent_at,in_reply_to) OVERRIDING SYSTEM VALUE VALUES
(101,'git@test',101,'git-only','removed content','2026-09-29',NULL),
(102,'parent@test',102,'git parent','removed parent','2026-09-29',NULL),
(103,'reply@test',102,'Linux reply','retained reply','2026-09-24','parent@test'),
(104,'cross@test',103,'cross-post','retained cross-post','2026-09-20',NULL),
(105,'linux@test',104,'linux only','untouched Linux','2026-09-25',NULL),
(106,'part@test',103,'git-only part','removed diff','2026-09-29','cross@test');
INSERT INTO messages_mailing_lists(message_id,mailing_list_id,effective_sent_at,timestamp_recorded)
SELECT m.id,l.id, CASE WHEN l.archive_group='git' THEN '2026-09-20'::timestamptz ELSE m.sent_at + interval '1 day' END,true
FROM messages m CROSS JOIN mailing_lists l
WHERE (l.archive_group='git' AND m.id IN (101,102,104,106))
   OR (l.archive_group='lkml' AND m.id IN (103,104,105));
INSERT INTO patchsets(id,thread_id,cover_letter_message_id,subject,total_parts,received_parts,status,sent_at)
OVERRIDING SYSTEM VALUE VALUES
(101,101,NULL,'git-only',1,1,'Complete','2026-09-29'),
(102,102,'parent@test','git parent',1,0,'Incomplete','2026-09-29'),
(103,103,'cross@test','cross-post',2,2,'Complete','2026-09-20');
INSERT INTO patches(id,patchset_id,message_id,part_index,diff) OVERRIDING SYSTEM VALUE VALUES
(101,101,'git@test',1,'deleted'),(103,103,'cross@test',1,'retained'),(106,103,'part@test',2,'deleted');
INSERT INTO mainline_patch_fingerprints(patch_id,matcher_version,stable_patch_id,content_fingerprint)
VALUES (101,1,'removed','a'),(103,1,'retained','b'),(106,1,'removed','c');
INSERT INTO patch_lineages(id,canonical_subject) OVERRIDING SYSTEM VALUE VALUES (101,'git-only'),(103,'cross-post');
INSERT INTO patchset_lineage_state(patchset_id,lineage_id,phase,revision,revision_explicit,is_resend,
display_subject,normalized_subject,author_email,match_source,match_confidence,matcher_version)
VALUES (101,101,'PATCH',1,false,false,'git-only','git-only','a@test','singleton',0,1),
(103,103,'PATCH',1,false,false,'cross-post','cross-post','a@test','singleton',0,1);
INSERT INTO maintenance_runs(id,kind,trigger,state) VALUES ('00000000-0000-0000-0000-000000000001','grokmirror','grokmirror','succeeded');
INSERT INTO maintenance_run_stages(id,run_id,mailing_list_id,position,operation,mode,state)
SELECT CASE archive_group WHEN 'git' THEN '00000000-0000-0000-0000-000000000002'::uuid
ELSE '00000000-0000-0000-0000-000000000003'::uuid END,
'00000000-0000-0000-0000-000000000001', id, id::integer,'ingest','incremental','succeeded'
FROM mailing_lists WHERE archive_group IN ('git','lkml');
-- A missing root with two retained branches must not pick one as the root.
INSERT INTO threads(id,root_message_id,last_updated_at) OVERRIDING SYSTEM VALUE
VALUES (201,'branches@test',now());
INSERT INTO messages(id,message_id,thread_id,in_reply_to,subject,body) OVERRIDING SYSTEM VALUE VALUES
(201,'branches@test',201,NULL,'removed parent','removed parent'),
(202,'left@test',201,'branches@test','left','retained left'),
(203,'right@test',201,'branches@test','right','retained right');
INSERT INTO messages_mailing_lists(message_id,mailing_list_id)
SELECT m.id,l.id FROM messages m CROSS JOIN mailing_lists l
WHERE (m.id=201 AND l.archive_group='git') OR (m.id IN (202,203) AND l.archive_group='lkml');
-- Exercise both sides of the 500-thread deletion boundary.
INSERT INTO threads(id,root_message_id,last_updated_at) OVERRIDING SYSTEM VALUE
SELECT id, 'batch-' || id || '@test', now() FROM generate_series(1000,1500) id;
SELECT setval(pg_get_serial_sequence('messages','id'), (SELECT max(id) FROM messages));
INSERT INTO messages(message_id,thread_id,body)
SELECT root_message_id,id,'batch content' FROM threads WHERE id BETWEEN 1000 AND 1500;
INSERT INTO messages_mailing_lists(message_id,mailing_list_id)
SELECT m.id,l.id FROM messages m CROSS JOIN mailing_lists l
WHERE m.thread_id BETWEEN 1000 AND 1500 AND l.archive_group='git';
UPDATE maintenance_runs SET state='running';
SQL
if sql -f "$root/Sources/NexusKb/Migrations/0021_remove_git_mailing_list.sql"; then
    echo 'ERROR: removal accepted active maintenance' >&2
    exit 1
fi
sql <<'SQL'
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM mailing_lists WHERE archive_group='git') THEN
    RAISE EXCEPTION 'Failed migration was not atomic';
END IF;
END $$;
UPDATE maintenance_runs SET state='succeeded';
SQL
for attempt in 1 2; do
    sql -f "$root/Sources/NexusKb/Migrations/0021_remove_git_mailing_list.sql"
    sql <<'SQL'
DO $$ BEGIN
IF EXISTS (SELECT 1 FROM mailing_lists WHERE archive_group='git') THEN RAISE EXCEPTION 'Git still registered'; END IF;
IF EXISTS (SELECT 1 FROM threads WHERE id=101) OR EXISTS (SELECT 1 FROM messages WHERE id=101)
OR EXISTS (SELECT 1 FROM patchsets WHERE id IN (101,102)) OR EXISTS (SELECT 1 FROM patch_lineages WHERE id=101)
OR EXISTS (SELECT 1 FROM mainline_patch_fingerprints WHERE patch_id IN (101,106))
OR EXISTS (SELECT 1 FROM thread_search_documents WHERE thread_id=101)
THEN RAISE EXCEPTION 'Git-only derived data survived'; END IF;
IF NOT EXISTS (SELECT 1 FROM messages WHERE id=102 AND is_placeholder AND body='' AND author IS NULL)
OR NOT EXISTS (SELECT 1 FROM messages WHERE id=103 AND body='retained reply' AND in_reply_to='parent@test')
THEN RAISE EXCEPTION 'Mixed thread topology was lost'; END IF;
IF NOT EXISTS (SELECT 1 FROM messages WHERE id=104 AND NOT is_placeholder AND body='retained cross-post' AND sent_at='2026-09-21')
OR NOT EXISTS (SELECT 1 FROM mainline_patch_fingerprints WHERE patch_id=103 AND stable_patch_id='retained')
OR NOT EXISTS (SELECT 1 FROM patchsets WHERE id=103 AND received_parts=1 AND status='Incomplete' AND sent_at='2026-09-21')
THEN RAISE EXCEPTION 'Cross-post or partial patchset damaged'; END IF;
IF NOT EXISTS (SELECT 1 FROM messages WHERE id=105 AND body='untouched Linux' AND sent_at='2026-09-25')
OR NOT EXISTS (SELECT 1 FROM threads WHERE id=102 AND subject='Linux reply' AND last_updated_at='2026-09-24')
THEN RAISE EXCEPTION 'Linux content or thread metadata wrong'; END IF;
IF EXISTS (SELECT 1 FROM thread_search_documents WHERE content LIKE '%removed%') THEN RAISE EXCEPTION 'Stale search content'; END IF;
IF NOT EXISTS (SELECT 1 FROM thread_search_documents WHERE thread_id=102 AND content LIKE '%retained reply%')
THEN RAISE EXCEPTION 'Retained reply disappeared from search'; END IF;
IF (SELECT count(*) FROM maintenance_run_stages) <> 1 THEN RAISE EXCEPTION 'Wrong maintenance stages removed'; END IF;
IF (SELECT count(*) FROM threads) <> 4 THEN RAISE EXCEPTION 'Batch boundary left Git threads behind'; END IF;
IF NOT EXISTS (SELECT 1 FROM threads WHERE id=201 AND root_message_id='branches@test' AND promoted_from_message_id IS NULL)
THEN RAISE EXCEPTION 'Ambiguous branch was promoted'; END IF;
END $$;
SQL
done
echo 'Git removal, cross-post preservation, derived cleanup, and replay passed.'
