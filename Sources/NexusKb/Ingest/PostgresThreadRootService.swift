import PostgresNIO
import Vapor

struct PostgresThreadRootService: Sendable {
    let client: PostgresClient

    func finalizeEligibleRoots(
        threadIDs: [Int64]? = nil,
        mailingListIDs: [Int64]? = nil,
        logger: Logger
    ) async throws {
        if threadIDs?.isEmpty == true {
            return
        }
        if mailingListIDs?.isEmpty == true {
            return
        }

        try await client.withTransaction(
            logger: logger
        ) { connection in
            try await reconcilePromotions(
                threadIDs: threadIDs,
                connection: connection,
                logger: logger
            )

            let includesAllThreads = threadIDs == nil
            let scopedThreadIDs = threadIDs ?? []
            let includesAllMailingLists = mailingListIDs == nil
            let scopedMailingListIDs = mailingListIDs ?? []

            let rows = try await connection.query(
                """
                WITH RECURSIVE candidates AS MATERIALIZED (
                    SELECT
                        thread.id AS thread_id,
                        root.message_id AS missing_root_message_id,
                        child.message_id AS promoted_root_message_id,
                        child.subject AS promoted_subject
                    FROM threads AS thread
                    JOIN messages AS root
                      ON root.thread_id = thread.id
                     AND root.message_id = thread.root_message_id
                     AND root.is_placeholder
                    JOIN LATERAL (
                        SELECT direct.*
                        FROM messages AS direct
                        WHERE direct.thread_id = thread.id
                          AND direct.in_reply_to = root.message_id
                        ORDER BY direct.id
                        LIMIT 2
                    ) AS child ON true
                    WHERE thread.promoted_from_message_id IS NULL
                      AND (
                            \(includesAllThreads)
                            OR thread.id = ANY(
                                \(scopedThreadIDs)::bigint[]
                            )
                          )
                      AND (
                            \(includesAllMailingLists)
                            OR (
                                EXISTS (
                                    SELECT 1
                                    FROM messages AS scoped_message
                                    JOIN messages_mailing_lists AS scoped_link
                                      ON scoped_link.message_id = scoped_message.id
                                    WHERE scoped_message.thread_id = thread.id
                                      AND scoped_link.mailing_list_id = ANY(
                                            \(scopedMailingListIDs)::bigint[]
                                          )
                                )
                                AND NOT EXISTS (
                                    SELECT 1
                                    FROM messages AS unscoped_message
                                    JOIN messages_mailing_lists AS unscoped_link
                                      ON unscoped_link.message_id = unscoped_message.id
                                    WHERE unscoped_message.thread_id = thread.id
                                      AND NOT (
                                            unscoped_link.mailing_list_id = ANY(
                                                \(scopedMailingListIDs)::bigint[]
                                            )
                                          )
                                )
                            )
                          )
                      AND NOT child.is_placeholder
                      AND (
                            SELECT count(*)
                            FROM messages AS direct_count
                            WHERE direct_count.thread_id = thread.id
                              AND direct_count.in_reply_to = root.message_id
                          ) = 1
                ),
                reachable AS (
                    SELECT
                        candidate.thread_id,
                        candidate.promoted_root_message_id AS message_id
                    FROM candidates AS candidate

                    UNION

                    SELECT
                        reachable.thread_id,
                        child.message_id
                    FROM reachable
                    JOIN messages AS child
                      ON child.thread_id = reachable.thread_id
                     AND child.in_reply_to = reachable.message_id
                ),
                eligible AS (
                    SELECT candidate.*
                    FROM candidates AS candidate
                    WHERE (
                            SELECT count(*)
                            FROM messages AS member
                            WHERE member.thread_id = candidate.thread_id
                          ) = 1 + (
                            SELECT count(*)
                            FROM reachable
                            WHERE reachable.thread_id = candidate.thread_id
                          )
                )
                UPDATE threads AS thread
                SET
                    root_message_id = eligible.promoted_root_message_id,
                    promoted_from_message_id = eligible.missing_root_message_id,
                    subject = eligible.promoted_subject
                FROM eligible
                WHERE thread.id = eligible.thread_id
                """,
                logger: logger
            )

            for try await _ in rows {}
        }
    }

    func reconcilePromotions(
        threadIDs: [Int64]? = nil,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        if threadIDs?.isEmpty == true {
            return
        }

        let includesAllThreads = threadIDs == nil
        let scopedThreadIDs = threadIDs ?? []

        let rows = try await connection.query(
            """
            WITH RECURSIVE promoted AS MATERIALIZED (
                SELECT
                    thread.id AS thread_id,
                    thread.root_message_id AS promoted_root_message_id,
                    thread.promoted_from_message_id AS missing_root_message_id
                FROM threads AS thread
                WHERE thread.promoted_from_message_id IS NOT NULL
                  AND (
                        \(includesAllThreads)
                        OR thread.id = ANY(
                            \(scopedThreadIDs)::bigint[]
                        )
                      )
                FOR UPDATE
            ),
            reachable AS (
                SELECT
                    promoted.thread_id,
                    promoted.promoted_root_message_id AS message_id
                FROM promoted

                UNION

                SELECT
                    reachable.thread_id,
                    child.message_id
                FROM reachable
                JOIN messages AS child
                  ON child.thread_id = reachable.thread_id
                 AND child.in_reply_to = reachable.message_id
            ),
            invalid AS (
                SELECT promoted.*
                FROM promoted
                LEFT JOIN messages AS missing_root
                  ON missing_root.thread_id = promoted.thread_id
                 AND missing_root.message_id = promoted.missing_root_message_id
                LEFT JOIN messages AS promoted_root
                  ON promoted_root.thread_id = promoted.thread_id
                 AND promoted_root.message_id = promoted.promoted_root_message_id
                WHERE missing_root.id IS NULL
                   OR NOT missing_root.is_placeholder
                   OR promoted_root.id IS NULL
                   OR promoted_root.is_placeholder
                   OR promoted_root.in_reply_to IS DISTINCT FROM
                        promoted.missing_root_message_id
                   OR (
                        SELECT count(*)
                        FROM messages AS direct_child
                        WHERE direct_child.thread_id = promoted.thread_id
                          AND direct_child.in_reply_to =
                                promoted.missing_root_message_id
                      ) <> 1
                   OR (
                        SELECT count(*)
                        FROM messages AS member
                        WHERE member.thread_id = promoted.thread_id
                      ) <> 1 + (
                        SELECT count(*)
                        FROM reachable
                        WHERE reachable.thread_id = promoted.thread_id
                      )
            )
            UPDATE threads AS thread
            SET
                root_message_id = invalid.missing_root_message_id,
                promoted_from_message_id = NULL,
                subject = COALESCE(
                    (
                        SELECT message.subject
                        FROM messages AS message
                        WHERE message.thread_id = thread.id
                          AND message.message_id =
                                invalid.missing_root_message_id
                          AND NOT message.is_placeholder
                    ),
                    (
                        SELECT message.subject
                        FROM messages AS message
                        WHERE message.thread_id = thread.id
                          AND NOT message.is_placeholder
                        ORDER BY
                            COALESCE(message.sent_at, message.created_at),
                            message.id
                        LIMIT 1
                    ),
                    '(placeholder)'
                )
            FROM invalid
            WHERE thread.id = invalid.thread_id
            """,
            logger: logger
        )

        for try await _ in rows {}
    }
}
