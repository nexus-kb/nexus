//
//  PostgresIngestService.swift
//  NexusKb
//
//  Created by Tanuj Ravi Rao on 8/13/26.
//

import Foundation
import PostgresNIO
import Vapor

enum PostgresIngestError:
    Error,
    Sendable,
    Equatable
{
    case missingEpoch(
        mailingListID: Int64,
        epoch: Int32
    )

    case cursorMismatch(
        expected: String?,
        actual: String?
    )

    case missingThread(String)
    case missingMessage(String)
    case missingMaintenanceStage(UUID)
}

struct PostgresIngestResult:
    Sendable,
    Equatable
{
    let messageID: Int64
    let threadID: Int64
    let wasAlreadyIngested: Bool
}

struct PreparedPublicInboxMessage:
    Sendable
{
    let commitOID: String
    let blobOID: String
    let parsed: ParsedIngestMessage
}

enum PreparedPublicInboxArchiveEntry:
    Sendable
{
    case message(PreparedPublicInboxMessage)
    case deletion(
        commitOID: String,
        blobOID: String
    )
    case skipped(
        commitOID: String,
        blobOID: String
    )

    var commitOID: String {
        switch self {
        case .message(let message):
            message.commitOID
        case .deletion(let commitOID, _):
            commitOID
        case .skipped(let commitOID, _):
            commitOID
        }
    }
}

struct PostgresIngestService: Sendable {

    private struct NormalizedPublicInboxMessage:
        Sendable
    {
        let commitOID: String
        let blobOID: String
        let parsed: ParsedIngestMessage
    }

    private struct MessageState: Sendable {
        let id: Int64
        var threadID: Int64
    }

    private struct ThreadMetadataInput:
        Sendable
    {
        var threadID: Int64
        let rootCandidateMessageID: String
        let subject: String
        let timestamp: Date
    }

    private struct BatchPersistenceState:
        Sendable
    {
        var messagesByMessageID: [String: MessageState] = [:]

        var mailingListBlobOIDByMessageDatabaseID:
            [Int64: String] = [:]

        var threadMetadataInputs:
            [ThreadMetadataInput] = []

        var recipientsByMessageDatabaseID: [Int64: [ResolvedBatchRecipient]] = [:]

        var affectedPatchSetIDs: Set<Int64> = []

        mutating func remapThread(
            from sourceThreadID: Int64,
            to targetThreadID: Int64
        ) {
            for messageID in Array(
                messagesByMessageID.keys
            ) {
                guard
                    var state =
                        messagesByMessageID[
                            messageID
                        ],
                    state.threadID
                        == sourceThreadID
                else {
                    continue
                }

                state.threadID =
                    targetThreadID

                messagesByMessageID[
                    messageID
                ] = state
            }

            for index
                in threadMetadataInputs.indices
            {
                guard
                    threadMetadataInputs[
                        index
                    ].threadID == sourceThreadID
                else {
                    continue
                }

                threadMetadataInputs[
                    index
                ].threadID = targetThreadID
            }
        }
    }

    let client: PostgresClient

    func archiveCursor(
        mailingListID: Int64,
        epoch: Int32,
        logger: Logger
    ) async throws -> String? {
        try await client.withTransaction(
            logger: logger
        ) { connection in
            try await ensureEpoch(
                mailingListID: mailingListID,
                epoch: epoch,
                connection: connection,
                logger: logger
            )

            return try await lockedCursor(
                mailingListID: mailingListID,
                epoch: epoch,
                connection: connection,
                logger: logger
            )
        }
    }

    func ingestBatch(
        _ messages:
            [PreparedPublicInboxMessage],
        mailingListID: Int64,
        epoch: Int32,
        expectedPreviousCommitOID: String?,
        maintenanceStageID: UUID? = nil,
        logger: Logger
    ) async throws -> [PostgresIngestResult] {
        try await ingestBatch(
            messages.map {
                .message($0)
            },
            mailingListID: mailingListID,
            epoch: epoch,
            expectedPreviousCommitOID:
                expectedPreviousCommitOID,
            maintenanceStageID: maintenanceStageID,
            logger: logger
        )
    }

    func ingestBatch(
        _ entries:
            [PreparedPublicInboxArchiveEntry],
        mailingListID: Int64,
        epoch: Int32,
        expectedPreviousCommitOID: String?,
        maintenanceStageID: UUID? = nil,
        logger: Logger
    ) async throws -> [PostgresIngestResult] {
        guard let finalEntry = entries.last
        else {
            return []
        }

        let messages = entries.compactMap {
            entry -> PreparedPublicInboxMessage? in

            guard case .message(let message) = entry
            else {
                return nil
            }

            return message
        }

        var finalDeletionBlobOIDs: Set<String> = []

        for entry in entries {
            switch entry {
            case .message(let message):
                finalDeletionBlobOIDs.remove(
                    message.blobOID
                )
            case .deletion(_, let blobOID):
                finalDeletionBlobOIDs.insert(blobOID)
            case .skipped:
                break
            }
        }

        let normalized = remappingThreadIdentifiers(
            in: messages.map {
                NormalizedPublicInboxMessage(
                    commitOID: $0.commitOID,
                    blobOID: $0.blobOID,
                    parsed:
                        PostgresTextNormalizer
                        .normalize($0.parsed)
                )
            }
        )

        return try await client.withTransaction(
            logger: logger
        ) { connection in
            try await ensureEpoch(
                mailingListID: mailingListID,
                epoch: epoch,
                connection: connection,
                logger: logger
            )

            let currentCursor =
                try await lockedCursor(
                    mailingListID:
                        mailingListID,
                    epoch: epoch,
                    connection: connection,
                    logger: logger
                )

            guard
                currentCursor
                    == expectedPreviousCommitOID
            else {
                throw
                    PostgresIngestError
                    .cursorMismatch(
                        expected:
                            expectedPreviousCommitOID,
                        actual: currentCursor
                    )
            }

            let recipientService =
                PostgresRecipientBatchService()

            let resolvedRecipients =
                try await recipientService
                .resolvePeople(
                    messages:
                        normalized.map(\.parsed),
                    connection: connection,
                    logger: logger
                )

            var batchState =
                BatchPersistenceState()

            var results: [PostgresIngestResult] = []
            results.reserveCapacity(
                normalized.count
            )

            for (
                messageIndex,
                message
            ) in normalized.enumerated() {
                let persisted: (
                    id: Int64,
                    threadID: Int64
                )
                do {
                    persisted = try await persistMessage(
                        blobOID:
                            message.blobOID,
                        parsed:
                            message.parsed,
                        batchState:
                            &batchState,
                        connection: connection,
                        logger: logger
                    )
                } catch let error as PostgresPatchIngestError {
                    logger.error(
                        "Patch persistence failed",
                        metadata: [
                            "commit-oid": "\(message.commitOID)",
                            "blob-oid": "\(message.blobOID)",
                            "epoch": "\(epoch)",
                            "mailing-list-id": "\(mailingListID)",
                            "message-id": "\(message.parsed.message.messageID)",
                            "subject": "\(message.parsed.message.subject)",
                            "error": "\(String(reflecting: error))",
                        ]
                    )
                    throw error
                }

                batchState
                    .recipientsByMessageDatabaseID[
                        persisted.id
                    ] =
                    resolvedRecipients
                    .recipients(
                        forMessageAt:
                            messageIndex
                    )

                results.append(
                    PostgresIngestResult(
                        messageID: persisted.id,
                        threadID:
                            persisted.threadID,
                        wasAlreadyIngested:
                            false
                    )
                )
            }

            try await persistMailingListLinks(
                linksByMessageDatabaseID:
                    batchState
                    .mailingListBlobOIDByMessageDatabaseID,
                mailingListID: mailingListID,
                connection: connection,
                logger: logger
            )

            try await updateThreadMetadata(
                inputs:
                    batchState
                    .threadMetadataInputs,
                connection: connection,
                logger: logger
            )

            try await recipientService
                .replaceRecipients(
                    recipientsByMessageID:
                        batchState
                        .recipientsByMessageDatabaseID,
                    connection: connection,
                    logger: logger
                )

            let unlinkedMessageIDs =
                try await removeMailingListLinks(
                    blobOIDs: Array(
                        finalDeletionBlobOIDs
                    ).sorted(),
                    mailingListID: mailingListID,
                    connection: connection,
                    logger: logger
                )

            let deletionAffectedPatchSetIDs =
                try await cleanupOrphanedMessages(
                candidateMessageIDs:
                    unlinkedMessageIDs,
                connection: connection,
                logger: logger
            )

            try await PostgresThreadRootService(
                client: client
            ).reconcilePromotions(
                connection: connection,
                logger: logger
            )

            try await enqueuePatchLineageWork(
                patchSetIDs:
                    batchState.affectedPatchSetIDs
                    .union(deletionAffectedPatchSetIDs),
                mailingListID: mailingListID,
                connection: connection,
                logger: logger
            )

            try await advanceCursor(
                mailingListID: mailingListID,
                epoch: epoch,
                expectedPreviousCommitOID:
                    expectedPreviousCommitOID,
                commitOID:
                    finalEntry.commitOID,
                connection: connection,
                logger: logger
            )

            if let maintenanceStageID {
                try await execute(
                    """
                    UPDATE maintenance_run_stages
                    SET processed_items =
                            processed_items + \(entries.count),
                        current_epoch = \(epoch)
                    WHERE id = \(maintenanceStageID)
                      AND mailing_list_id = \(mailingListID)
                    """,
                    on: connection,
                    logger: logger
                )
            }

            return results
        }
    }

    private func remappingThreadIdentifiers(
        in messages:
            [NormalizedPublicInboxMessage]
    ) -> [NormalizedPublicInboxMessage] {
        let canonicalMessageIDs = Set(
            messages.map {
                $0.parsed.message.messageID
            }
        )

        var canonicalIDsByAlias:
            [String: Set<String>] = [:]

        for message in messages {
            let canonicalMessageID =
                message.parsed.message.messageID

            for alias
                in message.parsed.messageIDAliases
                where alias != canonicalMessageID
                    && !canonicalMessageIDs
                        .contains(alias)
            {
                canonicalIDsByAlias[
                    alias,
                    default: []
                ].insert(canonicalMessageID)
            }
        }

        let canonicalIDByAlias =
            canonicalIDsByAlias.compactMapValues {
                candidates -> String? in

                guard candidates.count == 1
                else {
                    return nil
                }

                return candidates.first
            }

        guard !canonicalIDByAlias.isEmpty
        else {
            return messages
        }

        return messages.map { message in
            NormalizedPublicInboxMessage(
                commitOID: message.commitOID,
                blobOID: message.blobOID,
                parsed: remappingThreadIdentifiers(
                    in: message.parsed,
                    using: canonicalIDByAlias
                )
            )
        }
    }

    private func remappingThreadIdentifiers(
        in parsed: ParsedIngestMessage,
        using canonicalIDByAlias:
            [String: String]
    ) -> ParsedIngestMessage {
        let message = parsed.message

        return ParsedIngestMessage(
            message: IngestMailMessage(
                messageID: message.messageID,
                subject: message.subject,
                from: message.from,
                to: message.to,
                cc: message.cc,
                date: message.date,
                inReplyTo: message.inReplyTo.map {
                    canonicalIDByAlias[$0] ?? $0
                },
                references: message.references.map {
                    canonicalIDByAlias[$0] ?? $0
                },
                textBody: message.textBody
            ),
            messageIDAliases:
                parsed.messageIDAliases,
            author: parsed.author,
            patch: parsed.patch
        )
    }

    private func ensureEpoch(
        mailingListID: Int64,
        epoch: Int32,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        try await execute(
            """
            INSERT INTO mailing_list_archive_epochs (
                mailing_list_id,
                epoch
            )
            VALUES (
                \(mailingListID),
                \(epoch)
            )
            ON CONFLICT (
                mailing_list_id,
                epoch
            ) DO NOTHING
            """,
            on: connection,
            logger: logger
        )
    }

    private func persistMessage(
        blobOID: String,
        parsed: ParsedIngestMessage,
        batchState:
            inout BatchPersistenceState,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> (
        id: Int64,
        threadID: Int64
    ) {
        let message = parsed.message

        let threadAnchorMessageID =
            message.inReplyTo
            ?? message.messageID

        let timestamp =
            message.date
            ?? Date(timeIntervalSince1970: 0)

        let targetThreadID =
            try await ensureThread(
                for: threadAnchorMessageID,
                timestamp: timestamp,
                batchState: &batchState,
                connection: connection,
                logger: logger
            )

        let rows = try await connection.query(
            """
            INSERT INTO messages (
                message_id,
                thread_id,
                in_reply_to,
                references_ids,
                author,
                subject,
                sent_at,
                body,
                to_recipients,
                cc_recipients,
                is_placeholder,
                updated_at
            )
            VALUES (
                \(message.messageID),
                \(targetThreadID),
                \(message.inReplyTo),
                \(message.references),
                \(parsed.authorDisplayString),
                \(message.subject),
                \(message.date),
                \(message.textBody),
                \(parsed.toDisplayString),
                \(parsed.ccDisplayString),
                false,
                now()
            )
            ON CONFLICT (message_id) DO UPDATE
            SET
                thread_id =
                    EXCLUDED.thread_id,
                in_reply_to =
                    EXCLUDED.in_reply_to,
                references_ids =
                    EXCLUDED.references_ids,
                author =
                    EXCLUDED.author,
                subject =
                    EXCLUDED.subject,
                sent_at =
                    EXCLUDED.sent_at,
                body =
                    EXCLUDED.body,
                to_recipients =
                    EXCLUDED.to_recipients,
                cc_recipients =
                    EXCLUDED.cc_recipients,
                is_placeholder = false,
                updated_at = now()
            RETURNING
                new.id,
                old.thread_id
            """,
            logger: logger
        )

        guard
            let upserted =
                try await firstMessageUpsert(rows)
        else {
            throw
                PostgresIngestError
                .missingMessage(
                    message.messageID
                )
        }

        let messageDatabaseID = upserted.0
        let previousThreadID = upserted.1

        if let previousThreadID,
            previousThreadID != targetThreadID
        {
            batchState.affectedPatchSetIDs.formUnion(
                try await patchSetIDs(
                    threadIDs: [
                        previousThreadID,
                        targetThreadID,
                    ],
                    connection: connection,
                    logger: logger
                )
            )

            try await mergeThread(
                previousThreadID,
                into: targetThreadID,
                connection: connection,
                logger: logger
            )

            batchState.remapThread(
                from: previousThreadID,
                to: targetThreadID
            )
        }

        batchState.messagesByMessageID[
            message.messageID
        ] = MessageState(
            id: messageDatabaseID,
            threadID: targetThreadID
        )

        let patchSetID = try await PostgresPatchIngestService()
            .persist(
                parsed: parsed,
                threadID: targetThreadID,
                timestamp: timestamp,
                connection: connection,
                logger: logger
            )

        if let patchSetID {
            batchState.affectedPatchSetIDs.insert(
                patchSetID
            )
        }

        batchState
            .mailingListBlobOIDByMessageDatabaseID[
                messageDatabaseID
            ] = blobOID

        batchState.threadMetadataInputs.append(
            ThreadMetadataInput(
                threadID: targetThreadID,
                rootCandidateMessageID:
                    message.messageID,
                subject: message.subject,
                timestamp: timestamp
            )
        )

        return (
            messageDatabaseID,
            targetThreadID
        )
    }

    func resetMailingList(
        mailingListID: Int64,
        stageID: UUID,
        logger: Logger
    ) async throws {
        try await client.withTransaction(
            logger: logger
        ) { connection in
            let stageRows = try await connection.query(
                """
                SELECT reset_completed
                FROM maintenance_run_stages
                WHERE id = \(stageID)
                  AND mailing_list_id = \(mailingListID)
                FOR UPDATE
                """,
                logger: logger
            )
            var resetCompleted: Bool?
            for try await row in stageRows {
                resetCompleted = try row.decode(Bool.self)
            }
            guard let resetCompleted else {
                throw PostgresIngestError
                    .missingMaintenanceStage(stageID)
            }
            guard !resetCompleted else { return }

            let rows = try await connection.query(
                """
                DELETE FROM messages_mailing_lists
                WHERE mailing_list_id = \(mailingListID)
                RETURNING message_id
                """,
                logger: logger
            )
            var messageIDs: [Int64] = []
            for try await row in rows {
                messageIDs.append(try row.decode(Int64.self))
            }

            _ = try await cleanupOrphanedMessages(
                candidateMessageIDs: messageIDs,
                connection: connection,
                logger: logger
            )

            try await execute(
                """
                DELETE FROM mailing_list_archive_epochs
                WHERE mailing_list_id = \(mailingListID)
                """,
                on: connection,
                logger: logger
            )
            try await execute(
                """
                DELETE FROM patch_lineage_work_items
                WHERE mailing_list_id = \(mailingListID)
                """,
                on: connection,
                logger: logger
            )
            try await execute(
                """
                UPDATE maintenance_run_stages
                SET reset_completed = true,
                    processed_items = 0,
                    current_epoch = NULL
                WHERE id = \(stageID)
                  AND mailing_list_id = \(mailingListID)
                """,
                on: connection,
                logger: logger
            )
        }
    }

    private func enqueuePatchLineageWork(
        patchSetIDs: Set<Int64>,
        mailingListID: Int64,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        guard !patchSetIDs.isEmpty else {
            return
        }

        try await execute(
            """
            INSERT INTO patch_lineage_work_items (
                mailing_list_id, patchset_id
            )
            SELECT \(mailingListID), patchset.id
            FROM patchsets AS patchset
            WHERE patchset.id = ANY(
                    \(patchSetIDs.sorted())::bigint[]
                  )
            ON CONFLICT (mailing_list_id, patchset_id)
            DO UPDATE SET queued_at = now()
            """,
            on: connection,
            logger: logger
        )
    }

    private func ensureThread(
        for messageID: String,
        timestamp: Date,
        batchState:
            inout BatchPersistenceState,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Int64 {
        if let cached =
            batchState
            .messagesByMessageID[
                messageID
            ]
        {
            return cached.threadID
        }

        if let existing =
            try await existingMessage(
                messageID: messageID,
                connection: connection,
                logger: logger
            )
        {
            batchState.messagesByMessageID[
                messageID
            ] = existing

            return existing.threadID
        }

        let threadRows = try await connection.query(
            """
            INSERT INTO threads (
                root_message_id,
                subject,
                last_updated_at
            )
            VALUES (
                \(messageID),
                NULL,
                \(timestamp)
            )
            ON CONFLICT (
                root_message_id
            ) DO UPDATE
            SET last_updated_at = GREATEST(
                threads.last_updated_at,
                EXCLUDED.last_updated_at
            )
            RETURNING id
            """,
            logger: logger
        )

        guard let threadID =
                try await firstInt64(threadRows)
        else {
            throw
                PostgresIngestError
                .missingThread(messageID)
        }

        let placeholderRows =
            try await connection.query(
                """
                INSERT INTO messages (
                    message_id,
                    thread_id,
                    subject,
                    sent_at,
                    is_placeholder
                )
                VALUES (
                    \(messageID),
                    \(threadID),
                    '(placeholder)',
                    \(timestamp),
                    true
                )
                ON CONFLICT (
                    message_id
                ) DO NOTHING
                RETURNING
                    id,
                    thread_id
                """,
                logger: logger
            )

        if let inserted =
            try await firstMessageState(
                placeholderRows
            )
        {
            batchState.messagesByMessageID[
                messageID
            ] = inserted

            return inserted.threadID
        }

        // Another transaction may have inserted
        // this Message-ID after the first lookup.
        guard
            let concurrentlyInserted =
                try await existingMessage(
                    messageID: messageID,
                    connection: connection,
                    logger: logger
                )
        else {
            throw
                PostgresIngestError
                .missingMessage(messageID)
        }

        batchState.messagesByMessageID[
            messageID
        ] = concurrentlyInserted

        return concurrentlyInserted.threadID
    }

    private func mergeThread(
         _ sourceThreadID: Int64,
         into targetThreadID: Int64,
         connection: PostgresConnection,
         logger: Logger
     ) async throws {
         try await execute(
             """
             UPDATE messages
             SET thread_id = \(targetThreadID)
             WHERE thread_id = \(sourceThreadID)
             """,
             on: connection,
             logger: logger
         )

         try await execute(
             """
             UPDATE patchsets
             SET
                 thread_id = \(targetThreadID),
                 updated_at = now()
             WHERE thread_id = \(sourceThreadID)
             """,
             on: connection,
             logger: logger
         )

         try await execute(
             """
             INSERT INTO threads_subsystems (
                 thread_id,
                 subsystem_id
             )
             SELECT
                 \(targetThreadID),
                 subsystem_id
             FROM threads_subsystems
             WHERE thread_id = \(sourceThreadID)
             ON CONFLICT DO NOTHING
             """,
             on: connection,
             logger: logger
         )

         try await execute(
             """
             DELETE FROM threads_subsystems
             WHERE thread_id = \(sourceThreadID)
             """,
             on: connection,
             logger: logger
         )

         try await execute(
             """
             DELETE FROM threads
             WHERE id = \(sourceThreadID)
             """,
             on: connection,
             logger: logger
         )
     }

    private func patchSetIDs(
        threadIDs: [Int64],
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Set<Int64> {
        let rows = try await connection.query(
            """
            SELECT id
            FROM patchsets
            WHERE thread_id = ANY(\(threadIDs)::bigint[])
            """,
            logger: logger
        )
        var values: Set<Int64> = []
        for try await row in rows {
            values.insert(try row.decode(Int64.self))
        }
        return values
    }

    private func persistMailingListLinks(
        linksByMessageDatabaseID:
            [Int64: String],
        mailingListID: Int64,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        let links =
            linksByMessageDatabaseID
            .sorted {
                $0.key < $1.key
            }

        guard !links.isEmpty else {
            return
        }

        let messageIDs = links.map {
            $0.key
        }

        let blobOIDs = links.map {
            $0.value
        }

        try await execute(
            """
            INSERT INTO messages_mailing_lists (
                message_id,
                mailing_list_id,
                archive_blob_oid
            )
            SELECT
                input.message_id,
                \(mailingListID),
                input.archive_blob_oid
            FROM unnest(
                \(messageIDs)::bigint[],
                \(blobOIDs)::text[]
            ) AS input(
                message_id,
                archive_blob_oid
            )
            ON CONFLICT (
                message_id,
                mailing_list_id
            ) DO UPDATE
            SET archive_blob_oid =
                EXCLUDED.archive_blob_oid
            """,
            on: connection,
            logger: logger
        )
    }

    private func removeMailingListLinks(
        blobOIDs: [String],
        mailingListID: Int64,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> [Int64] {
        guard !blobOIDs.isEmpty else {
            return []
        }

        let rows = try await connection.query(
            """
            DELETE FROM messages_mailing_lists
            WHERE mailing_list_id =
                    \(mailingListID)
              AND archive_blob_oid = ANY(
                    \(blobOIDs)::text[]
              )
            RETURNING message_id
            """,
            logger: logger
        )

        var messageIDs: [Int64] = []

        for try await row in rows {
            messageIDs.append(
                try row.decode(Int64.self)
            )
        }

        return messageIDs
    }

    private func cleanupOrphanedMessages(
        candidateMessageIDs: [Int64],
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Set<Int64> {
        guard !candidateMessageIDs.isEmpty else {
            return []
        }

        let orphanRows = try await connection.query(
            """
            SELECT
                message.id,
                message.message_id,
                message.thread_id
            FROM messages AS message
            WHERE message.id = ANY(
                    \(candidateMessageIDs)::bigint[]
                  )
              AND NOT EXISTS (
                    SELECT 1
                    FROM messages_mailing_lists AS link
                    WHERE link.message_id =
                            message.id
                  )
            FOR UPDATE
            """,
            logger: logger
        )

        var orphanIDs: [Int64] = []
        var orphanMessageIDs: [String] = []
        var affectedThreadIDs: Set<Int64> = []

        for try await row in orphanRows {
            let value = try row.decode(
                (Int64, String, Int64).self
            )

            orphanIDs.append(value.0)
            orphanMessageIDs.append(value.1)
            affectedThreadIDs.insert(value.2)
        }

        guard !orphanIDs.isEmpty else {
            return []
        }

        let patchSetRows = try await connection.query(
            """
            SELECT patchset.id
            FROM patchsets AS patchset
            WHERE patchset.cover_letter_message_id =
                    ANY(\(orphanMessageIDs)::text[])

            UNION

            SELECT patch.patchset_id
            FROM patches AS patch
            WHERE patch.message_id =
                    ANY(\(orphanMessageIDs)::text[])
            """,
            logger: logger
        )

        var affectedPatchSetIDs: [Int64] = []

        for try await row in patchSetRows {
            affectedPatchSetIDs.append(
                try row.decode(Int64.self)
            )
        }

        try await execute(
            """
            DELETE FROM messages_recipients
            WHERE message_id = ANY(
                    \(orphanIDs)::bigint[]
                  )
            """,
            on: connection,
            logger: logger
        )

        try await execute(
            """
            DELETE FROM messages_subsystems
            WHERE message_id = ANY(
                    \(orphanIDs)::bigint[]
                  )
            """,
            on: connection,
            logger: logger
        )

        try await execute(
            """
            DELETE FROM patches
            WHERE message_id = ANY(
                    \(orphanMessageIDs)::text[]
                  )
            """,
            on: connection,
            logger: logger
        )

        try await execute(
            """
            UPDATE patchsets
            SET
                cover_letter_message_id = NULL,
                updated_at = now()
            WHERE cover_letter_message_id = ANY(
                    \(orphanMessageIDs)::text[]
                  )
            """,
            on: connection,
            logger: logger
        )

        let threadIDs = Array(
            affectedThreadIDs
        ).sorted()

        try await execute(
            """
            DELETE FROM threads AS thread
            WHERE thread.id = ANY(
                    \(threadIDs)::bigint[]
                  )
              AND NOT EXISTS (
                    SELECT 1
                    FROM messages AS message
                    WHERE message.thread_id =
                            thread.id
                      AND NOT (
                            message.id = ANY(
                                \(orphanIDs)::bigint[]
                            )
                          )
                  )
            """,
            on: connection,
            logger: logger
        )

        try await execute(
            """
            UPDATE messages
            SET
                in_reply_to = NULL,
                references_ids = ARRAY[]::text[],
                author = NULL,
                subject = '(placeholder)',
                sent_at = NULL,
                body = '',
                to_recipients = '',
                cc_recipients = '',
                is_placeholder = true,
                updated_at = now()
            WHERE id = ANY(
                    \(orphanIDs)::bigint[]
                  )
            """,
            on: connection,
            logger: logger
        )

        if !affectedPatchSetIDs.isEmpty {
            try await execute(
                """
                DELETE FROM patchsets AS patchset
                WHERE patchset.id = ANY(
                        \(affectedPatchSetIDs)::bigint[]
                      )
                  AND patchset.cover_letter_message_id
                        IS NULL
                  AND NOT EXISTS (
                        SELECT 1
                        FROM patches AS patch
                        WHERE patch.patchset_id =
                                patchset.id
                      )
                """,
                on: connection,
                logger: logger
            )

            try await execute(
                """
                UPDATE patchsets AS patchset
                SET
                    received_parts = (
                        SELECT count(*)::integer
                        FROM patches AS patch
                        WHERE patch.patchset_id =
                                patchset.id
                    ),
                    status = CASE
                        WHEN patchset.status =
                                'Malformed'
                        THEN patchset.status
                        WHEN (
                            SELECT count(*)
                            FROM patches AS patch
                            WHERE patch.patchset_id =
                                    patchset.id
                        ) >= patchset.total_parts
                        THEN 'Complete'
                        ELSE 'Incomplete'
                    END,
                    updated_at = now()
                WHERE patchset.id = ANY(
                        \(affectedPatchSetIDs)::bigint[]
                      )
                """,
                on: connection,
                logger: logger
            )
        }

        try await execute(
            """
            UPDATE threads AS thread
            SET
                subject = metadata.subject,
                last_updated_at =
                    metadata.last_updated_at
            FROM (
                SELECT
                    target.id,
                    COALESCE(
                        (
                            SELECT message.subject
                            FROM messages AS message
                            WHERE message.thread_id =
                                    target.id
                              AND NOT message.is_placeholder
                            ORDER BY
                                CASE
                                    WHEN message.message_id =
                                            target.root_message_id
                                    THEN 0
                                    ELSE 1
                                END,
                                COALESCE(
                                    message.sent_at,
                                    message.created_at
                                ),
                                message.id
                            LIMIT 1
                        ),
                        '(placeholder)'
                    ) AS subject,
                    COALESCE(
                        (
                            SELECT max(
                                COALESCE(
                                    message.sent_at,
                                    message.created_at
                                )
                            )
                            FROM messages AS message
                            WHERE message.thread_id =
                                    target.id
                              AND NOT message.is_placeholder
                        ),
                        target.created_at
                    ) AS last_updated_at
                FROM threads AS target
                WHERE target.id = ANY(
                        \(threadIDs)::bigint[]
                      )
            ) AS metadata
            WHERE thread.id = metadata.id
            """,
            on: connection,
            logger: logger
        )

        return Set(affectedPatchSetIDs)
    }

    private func updateThreadMetadata(
        inputs: [ThreadMetadataInput],
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        guard !inputs.isEmpty else {
            return
        }

        let threadIDs = inputs.map {
            $0.threadID
        }

        let rootCandidateMessageIDs =
            inputs.map {
                $0.rootCandidateMessageID
            }

        let subjects = inputs.map {
            $0.subject
        }

        let timestamps = inputs.map {
            $0.timestamp
        }

        try await execute(
            """
            WITH metadata_input AS (
                SELECT
                    input.thread_id,
                    input.root_candidate_message_id,
                    input.subject,
                    input.message_timestamp,
                    input.ordinality
                FROM unnest(
                    \(threadIDs)::bigint[],
                    \(rootCandidateMessageIDs)::text[],
                    \(subjects)::text[],
                    \(timestamps)::timestamptz[]
                ) WITH ORDINALITY
                  AS input(
                      thread_id,
                      root_candidate_message_id,
                      subject,
                      message_timestamp,
                      ordinality
                  )
            ),
            metadata_by_thread AS (
                SELECT
                    thread.id AS thread_id,
                    COALESCE(
                        (
                            array_agg(
                                input.subject
                                ORDER BY
                                    input.ordinality
                                    DESC
                            )
                            FILTER (
                                WHERE
                                    input
                                    .root_candidate_message_id
                                    =
                                    thread
                                    .root_message_id
                            )
                        )[1],
                        NULLIF(
                            thread.subject,
                            '(placeholder)'
                        ),
                        (
                            array_agg(
                                input.subject
                                ORDER BY
                                    input.ordinality
                            )
                        )[1]
                    ) AS subject,
                    GREATEST(
                        thread.last_updated_at,
                        max(
                            input.message_timestamp
                        )
                    ) AS last_updated_at
                FROM metadata_input AS input
                JOIN threads AS thread
                  ON thread.id =
                        input.thread_id
                GROUP BY thread.id
            )
            UPDATE threads AS thread
            SET
                subject = metadata.subject,
                last_updated_at =
                    metadata.last_updated_at
            FROM metadata_by_thread AS metadata
            WHERE thread.id =
                    metadata.thread_id
            """,
            on: connection,
            logger: logger
        )
    }

    private func existingMessage(
        messageID: String,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> MessageState? {
        let rows = try await connection.query(
            """
            SELECT
                id,
                thread_id
            FROM messages
            WHERE message_id = \(messageID)
            FOR UPDATE
            """,
            logger: logger
        )

        return try await firstMessageState(
            rows
        )
    }

    private func lockedCursor(
        mailingListID: Int64,
        epoch: Int32,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> String? {
        let rows = try await connection.query(
            """
            SELECT last_scanned_commit_oid
            FROM mailing_list_archive_epochs
            WHERE mailing_list_id = \(mailingListID)
              AND epoch = \(epoch)
            FOR UPDATE
            """,
            logger: logger
        )

        for try await row in rows {
            return try row.decode(
                String?.self
            )
        }

        throw PostgresIngestError.missingEpoch(
            mailingListID: mailingListID,
            epoch: epoch
        )
    }

    private func advanceCursor(
        mailingListID: Int64,
        epoch: Int32,
        expectedPreviousCommitOID: String?,
        commitOID: String,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        let rows = try await connection.query(
            """
            UPDATE mailing_list_archive_epochs
            SET
                last_scanned_commit_oid =
                    \(commitOID),
                updated_at = now()
            WHERE mailing_list_id =
                    \(mailingListID)
              AND epoch = \(epoch)
              AND last_scanned_commit_oid
                    IS NOT DISTINCT FROM
                    \(expectedPreviousCommitOID)
            RETURNING last_scanned_commit_oid
            """,
            logger: logger
        )

        for try await _ in rows {
            return
        }

        throw PostgresIngestError.cursorMismatch(
            expected: expectedPreviousCommitOID,
            actual: try await lockedCursor(
                mailingListID: mailingListID,
                epoch: epoch,
                connection: connection,
                logger: logger
            )
        )
    }

    private func execute(
        _ query: PostgresQuery,
        on connection: PostgresConnection,
        logger: Logger
    ) async throws {
        let rows = try await connection.query(
            query,
            logger: logger
        )

        for try await _ in rows {}
    }

    private func firstMessageState(
        _ rows: PostgresRowSequence
    ) async throws -> MessageState? {
        for try await row in rows {
            let value = try row.decode(
                (Int64, Int64).self
            )

            return MessageState(
                id: value.0,
                threadID: value.1
            )
        }

        return nil
    }

    private func firstMessageUpsert(
        _ rows: PostgresRowSequence
    ) async throws -> (
        Int64,
        Int64?
    )? {
        for try await row in rows {
            return try row.decode(
                (Int64, Int64?).self
            )
        }

        return nil
    }

    private func firstInt64(
        _ rows: PostgresRowSequence
    ) async throws -> Int64? {
        for try await row in rows {
            return try row.decode(
                Int64.self
            )
        }

        return nil
    }
}
