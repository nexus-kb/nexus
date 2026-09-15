//
//  PostgresPatchIngestService.swift
//  NexusKb
//
//  Created by Tanuj Ravi Rao on 8/13/26.
//

import Foundation
import PostgresNIO
import Vapor

enum PostgresPatchIngestError:
    Error,
    Sendable,
    Equatable
{
    case indexCollision(
        patchSetID: Int64,
        partIndex: Int32,
        incomingMessageID: String,
        existingMessageID: String
    )
    case missingPatchSet
}

struct PostgresPatchIngestService: Sendable {
    private struct Candidate {
        let id: Int64
        let threadID: Int64
        let coverLetterMessageID: String?
        let subject: String
        let author: String?
        let sentAt: Date?
        let totalParts: Int32
        let receivedParts: Int32
        let subjectIndex: Int32
        let containsIncomingPatch: Bool
        let hasPartIndexCollision: Bool
        let anchorMessageID: String?
    }
    
    func persist(
        parsed: ParsedIngestMessage,
        threadID: Int64,
        timestamp: Date,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Int64? {
        guard parsed.patch.isPatchOrCover else {
            return nil
        }

        let message = parsed.message
        let metadata = parsed.patch

        let coverLetterMessageID: String?

        if metadata.partIndex == 0
            || metadata.totalParts == 1
        {
            coverLetterMessageID = message.messageID
        } else {
            coverLetterMessageID = message.inReplyTo
        }

        guard metadata.diff != nil
                || coverLetterMessageID != nil
        else {
            return nil
        }

        if let existingPatchSetID = try await existingPatchSetID(
            messageID: message.messageID,
            connection: connection,
            logger: logger
        ) {
            if let diff = metadata.diff {
                try await execute(
                    """
                    UPDATE patches
                    SET diff = \(diff)
                    WHERE message_id = \(message.messageID)
                    """,
                    connection: connection,
                    logger: logger
                )
            }
            return existingPatchSetID
        }

        let candidates = try await matchingCandidates(
            parsed: parsed,
            threadID: threadID,
            timestamp: timestamp,
            coverLetterMessageID: coverLetterMessageID,
            connection: connection,
            logger: logger
        )

        let patchSetID: Int64

        if let target = candidates.first {
            patchSetID = target.id

            for source in candidates.dropFirst() {
                try await mergePatchSet(
                    source.id,
                    into: patchSetID,
                    connection: connection,
                    logger: logger
                )
            }

            try await updatePatchSet(
                patchSetID,
                parsed: parsed,
                threadID: threadID,
                timestamp: timestamp,
                coverLetterMessageID:
                    coverLetterMessageID,
                connection: connection,
                logger: logger
            )
        } else {
            patchSetID = try await createPatchSet(
                parsed: parsed,
                threadID: threadID,
                timestamp: timestamp,
                coverLetterMessageID:
                    coverLetterMessageID,
                connection: connection,
                logger: logger
            )
        }

        if let diff = metadata.diff {
            try await persistPatch(
                patchSetID: patchSetID,
                messageID: message.messageID,
                partIndex: metadata.partIndex,
                diff: diff,
                connection: connection,
                logger: logger
            )
        }

        try await refreshPatchSet(
            patchSetID,
            malformed:
                metadata.partIndex < 0
                || metadata.partIndex
                    > metadata.totalParts,
            connection: connection,
            logger: logger
        )

        return patchSetID
    }
    
    private func matchingCandidates(
        parsed: ParsedIngestMessage,
        threadID: Int64,
        timestamp: Date,
        coverLetterMessageID: String?,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> [Candidate] {
        if let existing = try await candidateForPatch(
            messageID: parsed.message.messageID,
            connection: connection,
            logger: logger
        ) {
            return [existing]
        }

        let candidates = try await loadCandidates(
            parsed: parsed,
            threadID: threadID,
            timestamp: timestamp,
            coverLetterMessageID:
                coverLetterMessageID,
            connection: connection,
            logger: logger
        )

        let newVersion = parsed.patch.version ?? 1
        let cleanNew = cleanSubject(
            parsed.message.subject
        )

        var matches: [Candidate] = []

        for candidate in candidates {
            let sameThread =
                candidate.threadID == threadID

            let isCoverDuplicate =
                candidate.coverLetterMessageID
                == parsed.message.messageID

            let isPatchDuplicate =
                candidate.containsIncomingPatch

            if candidate.receivedParts
                    >= candidate.totalParts,
               !isCoverDuplicate,
               !isPatchDuplicate,
               parsed.patch.partIndex != 0
            {
                continue
            }

            guard authorsMatch(
                candidate.author,
                parsed.author
            ) else {
                continue
            }

            if let sentAt = candidate.sentAt,
               abs(
                   sentAt.timeIntervalSince(
                       timestamp
                   )
               ) >= 86_400
            {
                continue
            }

            let existingVersion =
                subjectVersion(
                    candidate.subject
                ) ?? 1

            guard existingVersion == newVersion
                    || sameThread
            else {
                continue
            }

            guard candidate.totalParts
                    == parsed.patch.totalParts
                    || candidate.totalParts == 1
                    || parsed.patch.totalParts == 1
            else {
                continue
            }

            let hasIndexCollision: Bool

            if parsed.patch.partIndex == 0 {
                hasIndexCollision =
                    candidate.subjectIndex == 0
                    && candidate
                        .coverLetterMessageID != nil
                    && candidate
                        .coverLetterMessageID
                        != parsed.message.messageID
            } else {
                hasIndexCollision =
                    candidate.hasPartIndexCollision
            }

            guard !hasIndexCollision else {
                continue
            }

            let cleanOld = cleanSubject(
                candidate.subject
            )

            let subjectMatches: Bool

            if parsed.patch.totalParts == 1 {
                subjectMatches =
                    parsed.message.subject
                        == candidate.subject
                    || (
                        parsed.patch.partIndex == 0
                        && candidate.subjectIndex == 1
                    )
                    || (
                        parsed.patch.partIndex == 1
                        && candidate.subjectIndex == 0
                    )
                    || cleanNew == cleanOld
            } else if parsed.patch.partIndex
                        == candidate.subjectIndex
            {
                subjectMatches =
                    cleanNew == cleanOld
            } else {
                subjectMatches = true
            }

            guard subjectMatches else {
                continue
            }

            if !sameThread {
                guard subjectPrefixes(
                    parsed.message.subject
                ) == subjectPrefixes(
                    candidate.subject
                ) else {
                    continue
                }

                let existingPrefix =
                    candidate.anchorMessageID.map(
                        messageIDPrefix
                    ) ?? ""

                let newPrefix = messageIDPrefix(
                    parsed.message.messageID
                )

                let prefixMatches =
                    existingPrefix == newPrefix
                    && newPrefix.count > 10

                guard parsed.patch.totalParts == 1
                        || prefixMatches
                else {
                    continue
                }
            }

            matches.append(candidate)
        }

        return Dictionary(
            grouping: matches,
            by: \.id
        )
        .compactMap(\.value.first)
        .sorted {
            $0.id < $1.id
        }
    }

    private func candidateForPatch(
        messageID: String,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Candidate? {
        let rows = try await connection.query(
            """
            SELECT
                ps.id,
                ps.thread_id,
                ps.cover_letter_message_id,
                ps.subject,
                ps.author,
                ps.sent_at,
                ps.total_parts,
                ps.received_parts,
                ps.subject_index,
                true,
                false,
                COALESCE(
                    ps.cover_letter_message_id,
                    patch.message_id
                )
            FROM patchsets AS ps
            JOIN patches AS patch
              ON patch.patchset_id = ps.id
            WHERE patch.message_id = \(messageID)
            FOR UPDATE OF ps
            """,
            logger: logger
        )

        return try await firstCandidate(rows)
    }

    private func loadCandidates(
        parsed: ParsedIngestMessage,
        threadID: Int64,
        timestamp: Date,
        coverLetterMessageID: String?,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> [Candidate] {
        let start = timestamp.addingTimeInterval(
            -86_400
        )
        let end = timestamp.addingTimeInterval(
            86_400
        )

        let rows = try await connection.query(
            """
            WITH candidate_ids AS (
                SELECT id
                FROM patchsets
                WHERE thread_id = \(threadID)

                UNION

                SELECT id
                FROM patchsets
                WHERE cover_letter_message_id =
                        \(coverLetterMessageID)

                UNION

                SELECT id
                FROM patchsets
                WHERE author =
                        \(parsed.authorDisplayString)
                  AND sent_at BETWEEN
                        \(start) AND \(end)
            )
            SELECT
                patchset.id,
                patchset.thread_id,
                patchset.cover_letter_message_id,
                patchset.subject,
                patchset.author,
                patchset.sent_at,
                patchset.total_parts,
                patchset.received_parts,
                patchset.subject_index,
                EXISTS (
                    SELECT 1
                    FROM patches AS patch
                    WHERE patch.patchset_id =
                            patchset.id
                      AND patch.message_id =
                            \(parsed.message.messageID)
                ),
                EXISTS (
                    SELECT 1
                    FROM patches AS patch
                    WHERE patch.patchset_id =
                            patchset.id
                      AND patch.part_index =
                            \(parsed.patch.partIndex)
                      AND patch.message_id <>
                            \(parsed.message.messageID)
                ),
                COALESCE(
                    patchset.cover_letter_message_id,
                    (
                        SELECT patch.message_id
                        FROM patches AS patch
                        WHERE patch.patchset_id =
                                patchset.id
                        ORDER BY patch.part_index
                        LIMIT 1
                    )
                )
            FROM patchsets AS patchset
            JOIN candidate_ids AS candidate
              ON candidate.id = patchset.id
            ORDER BY patchset.id
            FOR UPDATE OF patchset
            """,
            logger: logger
        )

        var candidates: [Candidate] = []

        for try await row in rows {
            let value = try row.decode(
                (
                    Int64,
                    Int64,
                    String?,
                    String?,
                    String?,
                    Date?,
                    Int32,
                    Int32,
                    Int32,
                    Bool,
                    Bool,
                    String?
                ).self
            )

            candidates.append(
                Candidate(
                    id: value.0,
                    threadID: value.1,
                    coverLetterMessageID: value.2,
                    subject: value.3 ?? "",
                    author: value.4,
                    sentAt: value.5,
                    totalParts: value.6,
                    receivedParts: value.7,
                    subjectIndex: value.8,
                    containsIncomingPatch:
                        value.9,
                    hasPartIndexCollision:
                        value.10,
                    anchorMessageID: value.11
                )
            )
        }

        return candidates
    }

    private func firstCandidate(
        _ rows: PostgresRowSequence
    ) async throws -> Candidate? {
        for try await row in rows {
            let value = try row.decode(
                (
                    Int64,
                    Int64,
                    String?,
                    String?,
                    String?,
                    Date?,
                    Int32,
                    Int32,
                    Int32,
                    Bool,
                    Bool,
                    String?
                ).self
            )

            return Candidate(
                id: value.0,
                threadID: value.1,
                coverLetterMessageID: value.2,
                subject: value.3 ?? "",
                author: value.4,
                sentAt: value.5,
                totalParts: value.6,
                receivedParts: value.7,
                subjectIndex: value.8,
                containsIncomingPatch:
                    value.9,
                hasPartIndexCollision:
                    value.10,
                anchorMessageID: value.11
            )
        }

        return nil
    }

    private func createPatchSet(
        parsed: ParsedIngestMessage,
        threadID: Int64,
        timestamp: Date,
        coverLetterMessageID: String?,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Int64 {
        let rows = try await connection.query(
            """
            INSERT INTO patchsets (
                thread_id,
                cover_letter_message_id,
                subject,
                author,
                sent_at,
                status,
                total_parts,
                received_parts,
                subject_index,
                parser_version,
                to_recipients,
                cc_recipients
            )
            VALUES (
                \(threadID),
                \(coverLetterMessageID),
                \(parsed.message.subject),
                \(parsed.authorDisplayString),
                \(timestamp),
                'Incomplete',
                \(parsed.patch.totalParts),
                0,
                \(parsed.patch.partIndex),
                \(ParsedPatchMetadata.parserVersion),
                \(parsed.toDisplayString),
                \(parsed.ccDisplayString)
            )
            RETURNING id
            """,
            logger: logger
        )

        for try await row in rows {
            return try row.decode(Int64.self)
        }

        throw PostgresPatchIngestError
            .missingPatchSet
    }

    private func updatePatchSet(
        _ patchSetID: Int64,
        parsed: ParsedIngestMessage,
        threadID: Int64,
        timestamp: Date,
        coverLetterMessageID: String?,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE patchsets
            SET
                thread_id = \(threadID),
                cover_letter_message_id =
                    COALESCE(
                        \(coverLetterMessageID),
                        cover_letter_message_id
                    ),
                subject = CASE
                    WHEN \(parsed.patch.partIndex)
                            < subject_index
                    THEN \(parsed.message.subject)
                    ELSE subject
                END,
                subject_index = LEAST(
                    subject_index,
                    \(parsed.patch.partIndex)
                ),
                author =
                    \(parsed.authorDisplayString),
                sent_at = COALESCE(
                    sent_at,
                    \(timestamp)
                ),
                total_parts = CASE
                    WHEN \(parsed.patch.totalParts) = 1
                         AND total_parts > 1
                    THEN total_parts
                    ELSE \(parsed.patch.totalParts)
                END,
                parser_version =
                    \(ParsedPatchMetadata.parserVersion),
                to_recipients =
                    \(parsed.toDisplayString),
                cc_recipients =
                    \(parsed.ccDisplayString),
                updated_at = now()
            WHERE id = \(patchSetID)
            """,
            connection: connection,
            logger: logger
        )
    }

    private func persistPatch(
        patchSetID: Int64,
        messageID: String,
        partIndex: Int32,
        diff: String,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        guard partIndex >= 1 else {
            return
        }

        if let existingMessageID = try await patchIndexOccupant(
            patchSetID: patchSetID,
            messageID: messageID,
            partIndex: partIndex,
            connection: connection,
            logger: logger
        ) {
            throw PostgresPatchIngestError
                .indexCollision(
                    patchSetID: patchSetID,
                    partIndex: partIndex,
                    incomingMessageID: messageID,
                    existingMessageID: existingMessageID
                )
        }

        let oldPatchSetID =
            try await existingPatchSetID(
                messageID: messageID,
                connection: connection,
                logger: logger
            )

        try await execute(
            """
            INSERT INTO patches (
                patchset_id,
                message_id,
                part_index,
                diff
            )
            VALUES (
                \(patchSetID),
                \(messageID),
                \(partIndex),
                \(diff)
            )
            ON CONFLICT (message_id) DO UPDATE
            SET
                patchset_id =
                    EXCLUDED.patchset_id,
                part_index =
                    EXCLUDED.part_index,
                diff =
                    EXCLUDED.diff
            """,
            connection: connection,
            logger: logger
        )

        if let oldPatchSetID,
           oldPatchSetID != patchSetID
        {
            try await refreshPatchSet(
                oldPatchSetID,
                malformed: false,
                connection: connection,
                logger: logger
            )
        }
    }

    private func mergePatchSet(
        _ sourcePatchSetID: Int64,
        into targetPatchSetID: Int64,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        guard sourcePatchSetID
                != targetPatchSetID
        else {
            return
        }

        try await execute(
            """
            DELETE FROM patches AS source
            USING patches AS target
            WHERE source.patchset_id =
                    \(sourcePatchSetID)
              AND target.patchset_id =
                    \(targetPatchSetID)
              AND source.part_index =
                    target.part_index
            """,
            connection: connection,
            logger: logger
        )

        try await execute(
            """
            UPDATE patches
            SET patchset_id =
                \(targetPatchSetID)
            WHERE patchset_id =
                \(sourcePatchSetID)
            """,
            connection: connection,
            logger: logger
        )

        try await execute(
            """
            INSERT INTO patchsets_subsystems (
                patchset_id,
                subsystem_id
            )
            SELECT
                \(targetPatchSetID),
                subsystem_id
            FROM patchsets_subsystems
            WHERE patchset_id =
                \(sourcePatchSetID)
            ON CONFLICT DO NOTHING
            """,
            connection: connection,
            logger: logger
        )

        try await execute(
            """
            UPDATE patchsets AS target
            SET
                subject = CASE
                    WHEN source.subject_index
                            < target.subject_index
                    THEN source.subject
                    ELSE target.subject
                END,
                subject_index = LEAST(
                    target.subject_index,
                    source.subject_index
                ),
                cover_letter_message_id =
                    COALESCE(
                        target.cover_letter_message_id,
                        source.cover_letter_message_id
                    ),
                total_parts = GREATEST(
                    target.total_parts,
                    source.total_parts
                ),
                updated_at = now()
            FROM patchsets AS source
            WHERE target.id =
                    \(targetPatchSetID)
              AND source.id =
                    \(sourcePatchSetID)
            """,
            connection: connection,
            logger: logger
        )

        try await execute(
            """
            DELETE FROM patchsets
            WHERE id = \(sourcePatchSetID)
            """,
            connection: connection,
            logger: logger
        )
    }

    private func refreshPatchSet(
        _ patchSetID: Int64,
        malformed: Bool,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        try await execute(
            """
            WITH patch_count AS (
                SELECT count(*)::integer AS value
                FROM patches
                WHERE patchset_id = \(patchSetID)
            )
            UPDATE patchsets
            SET
                received_parts = patch_count.value,
                status = CASE
                    WHEN \(malformed)
                    THEN 'Malformed'
                    WHEN status = 'Malformed'
                    THEN status
                    WHEN patch_count.value >=
                            total_parts
                    THEN 'Complete'
                    ELSE 'Incomplete'
                END,
                updated_at = now()
            FROM patch_count
            WHERE id = \(patchSetID)
            """,
            connection: connection,
            logger: logger
        )
    }

    private func patchIndexOccupant(
        patchSetID: Int64,
        messageID: String,
        partIndex: Int32,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> String? {
        let rows = try await connection.query(
            """
            SELECT message_id
            FROM patches
            WHERE patchset_id = \(patchSetID)
              AND part_index = \(partIndex)
              AND message_id <> \(messageID)
            LIMIT 1
            """,
            logger: logger
        )

        for try await row in rows {
            return try row.decode(String.self)
        }

        return nil
    }

    private func existingPatchSetID(
        messageID: String,
        connection: PostgresConnection,
        logger: Logger
    ) async throws -> Int64? {
        let rows = try await connection.query(
            """
            SELECT patchset_id
            FROM patches
            WHERE message_id = \(messageID)
            FOR UPDATE
            """,
            logger: logger
        )

        for try await row in rows {
            return try row.decode(Int64.self)
        }

        return nil
    }

    private func messageIDPrefix(
        _ messageID: String
    ) -> String {
        String(
            messageID.split(
                separator: "-",
                maxSplits: 1
            ).first ?? ""
        )
    }

    private func authorsMatch(
        _ storedAuthor: String?,
        _ incomingAuthor: IngestMailbox
    ) -> Bool {
        guard let storedAuthor,
              let stored = IngestAddressProjector
                .parseList(storedAuthor)
                .first
        else {
            return false
        }

        return stored.address
            .caseInsensitiveCompare(
                incomingAuthor.address
            ) == .orderedSame
    }

    private func cleanSubject(
        _ subject: String
    ) -> String {
        var value = subject.replacingOccurrences(
            of: #"\[.*?\]"#,
            with: "",
            options: .regularExpression
        ).trimmingCharacters(
            in: .whitespacesAndNewlines
        )

        let prefixes = [
            "re:",
            "fwd:",
            "aw:",
            "forwarded:",
            "回复:",
            "回复：",
        ]

        while let prefix = prefixes.first(
            where: {
                value.lowercased()
                    .hasPrefix($0)
            }
        ) {
            value = String(
                value.dropFirst(prefix.count)
            ).trimmingCharacters(
                in: .whitespacesAndNewlines
            )
        }

        return value
    }

    private func subjectVersion(
        _ subject: String
    ) -> Int32? {
        let pattern =
            #"(?i)(?:\[[^\]]*?\bv(\d+)\b[^\]]*?\]|^\s*v(\d+)\b|PATCH\W*v(\d+)\b)"#

        guard let expression =
                try? NSRegularExpression(
                    pattern: pattern
                )
        else {
            return nil
        }

        let source = subject as NSString

        guard let match =
                expression.firstMatch(
                    in: subject,
                    range: NSRange(
                        location: 0,
                        length: source.length
                    )
                )
        else {
            return nil
        }

        for index in 1..<match.numberOfRanges {
            let range = match.range(at: index)

            if range.location != NSNotFound {
                return Int32(
                    source.substring(with: range)
                )
            }
        }

        return nil
    }

    private func subjectPrefixes(
        _ subject: String
    ) -> [String] {
        guard let expression =
                try? NSRegularExpression(
                    pattern: #"\[(.*?)\]"#
                )
        else {
            return []
        }

        let source = subject as NSString
        var prefixes = Set<String>()

        for match in expression.matches(
            in: subject,
            range: NSRange(
                location: 0,
                length: source.length
            )
        ) {
            guard match.numberOfRanges > 1 else {
                continue
            }

            let content = source.substring(
                with: match.range(at: 1)
            )

            for token in content.split(
                whereSeparator: {
                    !$0.isLetter
                    && !$0.isNumber
                    && $0 != "-"
                    && $0 != "."
                    && $0 != "_"
                }
            ) {
                let value = String(token)
                let lower = value.lowercased()

                if lower == "patch"
                    || lower == "rfc"
                    || lower == "resend"
                    || lower.allSatisfy(\.isNumber)
                    || (
                        lower.hasPrefix("v")
                        && lower.dropFirst()
                            .allSatisfy(\.isNumber)
                    )
                {
                    continue
                }

                prefixes.insert(value)
            }
        }

        return prefixes.sorted()
    }

    private func execute(
        _ query: PostgresQuery,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        let rows = try await connection.query(
            query,
            logger: logger
        )

        for try await _ in rows {}
    }
}
