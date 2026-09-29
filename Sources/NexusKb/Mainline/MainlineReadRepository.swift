import Foundation
import PostgresNIO
import Vapor

struct MainlineReadRepository: Sendable {
    let client: PostgresClient

    func statuses(patchsetIDs: [Int64], logger: Logger) async throws -> [Int64: MainlineStatusView]
    {
        guard !patchsetIDs.isEmpty else { return [:] }
        return try await client.withTransaction(logger: logger) { connection in
            _ = try await connection.query(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", logger: logger)
            return try await statuses(
                patchsetIDs: patchsetIDs, connection: connection, logger: logger)
        }
    }

    private func statuses(patchsetIDs: [Int64], connection: PostgresConnection, logger: Logger)
        async throws -> [Int64: MainlineStatusView]
    {
        var result: [Int64: MainlineStatusView] = [:]
        let state = try await indexStatus(connection: connection, logger: logger)
        for id in patchsetIDs {
            let rows = try await connection.query(
                """
                SELECT ps.total_parts, ps.status, p.part_index, p.message_id,
                       COALESCE(m.subject, ''), c.oid, c.subject, c.first_release,
                       pm.match_kind,
                       COALESCE(f.stable_patch_id IS NOT NULL AND f.matcher_version=\(MainlineIndexService.matcherVersion),false)
                FROM patchsets ps LEFT JOIN patches p ON p.patchset_id=ps.id
                LEFT JOIN messages m ON m.message_id=p.message_id
                LEFT JOIN mainline_patch_fingerprints f ON f.patch_id=p.id
                LEFT JOIN mainline_patch_matches pm ON pm.patch_id=p.id
                LEFT JOIN mainline_commits c ON c.oid=pm.commit_oid AND c.published
                WHERE ps.id=\(id) ORDER BY p.part_index, c.oid
                """, logger: logger)
            var total = 0
            var complete = false
            var allChecked = true
            var parts: [Int: (String, String, [MainlinePatchCommitView])] = [:]
            for try await row in rows {
                let cells = Array(row)
                total = Int(try cells[0].decode(Int32.self))
                complete = try cells[1].decode(String.self) == "Complete"
                allChecked = try cells[9].decode(Bool.self) && allChecked
                guard let part = try cells[2].decode(Int32?.self) else { continue }
                let oid = try cells[5].decode(String?.self)
                let messageID = try cells[3].decode(String.self)
                let subject = try cells[4].decode(String.self)
                var entry = parts[Int(part)] ?? (messageID, subject, [])
                if let oid, state.completed, state.lastError == nil {
                    entry.2.append(
                        .init(
                            oid: oid, subject: try cells[6].decode(String.self),
                            firstRelease: try cells[7].decode(String?.self),
                            matchKind: try cells[8].decode(String.self)))
                }
                parts[Int(part)] = entry
            }
            let views = parts.sorted { $0.key < $1.key }.map {
                MainlinePatchView(
                    partIndex: $0.key, messageId: $0.value.0, subject: $0.value.1,
                    commits: $0.value.2)
            }
            let matched = views.filter { !$0.commits.isEmpty }.count
            let allMatched =
                complete && total > 0 && matched == total && Set(parts.keys) == Set(1...total)
            let partReleases = views.map { view in
                view.commits.compactMap(\.firstRelease).min(by: MainlineVersion.less)
            }
            let containingRelease =
                allMatched && partReleases.allSatisfy { $0 != nil }
                ? partReleases.compactMap { $0 }.max(by: MainlineVersion.less)
                : nil
            let status: String
            if !state.completed || !allChecked || (!complete && matched == 0) {
                status = "not_checked"
            } else if matched == 0 {
                status = "no_match"
            } else if !allMatched {
                status = "partial"
            } else if containingRelease == nil {
                status = "merged_unreleased"
            } else {
                status = "merged_released"
            }
            result[id] = .init(
                state: status, checkedAt: allChecked && state.completed ? state.checkedAt : nil,
                coverageStart: state.baseRef,
                indexedTip: state.indexedTip, totalParts: total, matchedParts: matched,
                firstRelease: containingRelease, patches: views)
        }
        return result
    }

    func commit(prefix: String, logger: Logger) async throws -> MainlineCommitView? {
        guard prefix.count >= 7, prefix.count <= 64,
            prefix.utf8.allSatisfy({
                (48...57).contains($0) || (65...70).contains($0) || (97...102).contains($0)
            })
        else { throw Abort(.badRequest, reason: "Invalid commit hash") }
        do {
            return try await client.withTransaction(logger: logger) { connection in
                _ = try await connection.query(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", logger: logger)
                return try await commit(prefix: prefix, connection: connection, logger: logger)
            }
        } catch let error as PostgresTransactionError {
            if let abort = error.closureError as? Abort, error.rollbackError == nil { throw abort }
            throw error
        }
    }

    private func commit(prefix: String, connection: PostgresConnection, logger: Logger) async throws
        -> MainlineCommitView?
    {
        let state = try await indexStatus(connection: connection, logger: logger)
        guard state.completed, state.lastError == nil else {
            throw Abort(
                .serviceUnavailable,
                reason: "Mainline indexing is incomplete; retry after the index recovers")
        }
        let rows = try await connection.query(
            "SELECT oid, subject, first_release FROM mainline_commits WHERE published AND oid LIKE \(prefix.lowercased() + "%") ORDER BY oid LIMIT 2",
            logger: logger)
        var commits: [(String, String, String?)] = []
        for try await row in rows {
            let c = Array(row)
            commits.append(
                (
                    try c[0].decode(String.self), try c[1].decode(String.self),
                    try c[2].decode(String?.self)
                ))
        }
        guard commits.count < 2 else { throw Abort(.conflict, reason: "Ambiguous commit hash") }
        guard let found = commits.first else { return nil }
        let refs = try await strings(
            """
            SELECT r.message_id FROM mainline_commit_references r
            WHERE r.commit_oid=\(found.0) AND NOT EXISTS(SELECT 1 FROM messages m WHERE m.message_id=r.message_id AND NOT m.is_placeholder)
            ORDER BY r.message_id
            """, connection: connection, logger: logger)
        let rows2 = try await connection.query(
            """
            SELECT p.message_id, COALESCE(m.subject,''), t.root_message_id, p.patchset_id,
                   ls.lineage_id, ls.revision, p.part_index, pm.match_kind
            FROM mainline_patch_matches pm JOIN patches p ON p.id=pm.patch_id
            JOIN messages m ON m.message_id=p.message_id JOIN patchsets ps ON ps.id=p.patchset_id
            JOIN threads t ON t.id=ps.thread_id LEFT JOIN patchset_lineage_state ls ON ls.patchset_id=ps.id
            WHERE pm.commit_oid=\(found.0) ORDER BY p.patchset_id,p.part_index
            """, logger: logger)
        var submissions: [MainlineSubmissionView] = []
        for try await r in rows2 {
            let c = Array(r)
            submissions.append(
                .init(
                    messageId: try c[0].decode(String.self), subject: try c[1].decode(String.self),
                    rootMessageId: try c[2].decode(String.self),
                    patchsetId: try c[3].decode(Int64.self),
                    lineageId: try c[4].decode(Int64?.self),
                    revision: try c[5].decode(Int32?.self).map(Int.init),
                    partIndex: Int(try c[6].decode(Int32.self)),
                    matchKind: try c[7].decode(String.self)))
        }
        return .init(
            oid: found.0, subject: found.1, firstRelease: found.2, submissions: submissions,
            references: refs)
    }

    func indexStatus(logger: Logger) async throws -> MainlineIndexStatusView {
        try await client.withConnection { connection in
            try await indexStatus(connection: connection, logger: logger)
        }
    }

    private func indexStatus(connection: PostgresConnection, logger: Logger) async throws
        -> MainlineIndexStatusView
    {
        let rows = try await connection.query(
            "SELECT base_ref,coverage_start,indexed_tip,checked_at,completed,last_error FROM mainline_index_state WHERE singleton",
            logger: logger)
        for try await r in rows {
            let c = Array(r)
            return .init(
                baseRef: try c[0].decode(String.self), coverageStart: try c[1].decode(String?.self),
                indexedTip: try c[2].decode(String?.self), checkedAt: try c[3].decode(Date?.self),
                completed: try c[4].decode(Bool.self), lastError: try c[5].decode(String?.self))
        }
        return .init(
            baseRef: nil, coverageStart: nil, indexedTip: nil, checkedAt: nil, completed: false,
            lastError: nil)
    }
    private func strings(_ query: PostgresQuery, connection: PostgresConnection, logger: Logger)
        async throws -> [String]
    {
        var values: [String] = []
        let rows = try await connection.query(query, logger: logger)
        for try await row in rows { values.append(try row.decode(String.self)) }
        return values
    }
}

enum MainlineVersion {
    static func less(_ lhs: String, _ rhs: String) -> Bool {
        components(lhs).lexicographicallyPrecedes(components(rhs))
    }
    static func components(_ value: String) -> [Int] {
        value.dropFirst().split(separator: ".").compactMap { Int(String($0)) }
    }
    static func isFinalTag(_ value: String) -> Bool {
        // Modern mainline releases have two components. Linux 2.6 used three;
        // stable backports (v6.12.1 / v2.6.32.1) and all RCs are excluded.
        value.range(
            of: #"^v(?:[3-9][0-9]*\.[0-9]+|[12][0-9]+\.[0-9]+|2\.6\.[0-9]+)$"#,
            options: .regularExpression) != nil
    }
}
