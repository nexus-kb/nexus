import Foundation
import Logging
import PostgresNIO

enum MainlineIndexError: Error, Sendable {
    case busy
    case nonFastForward(String, String)
    case invalidBase(String)
    case changedRelease(String)
}

/// The host fetches the bare repository. This service only reads pinned Git
/// objects and publishes a reproducible, bounded index after a successful run.
struct MainlineIndexService: Sendable {
    static let matcherVersion = 1
    let client: PostgresClient
    let repositoryPath: String
    let baseRef: String

    init(
        client: PostgresClient, repositoryPath: String = "/opt/nexus/mainline.git",
        baseRef: String = "v2.6.12"
    ) {
        self.client = client
        self.repositoryPath = repositoryPath
        self.baseRef = baseRef
    }

    func run(logger: Logger, leaseCheck: (@Sendable () async throws -> Void)? = nil) async throws {
        try await client.withConnection { connection in
            let rows = try await connection.query(
                "SELECT pg_try_advisory_lock(62489201124001)", logger: logger)
            var acquired = false
            for try await row in rows { acquired = try row.decode(Bool.self) }
            // A competing job must not change the active job's status.
            guard acquired else { throw MainlineIndexError.busy }
            do {
                try await runLocked(connection: connection, logger: logger, leaseCheck: leaseCheck)
                _ = try await connection.query(
                    "SELECT pg_advisory_unlock(62489201124001)", logger: logger)
            } catch {
                _ = try? await connection.query(
                    """
                    INSERT INTO mainline_index_state(singleton,base_ref,completed,last_error)
                    VALUES(true,\(baseRef),false,\(String(reflecting: error)))
                    ON CONFLICT(singleton) DO UPDATE SET completed=false,last_error=excluded.last_error,updated_at=now()
                    """, logger: logger)
                _ = try? await connection.query(
                    "SELECT pg_advisory_unlock(62489201124001)", logger: logger)
                throw error
            }
        }
    }

    private func runLocked(
        connection: PostgresConnection, logger: Logger,
        leaseCheck: (@Sendable () async throws -> Void)?
    ) async throws {
        let git = MainlineGit(repositoryPath: repositoryPath)
        let tip = try await git.run(["rev-parse", "--verify", "refs/heads/master^{commit}"]).trimmed
        let base = try await git.run(["rev-parse", "--verify", "\(baseRef)^{commit}"]).trimmed
        guard try await git.isAncestor(base, of: tip) else {
            throw MainlineIndexError.invalidBase(baseRef)
        }
        var oldTip: String?
        var oldBase: String?
        var pendingTip: String?
        var pendingBase: String?
        let state = try await connection.query(
            "SELECT indexed_tip,coverage_start,target_tip,target_base FROM mainline_index_state WHERE singleton",
            logger: logger)
        for try await row in state {
            let cells = Array(row)
            oldTip = try cells[0].decode(String?.self)
            oldBase = try cells[1].decode(String?.self)
            pendingTip = try cells[2].decode(String?.self)
            pendingBase = try cells[3].decode(String?.self)
        }
        // Staged rows belong to this exact coverage range until publication.
        if let pendingBase, pendingBase != base {
            throw MainlineIndexError.invalidBase(baseRef)
        }
        for previous in [oldTip, pendingTip].compactMap({ $0 }) where previous != tip {
            guard try await git.isAncestor(previous, of: tip) else {
                throw MainlineIndexError.nonFastForward(previous, tip)
            }
        }
        if let oldBase, oldBase != base {
            // Expanding history is safe; silently narrowing it would leave
            // commits outside the advertised range in the index.
            guard try await git.isAncestor(base, of: oldBase) else {
                throw MainlineIndexError.invalidBase(baseRef)
            }
        }
        _ = try await connection.query(
            """
            INSERT INTO mainline_index_state(singleton,base_ref,coverage_start,target_tip,target_base,completed)
            VALUES(true,\(baseRef),\(base),\(tip),\(base),false)
            ON CONFLICT(singleton) DO UPDATE SET target_tip=excluded.target_tip,target_base=excluded.target_base,
                coverage_start=COALESCE(mainline_index_state.coverage_start,excluded.coverage_start),
                completed=false,last_error=NULL,updated_at=now()
            """, logger: logger)

        let range = oldBase == base ? "\(oldTip ?? base)..\(tip)" : "\(base)..\(tip)"
        let oids = try await git.run(["rev-list", "--reverse", "--topo-order", range])
            .split(whereSeparator: \.isWhitespace).map(String.init)
        for offset in stride(from: 0, to: oids.count, by: 128) {
            try await leaseCheck?()
            let batch = Array(oids[offset..<min(offset + 128, oids.count)])
            let existingRows = try await connection.query(
                "SELECT oid FROM mainline_commits WHERE oid=ANY(\(batch))", logger: logger)
            var existing: Set<String> = []
            for try await row in existingRows { existing.insert(try row.decode(String.self)) }
            let commits = try await git.commits(batch.filter { !existing.contains($0) })
            try await connection.withTransaction(logger: logger) { transaction in
                for commit in commits {
                    _ = try await transaction.query(
                        """
                        INSERT INTO mainline_commits(oid,subject,patch_id,patch_id_no_renames)
                        VALUES(\(commit.oid),\(commit.subject),\(commit.patchID),\(commit.patchIDNoRenames)) ON CONFLICT DO NOTHING
                        """, logger: logger)
                    for reference in MainlineGit.messageReferences(in: commit.message) {
                        _ = try await transaction.query(
                            """
                            INSERT INTO mainline_commit_references(commit_oid,message_id)
                            VALUES(\(commit.oid),\(reference)) ON CONFLICT DO NOTHING
                            """, logger: logger)
                    }
                }
            }
            if offset % 4096 == 0 {
                logger.info(
                    "Mainline commits indexed",
                    metadata: ["processed": "\(offset + batch.count)", "total": "\(oids.count)"])
            }
        }

        try await indexReleases(
            git: git, tip: tip, base: base, rescan: oldBase != base, connection: connection,
            logger: logger, leaseCheck: leaseCheck)
        try await fingerprintPatches(
            git: git, connection: connection, logger: logger, leaseCheck: leaseCheck)
        try await leaseCheck?()
        try await connection.withTransaction(logger: logger) { transaction in
            // Joining both indexes also matches late-arriving mail and old
            // patches to new commits without recomputing unchanged diffs.
            _ = try await transaction.query(
                """
                INSERT INTO mainline_patch_matches(patch_id,commit_oid,match_kind,matcher_version)
                SELECT p.id,c.oid,
                    CASE WHEN r.message_id IS NOT NULL THEN 'submission' ELSE 'equivalent' END,
                    \(Self.matcherVersion)
                FROM mainline_patch_fingerprints f
                JOIN patches p ON p.id=f.patch_id
                JOIN mainline_commits c ON c.patch_id=f.stable_patch_id OR c.patch_id_no_renames=f.stable_patch_id
                LEFT JOIN mainline_commit_references r ON r.commit_oid=c.oid AND r.message_id=p.message_id
                WHERE f.matcher_version=\(Self.matcherVersion)
                ON CONFLICT(patch_id,commit_oid) DO UPDATE SET
                    match_kind=excluded.match_kind,matcher_version=excluded.matcher_version
                """, logger: logger)
            _ = try await transaction.query(
                "UPDATE mainline_commits SET published=true WHERE NOT published", logger: logger)
            _ = try await transaction.query(
                """
                UPDATE mainline_index_state SET base_ref=\(baseRef),coverage_start=\(base),indexed_tip=\(tip),
                    target_tip=NULL,target_base=NULL,checked_at=now(),completed=true,last_error=NULL,matcher_version=\(Self.matcherVersion),updated_at=now()
                WHERE singleton
                """, logger: logger)
        }
    }

    private func indexReleases(
        git: MainlineGit, tip: String, base: String, rescan: Bool, connection: PostgresConnection,
        logger: Logger, leaseCheck: (@Sendable () async throws -> Void)?
    ) async throws {
        let names = try await git.run(["tag", "--list"])
            .split(whereSeparator: \.isWhitespace).map(String.init)
        // With a final-release boundary, earlier final tags cannot contain
        // commits in our range. Avoid walking the entire kernel DAG merely to
        // enumerate tags on every four-hour tick; validate new tags below.
        let tags = names.filter {
            MainlineVersion.isFinalTag($0)
                && (!MainlineVersion.isFinalTag(baseRef) || !MainlineVersion.less($0, baseRef))
        }.sorted(by: MainlineVersion.less)
        let rows = try await connection.query(
            "SELECT name,oid FROM mainline_releases", logger: logger)
        var indexed: [String: String] = [:]
        for try await row in rows {
            let cells = Array(row)
            indexed[try cells[0].decode(String.self)] = try cells[1].decode(String.self)
        }
        var previous = base
        for tag in tags {
            try await leaseCheck?()
            let tagOID = try await git.run(["rev-parse", "\(tag)^{commit}"]).trimmed
            if let known = indexed[tag], known != tagOID {
                throw MainlineIndexError.changedRelease(tag)
            }
            if rescan || indexed[tag] == nil {
                guard try await git.isAncestor(tagOID, of: tip)
                else { continue }
                // Final releases form the mainline ancestry chain. Walk each
                // release's additions once, rather than N commits × M tags.
                let oids = try await git.run(["rev-list", tagOID, "^\(previous)", "^\(base)"])
                    .split(whereSeparator: \.isWhitespace).map(String.init)
                for offset in stride(from: 0, to: oids.count, by: 1000) {
                    try await leaseCheck?()
                    let batch = Array(oids[offset..<min(offset + 1000, oids.count)])
                    _ = try await connection.query(
                        """
                        UPDATE mainline_commits SET first_release=\(tag)
                        WHERE oid=ANY(\(batch)) AND (first_release IS NULL OR
                            string_to_array(substring(first_release from 2),'.')::int[] >
                            string_to_array(substring(\(tag)::text from 2),'.')::int[])
                        """, logger: logger)
                }
                _ = try await connection.query(
                    "INSERT INTO mainline_releases(name,oid) VALUES(\(tag),\(tagOID)) ON CONFLICT DO NOTHING",
                    logger: logger)
            }
            previous = tagOID
        }
    }

    private func fingerprintPatches(
        git: MainlineGit, connection: PostgresConnection, logger: Logger,
        leaseCheck: (@Sendable () async throws -> Void)?
    ) async throws {
        var cursor: Int64 = 0
        // One reader and at most four hashing/writing batches. Keep both the
        // connection count and resident diff data bounded, and leave capacity
        // for mail ingestion and the job's lease heartbeat.
        try await withThrowingTaskGroup(of: Void.self) { group in
            var inFlight = 0
            var processed = 0
            while true {
                if inFlight == 4 {
                    try await group.next()
                    inFlight -= 1
                }
                try Task.checkCancellation()
                try await leaseCheck?()
                let rows = try await connection.query(
                    """
                    SELECT p.id,p.diff,md5(p.diff) FROM patches p
                    WHERE p.id>\(cursor) AND NOT EXISTS (
                        SELECT 1 FROM mainline_patch_fingerprints f
                        WHERE f.patch_id>\(cursor) AND f.patch_id=p.id AND f.matcher_version=\(Self.matcherVersion)
                    ) ORDER BY p.id LIMIT 128
                    """, logger: logger)
                var patches: [(Int64, String, String)] = []
                for try await row in rows {
                    let cells = Array(row)
                    patches.append(
                        (
                            try cells[0].decode(Int64.self),
                            try cells[1].decode(String.self), try cells[2].decode(String.self)
                        ))
                }
                guard let last = patches.last else { break }
                cursor = last.0
                let batch = patches
                group.addTask {
                    let stable = try await git.patchIDs(diffs: batch.map { $0.1 })
                    try Task.checkCancellation()
                    try await leaseCheck?()
                    let values = zip(batch, stable).map {
                        PatchFingerprint(id: $0.0.0, content: $0.0.2, stable: $0.1)
                    }
                    try await persistFingerprints(values, logger: logger)
                }
                inFlight += 1
                processed += batch.count
                if processed % 16384 == 0 {
                    logger.info(
                        "Mainline patches submitted for fingerprinting",
                        metadata: ["processed": "\(processed)"])
                }
            }
            try await group.waitForAll()
        }
    }

    struct PatchFingerprint: Encodable, Sendable {
        let id: Int64
        let content: String
        let stable: String?
    }

    func persistFingerprints(_ values: [PatchFingerprint], logger: Logger) async throws {
        let json = String(decoding: try JSONEncoder().encode(values), as: UTF8.self)
        try await client.withTransaction(logger: logger) { transaction in
            // Lock only unchanged inputs, in ID order. Ingestion either wins
            // before this check or invalidates these results after we commit.
            _ = try await transaction.query(
                """
                WITH input AS (
                    SELECT * FROM jsonb_to_recordset(\(json)::jsonb) AS x(id bigint,content text,stable text)
                ), current AS MATERIALIZED (
                    SELECT p.id,i.content,i.stable FROM patches p JOIN input i ON i.id=p.id
                    WHERE md5(p.diff)=i.content ORDER BY p.id FOR UPDATE OF p
                ), removed AS (
                    DELETE FROM mainline_patch_matches m USING current c WHERE m.patch_id=c.id
                )
                INSERT INTO mainline_patch_fingerprints(patch_id,content_fingerprint,stable_patch_id,matcher_version)
                SELECT id,content,stable,\(Self.matcherVersion) FROM current
                ON CONFLICT(patch_id) DO UPDATE SET content_fingerprint=excluded.content_fingerprint,
                    stable_patch_id=excluded.stable_patch_id,matcher_version=excluded.matcher_version,checked_at=now()
                """, logger: logger)
        }
    }
}

extension String {
    fileprivate var trimmed: String { trimmingCharacters(in: .whitespacesAndNewlines) }
}
