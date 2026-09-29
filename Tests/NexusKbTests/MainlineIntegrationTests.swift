import Foundation
import PostgresNIO
import Testing
import Vapor
import VaporTesting

@testable import NexusKb

// The mainline index is a singleton. These tests intentionally reset that
// cache and must only run against an explicitly disposable database.
@Suite(
    "Mainline integration", .serialized,
    .enabled(if: Environment.get("NEXUS_TEST_DISPOSABLE") == "1"))
struct MainlineIntegrationTests {
    @Test(
        "Optional real-clone smoke test",
        .enabled(if: Environment.get("NEXUS_MAINLINE_SMOKE_BASE") != nil))
    func indexesRealMainline() async throws {
        let base = try #require(Environment.get("NEXUS_MAINLINE_SMOKE_BASE"))
        let git = MainlineGit(repositoryPath: "/opt/nexus/mainline.git")
        let tip = try await git.run(["rev-parse", "master"]).trimmingCharacters(
            in: .whitespacesAndNewlines)
        let expected = try await git.run(["rev-list", "\(base)..\(tip)"]).split(separator: "\n")
            .map(String.init)
        #expect(!expected.isEmpty)
        let finalTags = try await git.run(["tag", "--list"]).split(whereSeparator: \.isWhitespace)
            .map(String.init).filter(MainlineVersion.isFinalTag).filter {
                !MainlineVersion.isFinalTag(base) || !MainlineVersion.less($0, base)
            }.sorted(by: MainlineVersion.less)
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            do {
                let service = MainlineIndexService(client: app.postgres, baseRef: base)
                try await service.run(logger: app.logger)
                let rows = try await app.postgres.query(
                    "SELECT count(*) FROM mainline_commits WHERE published", logger: app.logger)
                for try await row in rows {
                    #expect(try row.decode(Int64.self) == Int64(expected.count))
                }
                let repository = MainlineReadRepository(client: app.postgres)
                for index in stride(from: 0, to: expected.count, by: max(1, expected.count / 12)) {
                    let oid = expected[index]
                    let result = try #require(
                        try await repository.commit(prefix: oid, logger: app.logger))
                    var expectedRelease: String?
                    for tag in finalTags {
                        if try await git.isAncestor(oid, of: tag),
                            try await git.isAncestor(tag, of: tip)
                        {
                            expectedRelease = tag
                            break
                        }
                    }
                    #expect(result.firstRelease == expectedRelease)
                    let parents = try await git.run(["rev-list", "--parents", "-n1", oid]).split(
                        whereSeparator: \.isWhitespace)
                    if parents.count == 2 {
                        let diff = try await git.run([
                            "show", "--format=", "--find-renames=50%", "-l0", "--binary",
                            "--full-index",
                            "--diff-algorithm=myers", oid,
                        ])
                        let expectedPatchID = try await git.patchID(diff: diff)
                        let ids = try await app.postgres.query(
                            "SELECT patch_id FROM mainline_commits WHERE oid=\(oid)",
                            logger: app.logger)
                        for try await row in ids {
                            #expect(try row.decode(String?.self) == expectedPatchID)
                        }
                    }
                }
                try await service.run(logger: app.logger)
                #expect(try await repository.indexStatus(logger: app.logger).indexedTip == tip)
                app.logger.info(
                    "Real mainline smoke test verified \(expected.count) commits and incremental rerun"
                )
            } catch {
                try? await clearIndex(app)
                throw error
            }
            try await clearIndex(app)
        }
    }

    @Test("Git ancestry, final releases, late mail, attribution, and invalidation")
    func indexesAndMatches() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            let db = app.postgres
            let logger = app.logger
            try await clearIndex(app)
            let repository = MainlineReadRepository(client: db)
            let service = MainlineIndexService(
                client: db, repositoryPath: fixture.directory.path, baseRef: "v6.12")
            var threadIDs: [Int64] = []
            do {
                // Two revisions share the same diff. Only one is explicitly
                // referenced; the other must remain content-equivalent.
                let v1 = try await addSeries(
                    app, ids: [fixture.oldMessageID], diffs: [fixture.firstDiff])
                let v2 = try await addSeries(
                    app, ids: [fixture.sourceMessageID], diffs: [fixture.firstDiff])
                threadIDs += [v1.threadID, v2.threadID]
                let missingDiff = fixture.firstDiff.replacingOccurrences(
                    of: "+first", with: "+different-change")
                let partial = try await addSeries(
                    app, ids: ["partial-a-\(fixture.token)@test", fixture.unrelatedMessageID],
                    diffs: [fixture.firstDiff, missingDiff])
                let incomplete = try await addSeries(
                    app, ids: ["incomplete-\(fixture.token)@test"], diffs: [fixture.firstDiff],
                    total: 3)
                threadIDs += [partial.threadID, incomplete.threadID]
                var statuses = try await repository.statuses(
                    patchsetIDs: [v2.patchsetID], logger: logger)
                #expect(statuses[v2.patchsetID]?.state == "not_checked")

                try await service.run(logger: logger)
                statuses = try await repository.statuses(
                    patchsetIDs: [
                        v1.patchsetID, v2.patchsetID, partial.patchsetID, incomplete.patchsetID,
                    ], logger: logger)
                #expect(statuses[v2.patchsetID]?.state == "merged_released")
                #expect(statuses[v2.patchsetID]?.firstRelease == "v6.13")
                #expect(
                    statuses[v2.patchsetID]?.patches.first?.commits.first?.oid == fixture.firstOID)
                #expect(
                    statuses[v2.patchsetID]?.patches.first?.commits.first?.matchKind == "submission"
                )
                #expect(
                    statuses[v1.patchsetID]?.patches.first?.commits.first?.matchKind == "equivalent"
                )
                #expect(statuses[partial.patchsetID]?.state == "partial")
                #expect(statuses[partial.patchsetID]?.matchedParts == 1)
                #expect(statuses[partial.patchsetID]?.patches.last?.commits.isEmpty == true)
                #expect(statuses[incomplete.patchsetID]?.state == "partial")
                #expect(statuses[incomplete.patchsetID]?.totalParts == 3)

                // The first commit was on a side branch: first-parent-only
                // enumeration would lose it. Commit lookup returns both mails.
                let commit = try #require(
                    try await repository.commit(
                        prefix: String(fixture.firstOID.prefix(12)), logger: logger))
                #expect(commit.oid == fixture.firstOID)
                #expect(commit.firstRelease == "v6.13")
                #expect(
                    commit.submissions.contains {
                        $0.messageId == fixture.oldMessageID && $0.matchKind == "equivalent"
                    })
                #expect(
                    commit.submissions.contains {
                        $0.messageId == fixture.sourceMessageID && $0.matchKind == "submission"
                    })
                #expect(!commit.submissions.contains { $0.messageId == fixture.unrelatedMessageID })
                #expect(commit.references == [fixture.externalMessageID])

                // A new mail must not inherit the global checked state before
                // being processed; a no-tip-change run must still match it.
                let late = try await addSeries(
                    app, ids: ["late-\(fixture.token)@test"], diffs: [fixture.secondDiff])
                threadIDs.append(late.threadID)
                statuses = try await repository.statuses(
                    patchsetIDs: [late.patchsetID], logger: logger)
                #expect(statuses[late.patchsetID]?.state == "not_checked")
                #expect(statuses[late.patchsetID]?.checkedAt == nil)
                try await service.run(logger: logger)
                statuses = try await repository.statuses(
                    patchsetIDs: [late.patchsetID], logger: logger)
                #expect(statuses[late.patchsetID]?.state == "merged_unreleased")
                #expect(statuses[late.patchsetID]?.firstRelease == nil)
                #expect(
                    statuses[late.patchsetID]?.patches.first?.commits.first?.oid
                        == fixture.secondOID)

                // A final tag at an unchanged branch tip must update release
                // membership; neither v6.14-rc1 nor v6.13.1 qualifies.
                _ = try await fixture.git.run(["tag", "v6.14", fixture.secondOID])
                try await service.run(logger: logger)
                statuses = try await repository.statuses(
                    patchsetIDs: [late.patchsetID], logger: logger)
                #expect(statuses[late.patchsetID]?.firstRelease == "v6.14")

                let spanning = try await addSeries(
                    app, ids: ["span-a-\(fixture.token)@test", "span-b-\(fixture.token)@test"],
                    diffs: [fixture.firstDiff, fixture.secondDiff])
                threadIDs.append(spanning.threadID)
                try await service.run(logger: logger)
                statuses = try await repository.statuses(
                    patchsetIDs: [spanning.patchsetID], logger: logger)
                #expect(statuses[spanning.patchsetID]?.firstRelease == "v6.14")
                #expect(
                    statuses[spanning.patchsetID]?.patches.map { $0.commits.first?.firstRelease }
                        == ["v6.13", "v6.14"])

                // A changed diff invalidates old matches immediately, even
                // before the next worker run. The unrelated Link is not proof.
                _ = try await db.query(
                    "UPDATE patches SET diff=\(missingDiff) WHERE patchset_id=\(v2.patchsetID)",
                    logger: logger)
                statuses = try await repository.statuses(
                    patchsetIDs: [v2.patchsetID], logger: logger)
                #expect(statuses[v2.patchsetID]?.state == "not_checked")
                #expect(statuses[v2.patchsetID]?.matchedParts == 0)
                try await service.run(logger: logger)
                statuses = try await repository.statuses(
                    patchsetIDs: [v2.patchsetID], logger: logger)
                #expect(statuses[v2.patchsetID]?.state == "no_match")

                try await app.testing().test(.GET, "/api/v1/commits/\(fixture.firstOID)") {
                    response async throws in
                    #expect(response.status == .ok)
                    let value = try response.content.decode(MainlineCommitView.self)
                    #expect(value.oid == fixture.firstOID)
                    #expect(value.firstRelease == "v6.13")
                }
                try await app.testing().test(.GET, "/api/v1/commits/not-a-hash") { response async in
                    #expect(response.status == .badRequest)
                }
                try await app.testing().test(.GET, "/api/v1/commits/ffffffffffff") {
                    response async in
                    #expect(response.status == .notFound)
                }
                // Prefix ambiguity cannot silently pick the first row.
                for suffix in ["1", "2"] {
                    let oid = "abcdef0" + String(repeating: "0", count: 32) + suffix
                    _ = try await db.query(
                        "INSERT INTO mainline_commits(oid,subject,published) VALUES(\(oid),'ambiguity fixture',true)",
                        logger: logger)
                }
                try await app.testing().test(.GET, "/api/v1/commits/abcdef0") { response async in
                    #expect(response.status == .conflict)
                }

                // Interrupted indexing does not publish a successful cursor;
                // retry reuses persisted commits and completes safely.
                let gate = MainlineTestLeaseGate()
                do {
                    try await service.run(logger: logger) { try await gate.check() }
                    Issue.record("Expected injected lease failure")
                } catch MainlineTestError.interrupted {}
                #expect(try await repository.indexStatus(logger: logger).completed == false)
                try await service.run(logger: logger)
                #expect(try await repository.indexStatus(logger: logger).completed == true)

                // A rewrite cannot quietly keep advertising confirmed results.
                _ = try await fixture.git.run(["update-ref", "refs/heads/master", fixture.baseOID])
                do {
                    try await service.run(logger: logger)
                    Issue.record("Expected non-fast-forward rejection")
                } catch MainlineIndexError.nonFastForward {}
                statuses = try await repository.statuses(
                    patchsetIDs: [v1.patchsetID], logger: logger)
                #expect(statuses[v1.patchsetID]?.state == "not_checked")
                #expect(try await repository.indexStatus(logger: logger).lastError != nil)
            } catch {
                _ = try? await db.query(
                    "DELETE FROM threads WHERE id=ANY(\(threadIDs))", logger: logger)
                try? await clearIndex(app)
                throw error
            }
            _ = try await db.query("DELETE FROM threads WHERE id=ANY(\(threadIDs))", logger: logger)
            try await clearIndex(app)
        }
    }

    @Test("Interrupted expansion cannot publish staged commits under the old base")
    func interruptedExpansion() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let repository = MainlineReadRepository(client: app.postgres)
            let original = MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.13")
            let expanded = MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12")
            let cached = try await addSeries(
                app, ids: [fixture.sourceMessageID], diffs: [fixture.firstDiff])
            try await original.run(logger: app.logger)
            var checkedAt: Date?
            let cachedRows = try await app.postgres.query(
                "SELECT checked_at FROM mainline_patch_fingerprints f JOIN patches p ON p.id=f.patch_id WHERE p.patchset_id=\(cached.patchsetID)",
                logger: app.logger)
            for try await row in cachedRows { checkedAt = try row.decode(Date.self) }
            #expect(checkedAt != nil)
            do {
                try await expanded.run(logger: app.logger) {
                    let rows = try await app.postgres.query(
                        "SELECT count(*) FROM mainline_commits WHERE NOT published",
                        logger: app.logger)
                    for try await row in rows where try row.decode(Int64.self) > 0 {
                        throw MainlineTestError.interrupted
                    }
                }
                Issue.record("Expected interruption after older commits were persisted")
            } catch MainlineTestError.interrupted {}
            let before = try await repository.indexStatus(logger: app.logger)
            #expect(!before.completed)
            #expect(before.baseRef == "v6.13")
            do {
                try await original.run(logger: app.logger)
                Issue.record("Restoring the old base must reject pending expansion")
            } catch is MainlineIndexError {}
            #expect(try await repository.indexStatus(logger: app.logger).completed == false)
            let staged = try await app.postgres.query(
                "SELECT count(*) FROM mainline_commits WHERE NOT published", logger: app.logger)
            for try await row in staged { #expect(try row.decode(Int64.self) == 3) }
            try await expanded.run(logger: app.logger)
            let restored = try #require(
                try await repository.commit(prefix: fixture.firstOID, logger: app.logger))
            #expect(restored.firstRelease == "v6.13")
            #expect(try await repository.indexStatus(logger: app.logger).baseRef == "v6.12")
            #expect(restored.submissions.contains { $0.patchsetId == cached.patchsetID })
            let reused = try await app.postgres.query(
                "SELECT checked_at FROM mainline_patch_fingerprints f JOIN patches p ON p.id=f.patch_id WHERE p.patchset_id=\(cached.patchsetID)",
                logger: app.logger)
            for try await row in reused { #expect(try row.decode(Date.self) == checkedAt) }
            _ = try await app.postgres.query(
                "DELETE FROM threads WHERE id=\(cached.threadID)", logger: app.logger)
            try await clearIndex(app)
        }
    }

    @Test("Advancing-tip interruption resumes unpublished commit batches")
    func advancingTipResume() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let service = MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12")
            let repository = MainlineReadRepository(client: app.postgres)
            try await service.run(logger: app.logger)
            let tree = try await fixture.git.run(["rev-parse", "\(fixture.secondOID)^{tree}"])
                .trimmingCharacters(in: .whitespacesAndNewlines)
            var tip = fixture.secondOID
            for index in 0..<130 {
                tip = try await fixture.git.run([
                    "commit-tree", tree, "-p", tip, "-m", "new commit \(index)",
                ]).trimmingCharacters(in: .whitespacesAndNewlines)
            }
            _ = try await fixture.git.run(["update-ref", "refs/heads/master", tip])
            do {
                try await service.run(logger: app.logger) {
                    let rows = try await app.postgres.query(
                        "SELECT count(*) FROM mainline_commits WHERE NOT published",
                        logger: app.logger)
                    for try await row in rows where try row.decode(Int64.self) >= 128 {
                        throw MainlineTestError.interrupted
                    }
                }
                Issue.record("Expected failure after the first new batch")
            } catch MainlineTestError.interrupted {}
            #expect(
                try await repository.indexStatus(logger: app.logger).indexedTip == fixture.secondOID
            )
            let count = try await app.postgres.query(
                "SELECT count(*) FROM mainline_commits WHERE NOT published", logger: app.logger)
            for try await row in count { #expect(try row.decode(Int64.self) == 128) }
            try await service.run(logger: app.logger)
            #expect(try await repository.indexStatus(logger: app.logger).indexedTip == tip)
            let final = try await app.postgres.query(
                "SELECT count(*) FROM mainline_commits WHERE published", logger: app.logger)
            for try await row in final { #expect(try row.decode(Int64.self) == 134) }
            #expect(try await repository.commit(prefix: tip, logger: app.logger) != nil)
            try await clearIndex(app)
        }
    }

    @Test("Release attribution works when the base is on a side branch")
    func sideBranchBase() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let side = try await fixture.git.run([
                "commit-tree", "\(fixture.baseOID)^{tree}", "-p", fixture.baseOID, "-m",
                "side branch base",
            ]).trimmingCharacters(in: .whitespacesAndNewlines)
            let tip = try await fixture.git.run([
                "commit-tree", "\(fixture.secondOID)^{tree}", "-p", fixture.secondOID, "-p", side,
                "-m",
                "merge later side branch",
            ]).trimmingCharacters(in: .whitespacesAndNewlines)
            _ = try await fixture.git.run(["update-ref", "refs/heads/master", tip])
            _ = try await fixture.git.run(["tag", "v6.14", tip])
            try await MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: side
            ).run(logger: app.logger)
            let repository = MainlineReadRepository(client: app.postgres)
            let first = try #require(
                try await repository.commit(prefix: fixture.firstOID, logger: app.logger))
            #expect(first.firstRelease == "v6.13")
            let second = try #require(
                try await repository.commit(prefix: fixture.secondOID, logger: app.logger))
            #expect(second.firstRelease == "v6.14")
            #expect(try await repository.commit(prefix: side, logger: app.logger) == nil)
            try await clearIndex(app)
        }
    }

    @Test("Missing-parent placeholders retain external commit references")
    func placeholderReference() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let series = try await addSeries(
                app, ids: [fixture.externalMessageID], diffs: [fixture.secondDiff])
            _ = try await app.postgres.query(
                "DELETE FROM patches WHERE patchset_id=\(series.patchsetID)", logger: app.logger)
            _ = try await app.postgres.query(
                "UPDATE messages SET is_placeholder=true WHERE message_id=\(fixture.externalMessageID)",
                logger: app.logger)
            try await MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12"
            ).run(logger: app.logger)
            let repository = MainlineReadRepository(client: app.postgres)
            let result = try #require(
                try await repository.commit(prefix: fixture.firstOID, logger: app.logger))
            #expect(result.references.contains(fixture.externalMessageID))
            _ = try await app.postgres.query(
                "UPDATE messages SET is_placeholder=false WHERE message_id=\(fixture.externalMessageID)",
                logger: app.logger)
            let imported = try #require(
                try await repository.commit(prefix: fixture.firstOID, logger: app.logger))
            #expect(!imported.references.contains(fixture.externalMessageID))
            _ = try await app.postgres.query(
                "DELETE FROM threads WHERE id=\(series.threadID)", logger: app.logger)
            try await clearIndex(app)
        }
    }

    @Test("Readers withhold provisional tag-only results and use a consistent snapshot")
    func readerPublicationBoundary() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let service = MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12")
            let repository = MainlineReadRepository(client: app.postgres)
            try await service.run(logger: app.logger)
            let late = try await addSeries(
                app, ids: ["snapshot-\(fixture.token)@test"], diffs: [fixture.secondDiff])
            let fingerprint = try #require(try await fixture.git.patchID(diff: fixture.secondDiff))
            // Block the evidence query after the state query, then let a writer
            // change state and insert a fingerprint without publishing matches.
            let reader = try await app.postgres.withTransaction(logger: app.logger) { blocker in
                _ = try await blocker.query(
                    "LOCK TABLE mainline_patch_fingerprints IN ACCESS EXCLUSIVE MODE",
                    logger: app.logger)
                let task = Task {
                    try await repository.statuses(
                        patchsetIDs: [late.patchsetID], logger: app.logger)
                }
                do {
                    try await waitForBlockedReader(app)
                    _ = try await app.postgres.query(
                        "UPDATE mainline_index_state SET completed=false", logger: app.logger)
                    _ = try await blocker.query(
                        """
                        INSERT INTO mainline_patch_fingerprints(patch_id,content_fingerprint,stable_patch_id,matcher_version)
                        SELECT id,md5(diff),\(fingerprint),1 FROM patches WHERE patchset_id=\(late.patchsetID)
                        """, logger: app.logger)
                } catch {
                    task.cancel()
                    throw error
                }
                return task
            }
            let snapshot = try await reader.value
            #expect(snapshot[late.patchsetID]?.state == "not_checked")
            #expect(snapshot[late.patchsetID]?.matchedParts == 0)
            try await service.run(logger: app.logger)
            // Simulate a tag-only batch having updated an already-published
            // commit. completed=false without last_error also models a crash.
            _ = try await app.postgres.query(
                "UPDATE mainline_index_state SET completed=false,last_error=NULL",
                logger: app.logger)
            _ = try await app.postgres.query(
                "UPDATE mainline_commits SET first_release='v6.14' WHERE oid=\(fixture.secondOID)",
                logger: app.logger)
            let provisional = try await repository.statuses(
                patchsetIDs: [late.patchsetID], logger: app.logger)
            #expect(provisional[late.patchsetID]?.state == "not_checked")
            #expect(provisional[late.patchsetID]?.matchedParts == 0)
            #expect(provisional[late.patchsetID]?.firstRelease == nil)
            #expect(provisional[late.patchsetID]?.patches.allSatisfy { $0.commits.isEmpty } == true)
            try await app.testing().test(.GET, "/api/v1/commits/\(fixture.secondOID)") {
                response async in
                #expect(response.status == .serviceUnavailable)
            }
            _ = try await app.postgres.query(
                "DELETE FROM threads WHERE id=\(late.threadID)", logger: app.logger)
            try await clearIndex(app)
        }
    }

    @Test("Rename and rename-with-edit mail match in both diff presentations")
    func renameSubmissions() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let original = (0..<30).map { "int value_\($0) = \($0);\n" }.joined()
        let edited = original.replacingOccurrences(of: "value_17 = 17", with: "value_17 = 42")
        var parent = fixture.secondOID
        var commits: [String] = []
        for (index, entry) in [("old.c", original), ("renamed.c", original), ("edited.c", edited)]
            .enumerated()
        {
            let blob = try await fixture.git.run(
                ["hash-object", "-w", "--stdin"], input: Data(entry.1.utf8)
            ).trimmingCharacters(in: .whitespacesAndNewlines)
            let tree = try await fixture.git.run(
                ["mktree"], input: Data("100644 blob \(blob)\t\(entry.0)\n".utf8)
            ).trimmingCharacters(in: .whitespacesAndNewlines)
            parent = try await fixture.git.run([
                "commit-tree", tree, "-p", parent, "-m",
                "rename fixture \(index)\n\nLink: https://lore.kernel.org/rename-\(index)-\(fixture.token)@test",
            ]).trimmingCharacters(in: .whitespacesAndNewlines)
            if index > 0 { commits.append(parent) }
        }
        _ = try await fixture.git.run(["update-ref", "refs/heads/master", parent])
        _ = try await fixture.git.run(["tag", "v6.14", parent])
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            var series: [(Int64, Int64, String, String)] = []
            for (index, oid) in commits.enumerated() {
                for renames in [true, false] {
                    let diff = try await fixture.git.run([
                        "format-patch", "--stdout", "-1",
                        renames ? "--find-renames=50%" : "--no-renames", oid,
                    ])
                    #expect(diff.contains("rename from ") == renames)
                    let id =
                        renames
                        ? "rename-\(index + 1)-\(fixture.token)@test"
                        : "alternate-\(index)-\(fixture.token)@test"
                    let added = try await addSeries(app, ids: [id], diffs: [diff])
                    series.append(
                        (
                            added.threadID, added.patchsetID, oid,
                            renames ? "submission" : "equivalent"
                        ))
                }
            }
            try await MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12"
            ).run(logger: app.logger)
            let repository = MainlineReadRepository(client: app.postgres)
            let statuses = try await repository.statuses(
                patchsetIDs: series.map { $0.1 }, logger: app.logger)
            for (thread, patchset, oid, kind) in series {
                #expect(statuses[patchset]?.state == "merged_released")
                #expect(statuses[patchset]?.firstRelease == "v6.14")
                #expect(statuses[patchset]?.patches.first?.commits.map(\.oid) == [oid])
                #expect(statuses[patchset]?.patches.first?.commits.first?.matchKind == kind)
                let reverse = try #require(
                    try await repository.commit(prefix: oid, logger: app.logger))
                #expect(
                    reverse.submissions.contains {
                        $0.patchsetId == patchset && $0.matchKind == kind
                    })
                _ = try await app.postgres.query(
                    "DELETE FROM threads WHERE id=\(thread)", logger: app.logger)
            }
            try await clearIndex(app)
        }
    }

    @Test("Concurrent fingerprint batches finish before publication and reuse cached null results")
    func fingerprintPipeline() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let diffs = (0..<600).map { $0 % 5 == 0 ? "no diff" : fixture.firstDiff }
            let series = try await addSeries(
                app, ids: (0..<600).map { "batch-\($0)-\(fixture.token)@test" }, diffs: diffs)
            let service = MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12")
            try await service.run(logger: app.logger)
            let rows = try await app.postgres.query(
                "SELECT count(*),count(stable_patch_id),max(checked_at) FROM mainline_patch_fingerprints",
                logger: app.logger)
            var checkedAt: Date?
            for try await row in rows {
                let c = Array(row)
                #expect(try c[0].decode(Int64.self) == 600)
                #expect(try c[1].decode(Int64.self) == 480)
                checkedAt = try c[2].decode(Date.self)
            }
            let matches = try await app.postgres.query(
                "SELECT count(*) FROM mainline_patch_matches WHERE commit_oid=\(fixture.firstOID)",
                logger: app.logger)
            for try await row in matches { #expect(try row.decode(Int64.self) == 480) }
            try await service.run(logger: app.logger)
            let reused = try await app.postgres.query(
                "SELECT max(checked_at) FROM mainline_patch_fingerprints", logger: app.logger)
            for try await row in reused { #expect(try row.decode(Date.self) == checkedAt) }
            _ = try await app.postgres.query(
                "DELETE FROM threads WHERE id=\(series.threadID)", logger: app.logger)
            try await clearIndex(app)
        }
    }

    @Test("Batch persistence rechecks a diff changed while waiting for its row lock")
    func fingerprintWriteRace() async throws {
        let fixture = try await MainlineGitFixture()
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try await withApp(configure: configure) { app in
            try await clearIndex(app)
            let series = try await addSeries(
                app, ids: [fixture.sourceMessageID], diffs: [fixture.firstDiff])
            let service = MainlineIndexService(
                client: app.postgres, repositoryPath: fixture.directory.path, baseRef: "v6.12")
            let rows = try await app.postgres.query(
                "SELECT id,md5(diff) FROM patches WHERE patchset_id=\(series.patchsetID)",
                logger: app.logger)
            var values: [MainlineIndexService.PatchFingerprint] = []
            let stable = try await fixture.git.patchID(diff: fixture.firstDiff)
            for try await row in rows {
                let c = Array(row)
                values.append(
                    .init(
                        id: try c[0].decode(Int64.self), content: try c[1].decode(String.self),
                        stable: stable))
            }
            let batch = values
            let writer = try await app.postgres.withTransaction(logger: app.logger) { blocker in
                _ = try await blocker.query(
                    "SELECT id FROM patches WHERE patchset_id=\(series.patchsetID) FOR UPDATE",
                    logger: app.logger)
                let task = Task { try await service.persistFingerprints(batch, logger: app.logger) }
                var blocked = false
                for _ in 0..<200 {
                    let waits = try await app.postgres.query(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%jsonb_to_recordset%')",
                        logger: app.logger)
                    for try await row in waits { blocked = try row.decode(Bool.self) }
                    if blocked { break }
                    try await Task.sleep(for: .milliseconds(10))
                }
                #expect(blocked)
                _ = try await blocker.query(
                    "UPDATE patches SET diff=\(fixture.secondDiff) WHERE patchset_id=\(series.patchsetID)",
                    logger: app.logger)
                return task
            }
            try await writer.value
            let stale = try await app.postgres.query(
                "SELECT count(*) FROM mainline_patch_fingerprints", logger: app.logger)
            for try await row in stale { #expect(try row.decode(Int64.self) == 0) }
            try await service.run(logger: app.logger)
            let repository = MainlineReadRepository(client: app.postgres)
            let result = try await repository.statuses(
                patchsetIDs: [series.patchsetID], logger: app.logger)
            #expect(
                result[series.patchsetID]?.patches.first?.commits.first?.oid == fixture.secondOID)
            _ = try await app.postgres.query(
                "DELETE FROM threads WHERE id=\(series.threadID)", logger: app.logger)
            try await clearIndex(app)
        }
    }

    private func waitForBlockedReader(_ app: Application) async throws {
        for _ in 0..<200 {
            let rows = try await app.postgres.query(
                """
                SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='mainline_patch_fingerprints'::regclass
                    AND mode='AccessShareLock' AND NOT granted)
                """, logger: app.logger)
            for try await row in rows where try row.decode(Bool.self) { return }
            try await Task.sleep(for: .milliseconds(10))
        }
        throw MainlineTestError.readerDidNotBlock
    }

    private func clearIndex(_ app: Application) async throws {
        _ = try await app.postgres.query(
            "TRUNCATE mainline_patch_matches,mainline_patch_fingerprints,mainline_commit_references,mainline_commits,mainline_releases,mainline_index_state",
            logger: app.logger)
    }

    private func addSeries(_ app: Application, ids: [String], diffs: [String], total: Int? = nil)
        async throws -> (threadID: Int64, patchsetID: Int64)
    {
        try await app.postgres.withTransaction(logger: app.logger) { connection in
            let threadRows = try await connection.query(
                "INSERT INTO threads(root_message_id,subject,last_updated_at) VALUES(\(ids[0]),'mainline fixture',now()) RETURNING id",
                logger: app.logger)
            var threadID: Int64 = 0
            for try await row in threadRows { threadID = try row.decode(Int64.self) }
            let count = total ?? ids.count
            let status = count == ids.count ? "Complete" : "Incomplete"
            let seriesRows = try await connection.query(
                "INSERT INTO patchsets(thread_id,subject,status,total_parts,received_parts) VALUES(\(threadID),'mainline fixture',\(status),\(count),\(ids.count)) RETURNING id",
                logger: app.logger)
            var seriesID: Int64 = 0
            for try await row in seriesRows { seriesID = try row.decode(Int64.self) }
            for (offset, id) in ids.enumerated() {
                _ = try await connection.query(
                    "INSERT INTO messages(message_id,thread_id,subject) VALUES(\(id),\(threadID),\("[PATCH] fixture \(offset + 1)"))",
                    logger: app.logger)
                _ = try await connection.query(
                    "INSERT INTO patches(patchset_id,message_id,part_index,diff) VALUES(\(seriesID),\(id),\(offset + 1),\(diffs[offset]))",
                    logger: app.logger)
            }
            return (threadID, seriesID)
        }
    }
}

private enum MainlineTestError: Error { case interrupted, readerDidNotBlock }
private actor MainlineTestLeaseGate {
    var calls = 0
    func check() throws {
        calls += 1
        if calls == 2 { throw MainlineTestError.interrupted }
    }
}

private struct MainlineGitFixture {
    let directory: URL
    let git: MainlineGit
    let token = UUID().uuidString
    let baseOID: String
    let firstOID: String
    let secondOID: String
    let firstDiff: String
    let secondDiff: String
    var oldMessageID: String { "old-\(token)@test" }
    var sourceMessageID: String { "source-\(token)@test" }
    var unrelatedMessageID: String { "unrelated-\(token)@test" }
    var externalMessageID: String { "external-\(token)@test" }

    init() async throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "nexus-mainline-fixture-\(UUID())")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        git = MainlineGit(repositoryPath: directory.path)
        _ = try await git.run(["init", "--bare", directory.path])
        _ = try await git.run(["config", "user.name", "Mainline test"])
        _ = try await git.run(["config", "user.email", "mainline@test"])
        let empty = try await git.run(["mktree"], input: Data()).trimmingCharacters(
            in: .whitespacesAndNewlines)
        baseOID = try await git.run(["commit-tree", empty, "-m", "base"]).trimmingCharacters(
            in: .whitespacesAndNewlines)
        _ = try await git.run(["tag", "v6.12", baseOID])
        let firstBlob = try await git.run(
            ["hash-object", "-w", "--stdin"], input: Data("first\n".utf8)
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let firstTree = try await git.run(
            ["mktree"], input: Data("100644 blob \(firstBlob)\tfirst.c\n".utf8)
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let source = "source-\(token)@test"
        let unrelated = "unrelated-\(token)@test"
        let external = "external-\(token)@test"
        firstOID = try await git.run([
            "commit-tree", firstTree, "-p", baseOID, "-m",
            "first change\n\nLink: https://lore.kernel.org/r/\(source)\nLink: https://lore.kernel.org/lkml/\(unrelated)/\nLink: https://lore.kernel.org/\(external)",
        ]).trimmingCharacters(in: .whitespacesAndNewlines)
        let trunk = try await git.run(["commit-tree", empty, "-p", baseOID, "-m", "trunk"])
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let merge = try await git.run([
            "commit-tree", firstTree, "-p", trunk, "-p", firstOID, "-m", "merge subsystem",
        ]).trimmingCharacters(in: .whitespacesAndNewlines)
        _ = try await git.run(["tag", "v6.13-rc1", merge])
        _ = try await git.run(["tag", "-a", "v6.13", merge, "-m", "final release"])
        let secondBlob = try await git.run(
            ["hash-object", "-w", "--stdin"], input: Data("second\n".utf8)
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let secondTree = try await git.run(
            ["mktree"],
            input: Data(
                "100644 blob \(firstBlob)\tfirst.c\n100644 blob \(secondBlob)\tsecond.c\n".utf8)
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        secondOID = try await git.run([
            "commit-tree", secondTree, "-p", merge, "-m", "second change",
        ]).trimmingCharacters(in: .whitespacesAndNewlines)
        _ = try await git.run(["update-ref", "refs/heads/master", secondOID])
        _ = try await git.run(["tag", "v6.14-rc1", secondOID])
        _ = try await git.run(["tag", "v6.13.1", secondOID])
        firstDiff = try await git.run(["show", "--format=", firstOID])
        secondDiff = try await git.run(["show", "--format=", secondOID])
    }
}
