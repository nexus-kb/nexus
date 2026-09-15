@testable import NexusKb
import Foundation
import PostgresNIO
import Queues
import Testing
import Vapor
import VaporTesting

@Suite("Maintenance workflow API tests", .serialized)
struct MaintenanceWorkflowAPITests {
    @Test("Incremental ingest and lineage execute as separate tracked stages")
    func executesIncrementalStages() async throws {
        let archive = try MaintenanceTestArchive()
        _ = try archive.commitPatchMessage()

        try await withApp(configure: configure) { app in
            let fixture = try await MaintenanceAPIFixture(
                app: app,
                archivePath: archive.rootURL.path,
                ownedMessageIDs: [archive.messageID]
            )
            do {
                let ingest = try await trigger(
                    app: app,
                    path:
                        "/api/v1/admin/mailing-lists/\(fixture.archiveGroup)/ingest",
                    mode: "incremental"
                )
                fixture.runIDs.append(ingest.id)
                try await app.queues.queue.worker.run()

                let completedIngest = try await PostgresMaintenanceRepository(
                    client: app.postgres
                ).run(id: ingest.id, logger: app.logger)
                #expect(completedIngest.state == .succeeded)
                #expect(completedIngest.stages.first?.processedItems == 1)
                #expect(try await fixture.messageCount() == 1)
                var searchComponents = URLComponents()
                searchComponents.path = "/api/v1/search"
                searchComponents.queryItems = [
                    URLQueryItem(name: "q", value: archive.searchToken)
                ]
                try await app.testing().test(
                    .GET,
                    try #require(searchComponents.string)
                ) { response async throws in
                    #expect(response.status == .ok)
                    let results = try response.content.decode(
                        MailSearchCollectionView.self
                    )
                    #expect(
                        results.items.contains {
                            $0.rootMessageId == archive.messageID
                        }
                    )
                }
                #expect(try await fixture.lineageWorkCount() == 1)
                #expect(try await fixture.lineageStateCount() == 0)

                let lineage = try await trigger(
                    app: app,
                    path:
                        "/api/v1/admin/mailing-lists/\(fixture.archiveGroup)/patch-lineage",
                    mode: "incremental"
                )
                fixture.runIDs.append(lineage.id)
                try await app.queues.queue.worker.run()

                let completedLineage = try await PostgresMaintenanceRepository(
                    client: app.postgres
                ).run(id: lineage.id, logger: app.logger)
                #expect(completedLineage.state == .succeeded)
                #expect(completedLineage.stages.first?.processedItems == 1)
                #expect(try await fixture.lineageWorkCount() == 0)
                #expect(try await fixture.lineageStateCount() == 1)
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
            #expect(
                try await fixture.ownedArtifactCount()
                    == 0
            )
        }
    }

    @Test("Full ingest resets once and full lineage processes the rebuilt list")
    func executesFullStages() async throws {
        let archive = try MaintenanceTestArchive()
        _ = try archive.commitPatchMessage()

        try await withApp(configure: configure) { app in
            let fixture = try await MaintenanceAPIFixture(
                app: app,
                archivePath: archive.rootURL.path,
                ownedMessageIDs: [archive.messageID]
            )
            do {
                let ingest = try await trigger(
                    app: app,
                    path:
                        "/api/v1/admin/mailing-lists/\(fixture.archiveGroup)/ingest",
                    mode: "full"
                )
                fixture.runIDs.append(ingest.id)
                try await app.queues.queue.worker.run()

                let ingestRun = try await PostgresMaintenanceRepository(
                    client: app.postgres
                ).run(id: ingest.id, logger: app.logger)
                let ingestStage = try #require(ingestRun.stages.first)
                #expect(ingestRun.state == .succeeded)
                #expect(ingestStage.resetCompleted)
                #expect(try await fixture.messageCount() == 1)

                try await PostgresIngestService(client: app.postgres)
                    .resetMailingList(
                        mailingListID: fixture.mailingListID,
                        stageID: ingestStage.id,
                        logger: app.logger
                    )
                #expect(try await fixture.messageCount() == 1)

                let lineage = try await trigger(
                    app: app,
                    path:
                        "/api/v1/admin/mailing-lists/\(fixture.archiveGroup)/patch-lineage",
                    mode: "full"
                )
                fixture.runIDs.append(lineage.id)
                try await app.queues.queue.worker.run()

                let lineageRun = try await PostgresMaintenanceRepository(
                    client: app.postgres
                ).run(id: lineage.id, logger: app.logger)
                #expect(lineageRun.state == .succeeded)
                #expect(lineageRun.stages.first?.processedItems == 1)
                #expect(try await fixture.lineageStateCount() == 1)
                #expect(try await fixture.lineageWorkCount() == 0)
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
            #expect(
                try await fixture.ownedArtifactCount()
                    == 0
            )
        }
    }

    @Test("Manual ingest creates a tracked admin operation")
    func createsManualIngestRun() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await MaintenanceAPIFixture(app: app)

            do {
                try await app.testing().test(
                    .POST,
                    "/api/v1/admin/mailing-lists/\(fixture.archiveGroup)/ingest",
                    beforeRequest: { request in
                        try request.content.encode(["mode": "incremental"])
                    },
                    afterResponse: { response async throws in
                        #expect(response.status == .accepted)
                        let value = try response.content.decode(
                            MaintenanceRunView.self
                        )
                        fixture.runIDs.append(value.id)
                        #expect(value.kind == .ingest)
                        #expect(value.trigger == .operator)
                        #expect(value.state == .queued)
                        #expect(value.stages.count == 1)
                        #expect(value.stages.first?.operation == .ingest)
                        #expect(value.stages.first?.mode == .incremental)
                        #expect(
                            response.headers.first(name: .location)
                                == "/api/v1/admin/operations/\(value.id.uuidString.lowercased())"
                        )

                        try await app.testing().test(
                            .GET,
                            "/api/v1/admin/operations/\(value.id)"
                        ) { showResponse async throws in
                            #expect(showResponse.status == .ok)
                            let shown = try showResponse.content.decode(
                                MaintenanceRunView.self
                            )
                            #expect(shown.id == value.id)
                        }
                    }
                )
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Admin validation and route grouping")
    func validatesAdminRoutes() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await MaintenanceAPIFixture(
                app: app,
                archivePath: nil
            )

            do {
                try await app.testing().test(
                    .POST,
                    "/api/v1/admin/mailing-lists/\(fixture.archiveGroup)/ingest",
                    beforeRequest: { request in
                        try request.content.encode(["mode": "full"])
                    }
                ) { response async in
                    #expect(response.status == .conflict)
                }
                try await app.testing().test(
                    .POST,
                    "/api/v1/mailing-lists/\(fixture.archiveGroup)/ingest"
                ) { response async in
                    #expect(response.status == .notFound)
                }
                try await app.testing().test(
                    .POST,
                    "/api/v1/webhooks/grokmirror"
                ) { response async in
                    #expect(response.status == .notFound)
                }
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Repeated grokmirror hooks create distinct barrier workflows")
    func queuesRepeatedGrokmirrorRuns() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await MaintenanceAPIFixture(app: app)
            var created: [MaintenanceRunView] = []

            do {
                for _ in 0..<2 {
                    try await app.testing().test(
                        .POST,
                        "/api/v1/admin/webhooks/grokmirror"
                    ) { response async throws in
                        #expect(response.status == .accepted)
                        let run = try response.content.decode(
                            MaintenanceRunView.self
                        )
                        fixture.runIDs.append(run.id)
                        created.append(run)
                    }
                }

                #expect(created[0].id != created[1].id)
                let repository = PostgresMaintenanceRepository(
                    client: app.postgres
                )
                let secondRecord = try await repository.run(
                    id: created[1].id,
                    logger: app.logger
                )
                #expect(
                    try await repository.hasEarlierConflict(
                        run: secondRecord,
                        logger: app.logger
                    )
                )
                try await repository.markRunSucceeded(
                    created[0].id,
                    logger: app.logger
                )
                #expect(
                    try await repository.hasEarlierConflict(
                        run: secondRecord,
                        logger: app.logger
                    ) == false
                )
                for run in created {
                    let ingestPositions = run.stages.indices.filter {
                        run.stages[$0].operation == .ingest
                    }
                    let lineagePositions = run.stages.indices.filter {
                        run.stages[$0].operation == .patchLineage
                    }
                    #expect(ingestPositions.count == lineagePositions.count)
                    if let lastIngest = ingestPositions.last,
                       let firstLineage = lineagePositions.first
                    {
                        #expect(lastIngest < firstLineage)
                    }
                }
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    private func trigger(
        app: Application,
        path: String,
        mode: String
    ) async throws -> MaintenanceRunView {
        var value: MaintenanceRunView?
        try await app.testing().test(
            .POST,
            path,
            beforeRequest: { request in
                try request.content.encode(["mode": mode])
            },
            afterResponse: { response async throws in
                #expect(response.status == .accepted)
                value = try response.content.decode(MaintenanceRunView.self)
            }
        )
        return try #require(value)
    }
}

private final class MaintenanceAPIFixture: @unchecked Sendable {
    let app: Application
    let archiveGroup = "maintenance-api-\(UUID().uuidString.lowercased())"
    let mailingListID: Int64
    let ownedMessageIDs: [String]
    var runIDs: [UUID] = []
    private var removed = false

    init(
        app: Application,
        archivePath: String? = "/tmp/nexus-kb-test-archive",
        ownedMessageIDs: [String] = []
    ) async throws {
        self.app = app
        self.ownedMessageIDs = ownedMessageIDs
        let rows = try await app.postgres.query(
            """
            INSERT INTO mailing_lists (name, archive_group, archive_path)
            VALUES (\(archiveGroup), \(archiveGroup), \(archivePath))
            RETURNING id
            """,
            logger: app.logger
        )
        var id: Int64?
        for try await row in rows {
            id = try row.decode(Int64.self)
        }
        mailingListID = try #require(id)
    }

    func remove() async throws {
        guard !removed else { return }
        for runID in runIDs {
            let jobs = try await app.postgres.query(
                """
                DELETE FROM vapor_queue_jobs
                WHERE job_name = 'MaintenanceWorkflowJob'
                  AND convert_from(payload, 'UTF8')::jsonb ->> 'runID'
                        = \(runID.uuidString)
                """,
                logger: app.logger
            )
            for try await _ in jobs {}
            let runs = try await app.postgres.query(
                "DELETE FROM maintenance_runs WHERE id = \(runID)",
                logger: app.logger
            )
            for try await _ in runs {}
        }
        if !ownedMessageIDs.isEmpty {
            let threads = try await app.postgres.query(
                """
                DELETE FROM threads AS thread
                WHERE thread.root_message_id = ANY(
                        \(ownedMessageIDs)::text[]
                      )
                   OR EXISTS (
                    SELECT 1
                    FROM messages AS owned_message
                    WHERE owned_message.thread_id = thread.id
                      AND owned_message.message_id = ANY(
                            \(ownedMessageIDs)::text[]
                      )
                )
                """,
                logger: app.logger
            )
            for try await _ in threads {}
        }
        let lists = try await app.postgres.query(
            "DELETE FROM mailing_lists WHERE id = \(mailingListID)",
            logger: app.logger
        )
        for try await _ in lists {}
        removed = true
    }

    func ownedArtifactCount() async throws -> Int64 {
        guard !ownedMessageIDs.isEmpty else {
            return 0
        }

        return try await count(
            """
            WITH owned_threads AS MATERIALIZED (
                SELECT id
                FROM threads
                WHERE root_message_id = ANY(
                    \(ownedMessageIDs)::text[]
                )

                UNION

                SELECT thread_id
                FROM messages
                WHERE message_id = ANY(
                    \(ownedMessageIDs)::text[]
                )
            )
            SELECT (
                (
                    SELECT count(*)
                    FROM messages
                    WHERE message_id = ANY(
                        \(ownedMessageIDs)::text[]
                    )
                )
                + (SELECT count(*) FROM owned_threads)
                + (
                    SELECT count(*)
                    FROM thread_search_documents AS document
                    JOIN owned_threads AS owned
                      ON owned.id = document.thread_id
                )
            )::bigint
            """
        )
    }

    func messageCount() async throws -> Int64 {
        try await count(
            """
            SELECT count(*)::bigint
            FROM messages_mailing_lists
            WHERE mailing_list_id = \(mailingListID)
            """
        )
    }

    func lineageWorkCount() async throws -> Int64 {
        try await count(
            """
            SELECT count(*)::bigint
            FROM patch_lineage_work_items
            WHERE mailing_list_id = \(mailingListID)
            """
        )
    }

    func lineageStateCount() async throws -> Int64 {
        try await count(
            """
            SELECT count(DISTINCT state.patchset_id)::bigint
            FROM patchset_lineage_state AS state
            JOIN patches AS patch ON patch.patchset_id = state.patchset_id
            JOIN messages AS message ON message.message_id = patch.message_id
            JOIN messages_mailing_lists AS link ON link.message_id = message.id
            WHERE link.mailing_list_id = \(mailingListID)
            """
        )
    }

    private func count(_ query: PostgresQuery) async throws -> Int64 {
        let rows = try await app.postgres.query(query, logger: app.logger)
        for try await row in rows {
            return try row.decode(Int64.self)
        }
        return 0
    }
}

private final class MaintenanceTestArchive: @unchecked Sendable {
    let rootURL: URL
    let messageID: String
    let searchToken: String
    private let repositoryURL: URL

    init() throws {
        let identifier = UUID().uuidString
            .replacingOccurrences(of: "-", with: "")
            .lowercased()
        messageID = "maintenance-workflow-\(identifier)@example.com"
        searchToken = "nexusmaintenance\(identifier)"
        rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "nexus-kb-maintenance-archive-\(UUID().uuidString)",
                isDirectory: true
            )
        repositoryURL = rootURL
            .appendingPathComponent("git", isDirectory: true)
            .appendingPathComponent("0.git", isDirectory: true)
        try FileManager.default.createDirectory(
            at: repositoryURL,
            withIntermediateDirectories: true
        )
        try run(["init", "-b", "master"])
        try run(["config", "user.name", "Nexus KB Test"])
        try run(["config", "user.email", "nexus-kb-test@example.com"])
    }

    deinit {
        try? FileManager.default.removeItem(at: rootURL)
    }

    func commitPatchMessage() throws -> String {
        let message =
            """
            From: Nexus Test <nexus@example.com>
            Message-ID: <\(messageID)>
            Subject: [PATCH] test: \(searchToken) maintenance workflow
            Date: Fri, 21 Aug 2026 12:00:00 -0400

            diff --git a/file b/file
            --- a/file
            +++ b/file
            @@ -1 +1 @@
            -old
            +new
            """
        try Data(message.utf8).write(
            to: repositoryURL.appendingPathComponent("m")
        )
        try run(["add", "--all"])
        try run(["commit", "-m", "patch message"])
        return try text(["rev-parse", "HEAD"])
    }

    private func run(_ arguments: [String]) throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/git")
        process.arguments = ["-C", repositoryURL.path] + arguments
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw CocoaError(.fileWriteUnknown)
        }
    }

    private func text(_ arguments: [String]) throws -> String {
        let process = Process()
        let pipe = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/git")
        process.arguments = ["-C", repositoryURL.path] + arguments
        process.standardOutput = pipe
        process.standardError = FileHandle.nullDevice
        try process.run()
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw CocoaError(.fileReadUnknown)
        }
        return String(decoding: data, as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
