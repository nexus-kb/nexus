@testable import NexusKb
import Foundation
import PostgresNIO
import Testing
import Vapor
import VaporTesting

@Suite("Promoted thread roots")
struct PostgresThreadRootServiceTests {
    @Test("Promotes a sole available child and preserves the old root URL")
    func promotesSoleChild() async throws {
        try await withApp(configure: configure) { app in
            try await withPromotedRootFixture(app: app) { fixture in
                try await PostgresThreadRootService(
                    client: app.postgres
                ).finalizeEligibleRoots(
                    threadIDs: [fixture.threadID],
                    logger: app.logger
                )

                let state = try #require(
                    try await fixture.threadState()
                )
                #expect(state.id == fixture.threadID)
                #expect(state.rootMessageID == fixture.childMessageID)
                #expect(state.promotedFromMessageID == fixture.missingMessageID)
                #expect(state.subject == fixture.childSubject)

                let oldIdentifier = try MessageIdentifier(
                    fixture.missingMessageID
                )
                let repository = PostgresReadRepository(
                    client: app.postgres
                )
                let thread = try #require(
                    try await repository.thread(
                        rootMessageID: oldIdentifier,
                        logger: app.logger
                    )
                )
                #expect(thread.rootMessageID == fixture.childMessageID)
                #expect(thread.missingMessageCount == 0)

                let messages = try #require(
                    try await repository.messages(
                        rootMessageID: oldIdentifier,
                        limit: 10,
                        cursor: nil,
                        logger: app.logger
                    )
                )
                #expect(messages.rootMessageID == fixture.childMessageID)
                #expect(
                    messages.items.map(\.detail.messageID) == [
                        fixture.childMessageID,
                        fixture.descendantMessageID,
                    ]
                )
                #expect(
                    messages.items.first?.detail.inReplyToMessageID
                        == fixture.missingMessageID
                )
            }
        }
    }

    @Test("A later parent reverses promotion without changing the thread")
    func laterParentReversesPromotion() async throws {
        try await withApp(configure: configure) { app in
            try await withPromotedRootFixture(app: app) { fixture in
                let service = PostgresThreadRootService(
                    client: app.postgres
                )

                try await service.finalizeEligibleRoots(
                    threadIDs: [fixture.threadID],
                    logger: app.logger
                )
                try await fixture.resolveMissingParent()
                try await app.postgres.withTransaction(
                    logger: app.logger
                ) { connection in
                    try await service.reconcilePromotions(
                        threadIDs: [fixture.threadID],
                        connection: connection,
                        logger: app.logger
                    )
                }

                let state = try #require(
                    try await fixture.threadState()
                )
                #expect(state.id == fixture.threadID)
                #expect(state.rootMessageID == fixture.missingMessageID)
                #expect(state.promotedFromMessageID == nil)
                #expect(state.subject == "Late parent")
            }
        }
    }

    @Test("Multiple root children prevent and reverse promotion")
    func multipleChildrenPreventPromotion() async throws {
        try await withApp(configure: configure) { app in
            try await withPromotedRootFixture(app: app) { fixture in
                let service = PostgresThreadRootService(
                    client: app.postgres
                )

                try await fixture.insertSibling()
                try await service.finalizeEligibleRoots(
                    threadIDs: [fixture.threadID],
                    logger: app.logger
                )
                #expect(
                    try await fixture.threadState()?
                        .rootMessageID == fixture.missingMessageID
                )

                try await fixture.removeSibling()
                try await service.finalizeEligibleRoots(
                    threadIDs: [fixture.threadID],
                    logger: app.logger
                )
                #expect(
                    try await fixture.threadState()?
                        .rootMessageID == fixture.childMessageID
                )

                try await fixture.insertSibling()
                try await app.postgres.withTransaction(
                    logger: app.logger
                ) { connection in
                    try await service.reconcilePromotions(
                        threadIDs: [fixture.threadID],
                        connection: connection,
                        logger: app.logger
                    )
                }
                let state = try #require(
                    try await fixture.threadState()
                )
                #expect(state.rootMessageID == fixture.missingMessageID)
                #expect(state.promotedFromMessageID == nil)
            }
        }
    }

    @Test("Promotion waits for every mailing list represented in the thread")
    func waitsForEveryMailingList() async throws {
        try await withApp(configure: configure) { app in
            try await withPromotedRootFixture(app: app) { fixture in
                let mailingListIDs = try await fixture
                    .linkToTwoMailingLists()
                let service = PostgresThreadRootService(
                    client: app.postgres
                )

                try await service.finalizeEligibleRoots(
                    threadIDs: [fixture.threadID],
                    mailingListIDs: [mailingListIDs.0],
                    logger: app.logger
                )
                #expect(
                    try await fixture.threadState()?
                        .rootMessageID == fixture.missingMessageID
                )

                try await service.finalizeEligibleRoots(
                    threadIDs: [fixture.threadID],
                    mailingListIDs: [
                        mailingListIDs.0,
                        mailingListIDs.1,
                    ],
                    logger: app.logger
                )
                #expect(
                    try await fixture.threadState()?
                        .rootMessageID == fixture.childMessageID
                )
            }
        }
    }
}

private func withPromotedRootFixture(
    app: Application,
    _ body: (PromotedRootFixture) async throws -> Void
) async throws {
    let fixture = try await PromotedRootFixture(app: app)
    do {
        try await body(fixture)
    } catch {
        try? await fixture.remove()
        throw error
    }
    try await fixture.remove()
}

private struct PromotedThreadState {
    let id: Int64
    let rootMessageID: String
    let promotedFromMessageID: String?
    let subject: String?
}

private final class PromotedRootFixture: @unchecked Sendable {
    let app: Application
    let threadID: Int64
    let missingMessageID: String
    let childMessageID: String
    let descendantMessageID: String
    let siblingMessageID: String
    let childSubject = "First available message"
    private var mailingListIDs: [Int64] = []

    init(app: Application) async throws {
        self.app = app
        let prefix = UUID().uuidString
        missingMessageID = "\(prefix)-missing@example.com"
        childMessageID = "\(prefix)-child@example.com"
        descendantMessageID = "\(prefix)-descendant@example.com"
        siblingMessageID = "\(prefix)-sibling@example.com"

        let rows = try await app.postgres.query(
            """
            INSERT INTO threads (
                root_message_id,
                subject,
                last_updated_at
            ) VALUES (
                \(missingMessageID),
                '(placeholder)',
                now()
            )
            RETURNING id
            """,
            logger: app.logger
        )
        var value: Int64?
        for try await row in rows {
            value = try row.decode(Int64.self)
        }
        threadID = try #require(value)

        let messageRows = try await app.postgres.query(
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
                is_placeholder
            ) VALUES
            (
                \(missingMessageID), \(threadID), NULL,
                ARRAY[]::text[], NULL, '(placeholder)', now(), '', true
            ),
            (
                \(childMessageID), \(threadID), \(missingMessageID),
                ARRAY[\(missingMessageID)]::text[], 'Child',
                \(childSubject), now() + interval '1 second', 'Child body', false
            ),
            (
                \(descendantMessageID), \(threadID), \(childMessageID),
                ARRAY[\(missingMessageID), \(childMessageID)]::text[],
                'Descendant', 'Reply', now() + interval '2 seconds',
                'Reply body', false
            )
            """,
            logger: app.logger
        )
        for try await _ in messageRows {}
    }

    func threadState() async throws -> PromotedThreadState? {
        let rows = try await app.postgres.query(
            """
            SELECT
                id,
                root_message_id,
                promoted_from_message_id,
                subject
            FROM threads
            WHERE id = \(threadID)
            """,
            logger: app.logger
        )
        for try await row in rows {
            let value = try row.decode(
                (Int64, String, String?, String?).self
            )
            return PromotedThreadState(
                id: value.0,
                rootMessageID: value.1,
                promotedFromMessageID: value.2,
                subject: value.3
            )
        }
        return nil
    }

    func resolveMissingParent() async throws {
        let rows = try await app.postgres.query(
            """
            UPDATE messages
            SET
                author = 'Parent',
                subject = 'Late parent',
                sent_at = now() - interval '1 second',
                body = 'Parent body',
                is_placeholder = false
            WHERE message_id = \(missingMessageID)
            """,
            logger: app.logger
        )
        for try await _ in rows {}
    }

    func insertSibling() async throws {
        let rows = try await app.postgres.query(
            """
            INSERT INTO messages (
                message_id,
                thread_id,
                in_reply_to,
                references_ids,
                author,
                subject,
                sent_at,
                body
            ) VALUES (
                \(siblingMessageID), \(threadID), \(missingMessageID),
                ARRAY[\(missingMessageID)]::text[], 'Sibling',
                'Sibling reply', now() + interval '3 seconds', 'Sibling body'
            )
            ON CONFLICT (message_id) DO NOTHING
            """,
            logger: app.logger
        )
        for try await _ in rows {}
    }

    func removeSibling() async throws {
        let rows = try await app.postgres.query(
            "DELETE FROM messages WHERE message_id = \(siblingMessageID)",
            logger: app.logger
        )
        for try await _ in rows {}
    }

    func linkToTwoMailingLists() async throws -> (
        Int64,
        Int64
    ) {
        let archiveGroups = [
            "promoted-root-\(UUID().uuidString)",
            "promoted-root-\(UUID().uuidString)",
        ]
        let rows = try await app.postgres.query(
            """
            INSERT INTO mailing_lists (name, archive_group)
            SELECT value, value
            FROM unnest(\(archiveGroups)::text[]) AS value
            RETURNING id
            """,
            logger: app.logger
        )
        var values: [Int64] = []
        for try await row in rows {
            values.append(try row.decode(Int64.self))
        }
        mailingListIDs = values

        let linkRows = try await app.postgres.query(
            """
            INSERT INTO messages_mailing_lists (
                message_id,
                mailing_list_id
            )
            SELECT message.id, mailing_list_id
            FROM messages AS message
            CROSS JOIN unnest(
                \(values)::bigint[]
            ) AS scoped(mailing_list_id)
            WHERE message.message_id = \(childMessageID)
            """,
            logger: app.logger
        )
        for try await _ in linkRows {}

        return (
            try #require(values.first),
            try #require(values.last)
        )
    }

    func remove() async throws {
        let rows = try await app.postgres.query(
            "DELETE FROM threads WHERE id = \(threadID)",
            logger: app.logger
        )
        for try await _ in rows {}

        guard !mailingListIDs.isEmpty else {
            return
        }
        let mailingListRows = try await app.postgres.query(
            "DELETE FROM mailing_lists WHERE id = ANY(\(mailingListIDs)::bigint[])",
            logger: app.logger
        )
        for try await _ in mailingListRows {}
    }
}
