@testable import NexusKb
import Foundation
import PostgresNIO
import Vapor
import VaporTesting
import Testing

@Suite("Read API integration tests", .serialized)
struct ReadAPIIntegrationTests {
    @Test("List filtering preserves reply membership and both pagination directions")
    func mailingListPagination() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await ReadAPIIntegrationFixture(app: app)
            let excluded = try await ReadAPIIntegrationFixture(app: app)
            // Include a quote to ensure list names remain bound parameters.
            let group = "read-api-'\(UUID().uuidString)"
            let otherGroup = "other-\(group)"
            do {
                let lists = try await app.postgres.query(
                    """
                    INSERT INTO mailing_lists (name, archive_group)
                    VALUES ('Selected', \(group)), ('Other', \(otherGroup))
                    """, logger: app.logger
                )
                for try await _ in lists {}
                let reply = try await app.postgres.query(
                    """
                    INSERT INTO messages (message_id, thread_id, body)
                    VALUES (\("reply-\(fixture.rootMessageID)"), \(fixture.threadID), 'Reply')
                    """, logger: app.logger
                )
                for try await _ in reply {}
                let links = try await app.postgres.query(
                    """
                    INSERT INTO messages_mailing_lists (message_id, mailing_list_id)
                    SELECT message.id, list.id
                    FROM messages AS message
                    CROSS JOIN mailing_lists AS list
                    WHERE (list.archive_group = \(group)
                        AND (message.message_id = \("reply-\(fixture.rootMessageID)")
                            OR message.thread_id = \(fixture.companionThreadID)))
                       OR (list.archive_group = \(otherGroup)
                        AND message.thread_id = \(excluded.threadID))
                    """, logger: app.logger
                )
                for try await _ in links {}

                let encodedGroup = try #require(group.addingPercentEncoding(
                    withAllowedCharacters: .urlQueryAllowed
                ))
                var url = "/api/v1/threads?limit=1&mailingList=\(encodedGroup)"
                for pageIndex in 0..<3 {
                    try await app.testing().test(.GET, url) { response async throws in
                        #expect(response.status == .ok)
                        let page = try response.content.decode(ThreadListView.self)
                        #expect(page.items.count == 1)
                        if pageIndex == 1 {
                            #expect(page.items.first?.rootMessageId == fixture.companionRootMessageID)
                            #expect(page.items.first?.mailingLists.map(\.archiveGroup) == [group])
                            #expect(page.pagination.nextCursor == nil)
                        } else {
                            #expect(page.items.first?.rootMessageId == fixture.rootMessageID)
                            #expect(page.pagination.previousCursor == nil)
                        }
                        if pageIndex < 2 {
                            let cursor = try #require(pageIndex == 0
                                ? page.pagination.nextCursor : page.pagination.previousCursor)
                            url = "/api/v1/threads?cursor=\(cursor)&mailingList=\(encodedGroup)"
                        }
                    }
                }
                try await app.testing().test(.GET, "/api/v1/threads?limit=10") { response async throws in
                    let page = try response.content.decode(ThreadListView.self)
                    #expect(page.items.contains { $0.rootMessageId == fixture.rootMessageID })
                    #expect(page.items.contains { $0.rootMessageId == excluded.rootMessageID })
                }
                try await app.testing().test(
                    .GET, "/api/v1/threads?mailingList=missing-\(UUID().uuidString)"
                ) { response async throws in
                    #expect(response.status == .ok)
                    let page = try response.content.decode(ThreadListView.self)
                    #expect(page.items.isEmpty)
                }
            } catch {
                try? await fixture.remove()
                try? await excluded.remove()
                throw error
            }
            try await fixture.remove()
            try await excluded.remove()
            let removed = try await app.postgres.query(
                "DELETE FROM mailing_lists WHERE archive_group IN (\(group), \(otherGroup))",
                logger: app.logger
            )
            for try await _ in removed {}
        }
    }

    @Test("Message patch lookup preserves cover, patch, reply and ambiguous matches")
    func messagePatchLookup() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await ReadAPIIntegrationFixture(app: app)
            do {
                let root = fixture.rootMessageID
                let patch = "patch-\(root)"
                let reply = "reply-\(root)"
                let single = "single-\(root)"
                let rows = try await app.postgres.query(
                    """
                    INSERT INTO messages (message_id, thread_id, body, sent_at)
                    VALUES
                        (\(patch), \(fixture.threadID), 'Patch body', '2100-01-01T00:00:01Z'),
                        (\(reply), \(fixture.threadID), 'Reply body', '2100-01-01T00:00:02Z'),
                        (\(single), \(fixture.threadID), 'Single body', '2100-01-01T00:00:03Z')
                    """, logger: app.logger
                )
                for try await _ in rows {}

                // The oldest series must win even when the patch also covers
                // a newer series. A single patch can be its own cover letter.
                for (cover, total, member, position) in [
                    (root, 3, patch, 2),
                    (single, 1, single, 1),
                    (patch, 9, "", 0)
                ] {
                    let series = try await app.postgres.query(
                        """
                        INSERT INTO patchsets (thread_id, cover_letter_message_id, total_parts)
                        VALUES (\(fixture.threadID), \(cover), \(total))
                        RETURNING id
                        """, logger: app.logger
                    )
                    for try await row in series {
                        let id = try row.decode(Int64.self)
                        if !member.isEmpty {
                            let inserted = try await app.postgres.query(
                                """
                                INSERT INTO patches (patchset_id, message_id, part_index, diff)
                                VALUES (\(id), \(member), \(position), 'diff')
                                """, logger: app.logger
                            )
                            for try await _ in inserted {}
                        }
                    }
                }

                let encoded = try #require(root.addingPercentEncoding(
                    withAllowedCharacters: .readAPIPathAllowed
                ))
                try await app.testing().test(
                    .GET, "/api/v1/threads/\(encoded)/messages?limit=200"
                ) { response async throws in
                    #expect(response.status == .ok)
                    #expect(response.headers.first(name: "Server-Timing") != nil)
                    let value = try response.content.decode(ThreadMessagesView.self)
                    #expect(value.items.map(\.messageId) == [root, patch, reply, single])
                    #expect(value.items.map(\.body) == ["Fixture body", "Patch body", "Reply body", "Single body"])
                    #expect(value.items.map { $0.patch?.partIndex } == [0, 2, nil, 1])
                    #expect(value.items.map { $0.patch?.totalParts } == [3, 3, nil, 1])
                }

                var url = "/api/v1/threads/\(encoded)/messages?limit=2"
                for pageIndex in 0..<3 {
                    try await app.testing().test(.GET, url) { response async throws in
                        #expect(response.status == .ok)
                        let page = try response.content.decode(ThreadMessagesView.self)
                        let isLastPage = pageIndex == 1
                        #expect(page.items.map(\.messageId) == (isLastPage ? [reply, single] : [root, patch]))
                        #expect(page.items.map { $0.patch?.partIndex } == (isLastPage ? [nil, 1] : [0, 2]))
                        if pageIndex < 2 {
                            let cursor = try #require(isLastPage
                                ? page.pagination.previousCursor
                                : page.pagination.nextCursor)
                            url = "/api/v1/threads/\(encoded)/messages?cursor=\(cursor)"
                        }
                    }
                }
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Read endpoints execute against Postgres")
    func readEndpoints() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture =
                try await ReadAPIIntegrationFixture(
                    app: app
                )

            do {
            var firstThread: ThreadSummaryView?
            var firstPage: ThreadListView?
            var firstMessage: MessageDetailView?

            try await app.testing().test(
                .GET,
                "/api/v1/threads?limit=1"
            ) { response async throws in
                #expect(response.status == .ok)
                let value = try response.content.decode(
                    ThreadListView.self
                )
                #expect(value.items.count <= 1)
                firstThread = value.items.first
                firstPage = value
            }

            try await app.testing().test(
                .GET,
                "/api/v1/threads?q=\(fixture.searchToken)"
            ) { response async throws in
                #expect(response.status == .badRequest)
                #expect(
                    response.body.string.contains(
                        "Thread search moved"
                    )
                )
            }

            if let firstPage,
               let nextCursor =
                    firstPage.pagination.nextCursor
            {
                var nextPage: ThreadListView?

                try await app.testing().test(
                    .GET,
                    "/api/v1/threads?cursor=\(nextCursor)"
                ) { response async throws in
                    #expect(response.status == .ok)
                    let value = try response.content.decode(
                        ThreadListView.self
                    )
                    #expect(
                        value.pagination.previousCursor
                            != nil
                    )
                    nextPage = value
                }

                if let previousCursor =
                    nextPage?.pagination.previousCursor
                {
                    try await app.testing().test(
                        .GET,
                        "/api/v1/threads?cursor=\(previousCursor)"
                    ) { response async throws in
                        #expect(response.status == .ok)
                        let value = try response.content.decode(
                            ThreadListView.self
                        )
                        #expect(
                            value.items.first?.rootMessageId
                                == firstPage.items.first?
                                    .rootMessageId
                        )
                    }
                }
            }

            try await app.testing().test(
                .GET,
                "/api/v1/mailing-lists"
            ) { response async throws in
                #expect(response.status == .ok)
                _ = try response.content.decode(
                    MailingListCollectionView.self
                )
            }

            try await app.testing().test(
                .GET,
                "/api/v1/subsystems"
            ) { response async throws in
                #expect(response.status == .ok)
                _ = try response.content.decode(
                    SubsystemCollectionView.self
                )
            }

            let requiredThread = try #require(
                firstThread
            )
            #expect(
                requiredThread.rootMessageId
                    == fixture.rootMessageID
            )

            let encoded = try #require(
                requiredThread.rootMessageId
                    .addingPercentEncoding(
                        withAllowedCharacters:
                            .readAPIPathAllowed
                    )
            )

            try await app.testing().test(
                .GET,
                "/api/v1/threads/\(encoded)"
            ) { response async throws in
                #expect(response.status == .ok)
                let value = try response.content.decode(
                    ThreadDetailView.self
                )
                #expect(
                    value.rootMessageId
                        == requiredThread.rootMessageId
                )
            }

            try await app.testing().test(
                .GET,
                "/api/v1/threads/\(encoded)/messages?limit=1"
            ) { response async throws in
                #expect(response.status == .ok)
                let value = try response.content.decode(
                    ThreadMessagesView.self
                )
                #expect(value.items.count <= 1)
                firstMessage = value.items.first
                #expect(
                    firstMessage?.body
                        == "Fixture body"
                )
            }

            if let firstMessage {
                let encodedMessage = try #require(
                    firstMessage.messageId
                        .addingPercentEncoding(
                            withAllowedCharacters:
                                .readAPIPathAllowed
                        )
                )

                try await app.testing().test(
                    .GET,
                    "/api/v1/messages/\(encodedMessage)"
                ) { response async throws in
                    #expect(response.status == .ok)
                    let value = try response.content.decode(
                        MessageDetailView.self
                    )
                    #expect(
                        value.messageId
                            == firstMessage.messageId
                    )
                    #expect(
                        value.rootMessageId
                            == requiredThread.rootMessageId
                    )
                }
            }
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }
}

private final class ReadAPIIntegrationFixture {
    let app: Application
    let rootMessageID: String
    let companionRootMessageID: String
    let searchToken: String
    let threadID: Int64
    let companionThreadID: Int64

    init(app: Application) async throws {
        self.app = app
        self.rootMessageID =
            "read-api-\(UUID().uuidString)@example.com"
        self.searchToken =
            "readapi\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
        let sentAt = Date(
            timeIntervalSince1970:
                4_102_444_800
        )
        let rows = try await app.postgres.query(
            """
            INSERT INTO threads (
                root_message_id,
                subject,
                last_updated_at
            )
            VALUES (
                \(rootMessageID),
                \(searchToken),
                \(sentAt)
            )
            RETURNING id
            """,
            logger: app.logger
        )
        var insertedThreadID: Int64?

        for try await row in rows {
            insertedThreadID = try row.decode(
                Int64.self
            )
        }

        self.threadID = try #require(
            insertedThreadID
        )
        self.companionRootMessageID =
            "read-api-companion-\(UUID().uuidString)@example.com"
        let companionSentAt = sentAt.addingTimeInterval(
            -1
        )
        let companionRows = try await app.postgres.query(
            """
            INSERT INTO threads (
                root_message_id,
                subject,
                last_updated_at
            )
            VALUES (
                \(companionRootMessageID),
                \(searchToken),
                \(companionSentAt)
            )
            RETURNING id
            """,
            logger: app.logger
        )
        var insertedCompanionThreadID: Int64?

        for try await row in companionRows {
            insertedCompanionThreadID = try row.decode(
                Int64.self
            )
        }

        self.companionThreadID = try #require(
            insertedCompanionThreadID
        )
        let messageRows = try await app.postgres.query(
            """
            INSERT INTO messages (
                message_id,
                thread_id,
                author,
                subject,
                sent_at,
                body
            )
            VALUES (
                \(rootMessageID),
                \(threadID),
                'Read API <read-api@example.com>',
                \(searchToken),
                \(sentAt),
                'Fixture body'
            ),
            (
                \(companionRootMessageID),
                \(companionThreadID),
                'Read API <read-api@example.com>',
                \(searchToken),
                \(companionSentAt),
                'Companion fixture body'
            )
            """,
            logger: app.logger
        )

        for try await _ in messageRows {}
    }

    func remove() async throws {
        let rows = try await app.postgres.query(
            """
            DELETE FROM threads
            WHERE id IN (
                \(threadID),
                \(companionThreadID)
            )
            """,
            logger: app.logger
        )

        for try await _ in rows {}
    }
}

private extension CharacterSet {
    static var readAPIPathAllowed: CharacterSet {
        var value = CharacterSet.urlPathAllowed
        value.remove(charactersIn: "/?#%")
        return value
    }
}
