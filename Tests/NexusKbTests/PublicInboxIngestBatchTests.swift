@testable import NexusKb
import Foundation
import PostgresNIO
import Testing
import Vapor
import VaporTesting

@Suite(
    "Public-inbox database batch tests",
    .serialized
)
struct PublicInboxIngestBatchTests {
    @Test("Repeated commits of the same blob retain earliest clock evidence")
    func preservesSameBlobTimestamp() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let first = try fixture.message(
                    number: 1, dateHeader: "broken",
                    archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_000)
                )
                let later = try fixture.message(
                    number: 1, dateHeader: "broken",
                    archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_600)
                )
                let service = PostgresIngestService(client: app.postgres)
                _ = try await service.ingestBatch(
                    [first], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil, logger: app.logger
                )
                _ = try await service.ingestBatch(
                    [PreparedPublicInboxMessage(commitOID: String(repeating: "f", count: 40),
                                                blobOID: first.blobOID, parsed: later.parsed)],
                    mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: first.commitOID, logger: app.logger
                )
                #expect(try await fixture.threadMetadata(messageID: first.parsed.message.messageID)?
                    .lastUpdatedAt == Date(timeIntervalSince1970: 1_600_000_000))
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Cross-post timestamps converge and refresh derived dates on replay", arguments: [false, true])
    func reconcilesCrossPostTimestamps(laterFirst: Bool) async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let messageID = "\(fixture.prefix)-clock@example.com"
                let early = try fixture.message(
                    number: 1, messageID: messageID, subject: "[PATCH] clock repair",
                    dateHeader: "broken", archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_000)
                )
                let late = try fixture.message(
                    number: 2, messageID: messageID, subject: "[PATCH] clock repair",
                    dateHeader: "broken", archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_300)
                )
                let unknown = try fixture.message(
                    number: 3, messageID: messageID, subject: "[PATCH] clock repair",
                    dateHeader: "broken", archiveTimestamp: Date(timeIntervalSince1970: 0)
                )
                let otherList = try await fixture.createAdditionalMailingList()
                let earlyList = laterFirst ? otherList : fixture.mailingListID
                let lateList = laterFirst ? fixture.mailingListID : otherList
                let first = laterFirst ? late : early
                let second = laterFirst ? early : late
                let service = PostgresIngestService(client: app.postgres)
                _ = try await service.ingestBatch(
                    [first], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil, logger: app.logger
                )
                try await fixture.reconcilePendingLineages()
                let lineage = try #require(try await fixture.lineageState(messageID: messageID))
                _ = try await service.ingestBatch(
                    [second], mailingListID: otherList, epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil, logger: app.logger
                )
                try await fixture.expectPatchTimestampMetadata(messageID: messageID, seconds: 1_600_000_000)

                // Replaying the later copy cannot undo the canonical correction.
                _ = try await service.ingestBatch(
                    [late], mailingListID: lateList, epoch: fixture.epoch,
                    expectedPreviousCommitOID: late.commitOID, logger: app.logger
                )
                try await fixture.expectPatchTimestampMetadata(messageID: messageID, seconds: 1_600_000_000)

                // Replacing the earlier copy with unknown evidence keeps the
                // other list's usable date; replacing both makes all dates nil.
                _ = try await service.ingestBatch(
                    [unknown], mailingListID: earlyList, epoch: fixture.epoch,
                    expectedPreviousCommitOID: early.commitOID, logger: app.logger
                )
                try await fixture.expectPatchTimestampMetadata(messageID: messageID, seconds: 1_600_000_300)
                _ = try await service.ingestBatch(
                    [unknown], mailingListID: lateList, epoch: fixture.epoch,
                    expectedPreviousCommitOID: late.commitOID, logger: app.logger
                )
                try await fixture.expectPatchTimestampMetadata(messageID: messageID, seconds: nil)

                _ = try await service.ingestBatch(
                    [early], mailingListID: earlyList, epoch: fixture.epoch,
                    expectedPreviousCommitOID: unknown.commitOID, logger: app.logger
                )
                try await fixture.expectPatchTimestampMetadata(messageID: messageID, seconds: 1_600_000_000)
                _ = try await service.ingestBatch(
                    [.deletion(commitOID: String(repeating: "f", count: 40), blobOID: early.blobOID)],
                    mailingListID: earlyList, epoch: fixture.epoch,
                    expectedPreviousCommitOID: early.commitOID, logger: app.logger
                )
                try await fixture.expectPatchTimestampMetadata(messageID: messageID, seconds: nil)
                #expect(try await fixture.lineageState(messageID: messageID)?.lineageID == lineage.lineageID)
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Undated archive mail never overrides known thread activity, even after deletion")
    func archiveDatesControlThreadActivity() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let root = try fixture.message(
                    number: 1, dateHeader: "Sun, 13 Sep 2020 12:26:40 +0000",
                    archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_005)
                )
                let unknown = try fixture.message(
                    number: 2, inReplyTo: root.parsed.message.messageID,
                    dateHeader: "Wed, 3 Jan 1990 21:25:00 +0100",
                    archiveTimestamp: Date(timeIntervalSince1970: 631_398_300)
                )
                let future = try fixture.message(
                    number: 3, inReplyTo: root.parsed.message.messageID,
                    dateHeader: "Mon, 18 Jun 2085 15:57:19 +0000",
                    archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_060)
                )
                #expect(unknown.parsed.message.date == nil)
                let service = PostgresIngestService(client: app.postgres)
                _ = try await service.ingestBatch(
                    [root, unknown, future], mailingListID: fixture.mailingListID,
                    epoch: fixture.epoch, expectedPreviousCommitOID: nil, logger: app.logger
                )
                #expect(try await fixture.threadMetadata(messageID: root.parsed.message.messageID)?
                    .lastUpdatedAt == Date(timeIntervalSince1970: 1_600_000_060))
                let deletion = String(repeating: "f", count: 40)
                _ = try await service.ingestBatch(
                    [.deletion(commitOID: deletion, blobOID: future.blobOID)],
                    mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: future.commitOID, logger: app.logger
                )
                #expect(try await fixture.threadMetadata(messageID: root.parsed.message.messageID)?
                    .lastUpdatedAt == Date(timeIntervalSince1970: 1_600_000_000))
                _ = try await service.ingestBatch(
                    [.deletion(commitOID: String(repeating: "e", count: 40), blobOID: root.blobOID)],
                    mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: deletion, logger: app.logger
                )
                #expect(try await fixture.threadMetadata(messageID: unknown.parsed.message.messageID)?
                    .lastUpdatedAt == Date(timeIntervalSince1970: 0))
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Batch commits messages and final cursor")
    func commitsBatchAndCursor() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let first = try fixture.message(
                    number: 1
                )
                let second = try fixture.message(
                    number: 2
                )

                let results = try await PostgresIngestService(
                    client: app.postgres
                ).ingestBatch(
                    [first, second],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                #expect(results.count == 2)
                #expect(
                    try await fixture.cursor()
                        == second.commitOID
                )
                #expect(
                    try await fixture.messageCount()
                        == 2
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Skipped entry advances the cursor without persistence")
    func skippedEntryAdvancesCursor() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let skippedCommitOID = String(
                    repeating: "a",
                    count: 40
                )
                let results = try await PostgresIngestService(
                    client: app.postgres
                ).ingestBatch(
                    [
                        .skipped(
                            commitOID: skippedCommitOID,
                            blobOID: String(
                                repeating: "b",
                                count: 40
                            )
                        )
                    ],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                #expect(results.isEmpty)
                #expect(
                    try await fixture.cursor()
                        == skippedCommitOID
                )
                #expect(
                    try await fixture.messageCount()
                        == 0
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Reply to a skipped message creates a placeholder")
    func replyToSkippedMessageCreatesPlaceholder()
        async throws
    {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let missingMessageID =
                    "missing-root@example.com"
                let reply = try fixture.message(
                    number: 1,
                    inReplyTo: missingMessageID
                )
                let results = try await PostgresIngestService(
                    client: app.postgres
                ).ingestBatch(
                    [
                        .skipped(
                            commitOID: String(
                                repeating: "c",
                                count: 40
                            ),
                            blobOID: String(
                                repeating: "d",
                                count: 40
                            )
                        ),
                        .message(reply),
                    ],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                #expect(results.count == 1)
                #expect(
                    try await fixture.messageState(
                        messageID: missingMessageID
                    )?.isPlaceholder == true
                )
                #expect(
                    try await fixture.cursor()
                        == reply.commitOID
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
            #expect(
                try await fixture.messageState(
                    messageID: "missing-root@example.com"
                ) == nil
            )
        }
    }

    @Test("Failure rolls back every message and cursor")
    func rollsBackBatch() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let first = try fixture.message(
                    number: 1
                )
                let invalid = try fixture.message(
                    number: 2,
                    invalidPatchIndex: true
                )

                do {
                    _ = try await PostgresIngestService(
                        client: app.postgres
                    ).ingestBatch(
                        [first, invalid],
                        mailingListID:
                            fixture.mailingListID,
                        epoch: fixture.epoch,
                        expectedPreviousCommitOID: nil,
                        logger: app.logger
                    )

                    Issue.record(
                        "Expected invalid patch metadata to roll back the batch"
                    )
                } catch let error as PostgresTransactionError {
                    #expect(error.closureError != nil)
                    #expect(error.rollbackError == nil)
                } catch {
                    Issue.record(
                        "Unexpected batch failure: \(error)"
                    )
                }

                #expect(
                    try await fixture.messageCount()
                        == 0
                )
                #expect(
                    try await fixture.cursor()
                        == nil
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Stale expected cursor cannot persist another batch")
    func rejectsStaleCursor() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let first = try fixture.message(
                    number: 1
                )
                let second = try fixture.message(
                    number: 2
                )
                let service = PostgresIngestService(
                    client: app.postgres
                )

                _ = try await service.ingestBatch(
                    [first],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                do {
                    _ = try await service.ingestBatch(
                        [second],
                        mailingListID:
                            fixture.mailingListID,
                        epoch: fixture.epoch,
                        expectedPreviousCommitOID: nil,
                        logger: app.logger
                    )

                    Issue.record(
                        "Expected stale cursor to be rejected"
                    )
                } catch let error as PostgresTransactionError {
                    let ingestError = try #require(
                        error.closureError
                            as? PostgresIngestError
                    )

                    #expect(
                        ingestError == .cursorMismatch(
                            expected: nil,
                            actual: first.commitOID
                        )
                    )
                } catch {
                    Issue.record(
                        "Unexpected stale-cursor error: \(error)"
                    )
                }

                #expect(
                    try await fixture.cursor()
                        == first.commitOID
                )
                #expect(
                    try await fixture.messageCount()
                        == 1
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Deletion retracts an orphan and advances the cursor")
    func retractsDeletedOrphan() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let message = try fixture.message(
                    number: 1
                )
                let deletionCommit =
                    String(repeating: "f", count: 40)
                let service = PostgresIngestService(
                    client: app.postgres
                )

                _ = try await service.ingestBatch(
                    [message],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                _ = try await service.ingestBatch(
                    [
                        .deletion(
                            commitOID: deletionCommit,
                            blobOID: message.blobOID
                        )
                    ],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID:
                        message.commitOID,
                    logger: app.logger
                )

                #expect(
                    try await fixture.cursor()
                        == deletionCommit
                )
                #expect(
                    try await fixture.messageState(
                        messageID:
                            message.parsed.message
                            .messageID
                    ) == nil
                )
                #expect(
                    try await fixture.threadCount() == 0
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Deleted parent becomes a placeholder")
    func deletedParentBecomesPlaceholder() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let parent = try fixture.message(
                    number: 1
                )
                let reply = try fixture.message(
                    number: 2,
                    inReplyTo:
                        parent.parsed.message
                        .messageID
                )
                let deletionCommit =
                    String(repeating: "e", count: 40)
                let service = PostgresIngestService(
                    client: app.postgres
                )

                _ = try await service.ingestBatch(
                    [parent, reply],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                _ = try await service.ingestBatch(
                    [
                        .deletion(
                            commitOID: deletionCommit,
                            blobOID: parent.blobOID
                        )
                    ],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID:
                        reply.commitOID,
                    logger: app.logger
                )

                let parentState = try await fixture
                    .messageState(
                        messageID:
                            parent.parsed.message
                            .messageID
                    )

                #expect(
                    parentState?.isPlaceholder == true
                )
                #expect(
                    try await fixture.mailingListBlobOID(
                        messageID:
                            parent.parsed.message
                            .messageID
                    ) == nil
                )
                #expect(
                    try await fixture.messageState(
                        messageID:
                            reply.parsed.message
                            .messageID
                    )?.isPlaceholder == false
                )
                #expect(
                    try await fixture.cursor()
                        == deletionCommit
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Deleting a patch removes its empty patchset and lineage")
    func deletingPatchRemovesDerivedState() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let message = try fixture.message(
                    number: 1,
                    subject:
                        "[PATCH] \(fixture.prefix): delete me",
                    body:
                        """
                        diff --git a/file b/file
                        --- a/file
                        +++ b/file
                        @@ -1 +1 @@
                        -old
                        +new
                        """
                )
                let service = PostgresIngestService(
                    client: app.postgres
                )

                _ = try await service.ingestBatch(
                    [message],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                try await fixture.reconcilePendingLineages()

                let patchSet = try #require(
                    try await fixture.patchSetState(
                        messageID:
                            message.parsed.message
                            .messageID
                    )
                )
                let lineage = try #require(
                    try await fixture.lineageState(
                        messageID:
                            message.parsed.message
                            .messageID
                    )
                )

                _ = try await service.ingestBatch(
                    [
                        .deletion(
                            commitOID:
                                String(
                                    repeating: "c",
                                    count: 40
                                ),
                            blobOID: message.blobOID
                        )
                    ],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID:
                        message.commitOID,
                    logger: app.logger
                )

                #expect(
                    try await fixture.patchSetExists(
                        id: patchSet.id
                    ) == false
                )
                #expect(
                    try await fixture.lineageExists(
                        id: lineage.lineageID
                    ) == false
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Unanchored patch subjects do not create patchsets")
    func skipsUnanchoredPatchSubject() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let subject =
                    "[PATCH] 4/6 - \(fixture.prefix) no diff"
                let message = try fixture.message(
                    number: 1,
                    subject: subject,
                    body: "No diff was included."
                )

                #expect(
                    message.parsed.patch.isPatchOrCover
                )
                #expect(message.parsed.patch.diff == nil)
                #expect(
                    message.parsed.message.inReplyTo
                        == nil
                )

                _ = try await PostgresIngestService(
                    client: app.postgres
                ).ingestBatch(
                    [message],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                #expect(
                    try await fixture.patchSetCount(
                        subject: subject
                    ) == 0
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Lineage backfill skips legacy anchorless patchsets")
    func lineageBackfillSkipsAnchorlessPatchSet()
        async throws
    {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let message = try fixture.message(
                    number: 1
                )

                _ = try await PostgresIngestService(
                    client: app.postgres
                ).ingestBatch(
                    [message],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                let messageState = try #require(
                    try await fixture.messageState(
                        messageID:
                            message.parsed.message
                            .messageID
                    )
                )
                let sentAt = Date(
                    timeIntervalSince1970:
                        5_000_000_000
                )
                let patchSetID = try await fixture
                    .insertAnchorlessPatchSet(
                        threadID: messageState.threadID,
                        subject:
                            "\(fixture.prefix) legacy patchset",
                        sentAt: sentAt
                    )
                var skipped = false
                try await app.postgres.withTransaction(
                    logger: app.logger
                ) { connection in
                    do {
                        _ = try await PostgresPatchLineageService()
                            .reconcile(
                                patchSetID: patchSetID,
                                connection: connection,
                                logger: app.logger
                            )
                    } catch PostgresPatchLineageError
                        .missingPatchSet
                    {
                        skipped = true
                    }
                }

                #expect(skipped)
                #expect(
                    try await fixture
                        .patchSetHasLineageState(
                            id: patchSetID
                        ) == false
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Deletion preserves a message linked to another list")
    func preservesCrossListMessage() async throws {
        try await withApp(
            configure: configure
        ) { app in
            let fixture = try await DatabaseFixture(
                app: app
            )

            do {
                let message = try fixture.message(
                    number: 1
                )
                let otherMailingListID =
                    try await fixture
                    .createAdditionalMailingList()
                let service = PostgresIngestService(
                    client: app.postgres
                )

                _ = try await service.ingestBatch(
                    [message],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                _ = try await service.ingestBatch(
                    [message],
                    mailingListID:
                        otherMailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                _ = try await service.ingestBatch(
                    [
                        .deletion(
                            commitOID:
                                String(
                                    repeating: "d",
                                    count: 40
                                ),
                            blobOID: message.blobOID
                        )
                    ],
                    mailingListID:
                        fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID:
                        message.commitOID,
                    logger: app.logger
                )

                #expect(
                    try await fixture.mailingListBlobOID(
                        messageID:
                            message.parsed.message
                            .messageID
                    ) == nil
                )
                #expect(
                    try await fixture.mailingListBlobOID(
                        messageID:
                            message.parsed.message
                            .messageID,
                        mailingListID:
                            otherMailingListID
                    ) == message.blobOID
                )
                #expect(
                    try await fixture.messageState(
                        messageID:
                            message.parsed.message
                            .messageID
                    )?.isPlaceholder == false
                )
            } catch {
                try? await fixture.remove()
                throw error
            }

            try await fixture.remove()
        }
    }

    @Test("Ingest queues lineage work without running the matcher")
    func queuesLineageWork() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let message = try fixture.message(
                    number: 1,
                    subject: "[PATCH] \(fixture.prefix): queued lineage",
                    body:
                        """
                        diff --git a/file b/file
                        --- a/file
                        +++ b/file
                        @@ -1 +1 @@
                        -old
                        +new
                        """
                )
                _ = try await PostgresIngestService(
                    client: app.postgres
                ).ingestBatch(
                    [message],
                    mailingListID: fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                #expect(
                    try await fixture.lineageState(
                        messageID: message.parsed.message.messageID
                    ) == nil
                )
                #expect(try await fixture.lineageWorkCount() == 1)
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Cross-list duplicate keeps its original patch position")
    func preservesDuplicatePatchAssociation() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let patchBody =
                    """
                    diff --git a/file b/file
                    --- a/file
                    +++ b/file
                    @@ -1 +1 @@
                    -old
                    +new
                    """
                let cover = try fixture.message(
                    number: 1,
                    subject: "[PATCH 0/2] duplicate series"
                )
                let partOne = try fixture.message(
                    number: 2,
                    inReplyTo: cover.parsed.message.messageID,
                    subject: "[PATCH 1/2] first part",
                    body: patchBody
                )
                let partTwo = try fixture.message(
                    number: 3,
                    inReplyTo: cover.parsed.message.messageID,
                    subject: "[PATCH 2/2] second part",
                    body: patchBody
                )
                let duplicatePartTwo = try fixture.message(
                    number: 4,
                    messageID: partTwo.parsed.message.messageID,
                    subject: "[PATCH] differently wrapped second part",
                    body: patchBody.replacingOccurrences(
                        of: "+new",
                        with: "+newer"
                    )
                )
                let otherMailingListID =
                    try await fixture.createAdditionalMailingList()
                let service = PostgresIngestService(client: app.postgres)

                _ = try await service.ingestBatch(
                    [cover, partOne, partTwo],
                    mailingListID: fixture.mailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )
                let original = try #require(
                    try await fixture.patchSetState(
                        messageID: cover.parsed.message.messageID
                    )
                )

                _ = try await service.ingestBatch(
                    [duplicatePartTwo],
                    mailingListID: otherMailingListID,
                    epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil,
                    logger: app.logger
                )

                let updated = try #require(
                    try await fixture.patchSetState(
                        messageID: cover.parsed.message.messageID
                    )
                )
                #expect(updated.id == original.id)
                #expect(updated.threadID == original.threadID)
                #expect(updated.totalParts == 2)
                #expect(updated.receivedParts == 2)
                #expect(updated.status == "Complete")
                #expect(
                    try await fixture.patchRows(
                        patchSetID: updated.id
                    ) == [
                        TestPatchRow(
                            messageID: partOne.parsed.message.messageID,
                            partIndex: 1,
                            diff: patchBody
                        ),
                        TestPatchRow(
                            messageID: partTwo.parsed.message.messageID,
                            partIndex: 2,
                            diff: duplicatePartTwo.parsed.patch.diff ?? ""
                        ),
                    ]
                )
                #expect(
                    try await fixture.messageBody(
                        messageID: partTwo.parsed.message.messageID
                    ) == duplicatePartTwo.parsed.message.textBody
                )
                #expect(
                    try await fixture.mailingListBlobOID(
                        messageID: partTwo.parsed.message.messageID,
                        mailingListID: otherMailingListID
                    ) == duplicatePartTwo.blobOID
                )
                #expect(
                    try await fixture.cursor(
                        mailingListID: otherMailingListID
                    ) == duplicatePartTwo.commitOID
                )
                #expect(
                    try await fixture.lineageWorkCount(
                        mailingListID: otherMailingListID
                    ) == 1
                )
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }
}

private struct TestPerson {
    let id: Int64
    let name: String?
    let email: String
}

private struct TestRecipient {
    let email: String
    let type: String
}

private struct TestMessageState {
    let threadID: Int64
    let isPlaceholder: Bool
}

private struct TestStoredMessage {
    let threadID: Int64
    let inReplyTo: String?
    let references: [String]
    let subject: String
}

private struct TestPatchSetState {
    let id: Int64
    let threadID: Int64
    let coverLetterMessageID: String?
    let totalParts: Int32
    let receivedParts: Int32
    let status: String
}

private struct TestPatchRow: Equatable {
    let messageID: String
    let partIndex: Int32
    let diff: String
}

private struct TestThreadMetadata {
    let subject: String?
    let lastUpdatedAt: Date
}

private struct TestPromotedThread {
    let threadID: Int64
    let promotedRootMessageID: String
}

private struct TestLineageState {
    let lineageID: Int64
    let source: String
    let phase: String
    let revision: Int32
    let changeID: String?
}

private final class DatabaseFixture {
    let app: Application
    let prefix: String
    let mailingListID: Int64
    let epoch: Int32 = 2_000_000_000
    private var additionalMailingListIDs: [Int64] = []

    init(app: Application) async throws {
        self.app = app
        self.prefix = "nexus-kb-batch-test-\(UUID().uuidString)"

        let rows = try await app.postgres.query(
            """
            INSERT INTO mailing_lists (
                name,
                archive_group
            )
            VALUES (
                \(prefix),
                \(prefix)
            )
            RETURNING id
            """,
            logger: app.logger
        )

        var value: Int64?

        for try await row in rows {
            value = try row.decode(Int64.self)
        }

        self.mailingListID = try #require(value)
    }

    func message(
        number: Int,
        messageID: String? = nil,
        additionalMessageIDs: [String] = [],
        inReplyTo: String? = nil,
        references: [String] = [],
        to: [String] = [],
        cc: [String] = [],
        subject: String? = nil,
        dateHeader: String =
            "Tue, 18 Aug 2026 12:00:00 -0400",
        archiveTimestamp: Date? = nil,
        body: String? = nil,
        invalidPatchIndex: Bool = false
    ) throws -> PreparedPublicInboxMessage {
        let resolvedMessageID =
            messageID
            ?? "\(prefix)-\(number)@example.com"

        let resolvedSubject =
            subject
            ?? "Batch transaction test \(number)"

        var headerLines = [
            "From: Batch Test <batch-test@example.com>",
            "Message-ID: <\(resolvedMessageID)>",
            "Subject: \(resolvedSubject)",
            "Date: \(dateHeader)",
        ]

        if let inReplyTo {
            headerLines.append(
                "In-Reply-To: <\(inReplyTo)>"
            )
        }

        if !references.isEmpty {
            headerLines.append(
                "References: "
                + references.map {
                    "<\($0)>"
                }.joined(separator: " ")
            )
        }

        if !to.isEmpty {
            headerLines.append(
                "To: \(to.joined(separator: ", "))"
            )
        }

        if !cc.isEmpty {
            headerLines.append(
                "Cc: \(cc.joined(separator: ", "))"
            )
        }

        for additionalMessageID
            in additionalMessageIDs
        {
            headerLines.append(
                "Message-ID: <\(additionalMessageID)>"
            )
        }

        let rawMessage =
            headerLines.joined(
                separator: "\r\n"
            )
            + "\r\n\r\n"
            + (
                body
                ?? "Test body \(number)\r\n"
            )

        let parsed = try IngestMessageParser()
            .parse(
                Data(rawMessage.utf8),
                archiveTimestamp: archiveTimestamp
            )

        let effectiveParsed: ParsedIngestMessage

        if invalidPatchIndex {
            effectiveParsed = ParsedIngestMessage(
                message: parsed.message,
                author: parsed.author,
                patch: ParsedPatchMetadata(
                    partIndex: -1,
                    totalParts: 1,
                    version: nil,
                    isPatchOrCover: true,
                    diff: "diff --git a/a b/a"
                )
            )
        } else {
            effectiveParsed = parsed
        }

        return PreparedPublicInboxMessage(
            commitOID:
                String(
                    format: "%040x",
                    number
                ),
            blobOID:
                String(
                    format: "%040x",
                    number + 10_000
                ),
            parsed: effectiveParsed
        )
    }

    func cursor(
        mailingListID: Int64? = nil
    ) async throws -> String? {
        let targetMailingListID =
            mailingListID ?? self.mailingListID
        let rows = try await app.postgres.query(
            """
            SELECT last_scanned_commit_oid
            FROM mailing_list_archive_epochs
            WHERE mailing_list_id = \(targetMailingListID)
              AND epoch = \(epoch)
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(String?.self)
        }

        return nil
    }

    func createAdditionalMailingList()
        async throws -> Int64
    {
        let rows = try await app.postgres.query(
            """
            INSERT INTO mailing_lists (
                name,
                archive_group
            )
            VALUES (
                \(prefix + "-additional"),
                \(prefix + "-additional")
            )
            RETURNING id
            """,
            logger: app.logger
        )

        var value: Int64?

        for try await row in rows {
            value = try row.decode(Int64.self)
        }

        let mailingListID = try #require(value)

        additionalMailingListIDs.append(
            mailingListID
        )

        return mailingListID
    }

    func messageCount() async throws -> Int64 {
        let rows = try await app.postgres.query(
            """
            SELECT count(*)::bigint
            FROM messages
            WHERE message_id LIKE \(prefix + "%")
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(Int64.self)
        }

        return 0
    }

    func remove() async throws {
        for mailingListID
            in additionalMailingListIDs
        {
            try await execute(
                """
                DELETE FROM mailing_lists
                WHERE id = \(mailingListID)
                """
            )
        }

        try await execute(
            """
            DELETE FROM mailing_lists
            WHERE id = \(mailingListID)
            """
        )

        try await execute(
            """
            DELETE FROM threads AS thread
            WHERE thread.root_message_id LIKE
                    \(prefix + "%")
               OR (
                    EXISTS (
                        SELECT 1
                        FROM messages AS owned_message
                        WHERE owned_message.thread_id =
                                thread.id
                          AND owned_message.message_id LIKE
                                \(prefix + "%")
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM messages AS unowned_message
                        WHERE unowned_message.thread_id =
                                thread.id
                          AND NOT unowned_message.is_placeholder
                          AND unowned_message.message_id NOT LIKE
                                \(prefix + "%")
                    )
               )
            """
        )
        try await execute(
            """
            DELETE FROM people
            WHERE lower(email) LIKE
                lower(\(prefix + "%"))
            """
        )
    }

    private func execute(
        _ query: PostgresQuery
    ) async throws {
        let rows = try await app.postgres.query(
            query,
            logger: app.logger
        )

        for try await _ in rows {}
    }

    func people(
        email: String
    ) async throws -> [TestPerson] {
        let rows = try await app.postgres.query(
            """
            SELECT
                id,
                name,
                email
            FROM people
            WHERE lower(email) =
                lower(\(email))
            ORDER BY id
            """,
            logger: app.logger
        )

        var people: [TestPerson] = []

        for try await row in rows {
            let value = try row.decode(
                (
                    Int64,
                    String?,
                    String
                ).self
            )

            people.append(
                TestPerson(
                    id: value.0,
                    name: value.1,
                    email: value.2
                )
            )
        }

        return people
    }

    func recipients(
        messageID: String
    ) async throws -> [TestRecipient] {
        let rows = try await app.postgres.query(
            """
            SELECT
                lower(person.email),
                recipient.recipient_type
            FROM messages_recipients
                AS recipient
            JOIN messages AS message
              ON message.id =
                    recipient.message_id
            JOIN people AS person
              ON person.id =
                    recipient.person_id
            WHERE message.message_id =
                \(messageID)
            ORDER BY
                lower(person.email),
                recipient.recipient_type
            """,
            logger: app.logger
        )

        var recipients: [TestRecipient] = []

        for try await row in rows {
            let value = try row.decode(
                (String, String).self
            )

            recipients.append(
                TestRecipient(
                    email: value.0,
                    type: value.1
                )
            )
        }

        return recipients
    }

    func messageState(
        messageID: String
    ) async throws -> TestMessageState? {
        let rows = try await app.postgres.query(
            """
            SELECT
                thread_id,
                is_placeholder
            FROM messages
            WHERE message_id = \(messageID)
            """,
            logger: app.logger
        )

        for try await row in rows {
            let value = try row.decode(
                (Int64, Bool).self
            )

            return TestMessageState(
                threadID: value.0,
                isPlaceholder: value.1
            )
        }

        return nil
    }

    func storedMessage(
        messageID: String
    ) async throws -> TestStoredMessage? {
        let rows = try await app.postgres.query(
            """
            SELECT
                thread_id,
                in_reply_to,
                references_ids,
                subject
            FROM messages
            WHERE message_id = \(messageID)
            """,
            logger: app.logger
        )

        for try await row in rows {
            let value = try row.decode(
                (
                    Int64,
                    String?,
                    [String],
                    String
                ).self
            )

            return TestStoredMessage(
                threadID: value.0,
                inReplyTo: value.1,
                references: value.2,
                subject: value.3
            )
        }

        return nil
    }

    func patchSetState(
        messageID: String
    ) async throws -> TestPatchSetState? {
        let rows = try await app.postgres.query(
            """
            SELECT DISTINCT
                patchset.id,
                patchset.thread_id,
                patchset.cover_letter_message_id,
                patchset.total_parts,
                patchset.received_parts,
                patchset.status
            FROM patchsets AS patchset
            LEFT JOIN patches AS patch
              ON patch.patchset_id = patchset.id
            WHERE patchset.cover_letter_message_id =
                    \(messageID)
               OR patch.message_id = \(messageID)
            """,
            logger: app.logger
        )

        for try await row in rows {
            let value = try row.decode(
                (
                    Int64,
                    Int64,
                    String?,
                    Int32,
                    Int32,
                    String
                ).self
            )

            return TestPatchSetState(
                id: value.0,
                threadID: value.1,
                coverLetterMessageID:
                    value.2,
                totalParts: value.3,
                receivedParts: value.4,
                status: value.5
            )
        }

        return nil
    }

    func patchMessageIDs(
        patchSetID: Int64
    ) async throws -> [String] {
        let rows = try await app.postgres.query(
            """
            SELECT message_id
            FROM patches
            WHERE patchset_id = \(patchSetID)
            ORDER BY part_index
            """,
            logger: app.logger
        )

        var messageIDs: [String] = []

        for try await row in rows {
            messageIDs.append(
                try row.decode(String.self)
            )
        }

        return messageIDs
    }

    func patchRows(
        patchSetID: Int64
    ) async throws -> [TestPatchRow] {
        let rows = try await app.postgres.query(
            """
            SELECT message_id, part_index, diff
            FROM patches
            WHERE patchset_id = \(patchSetID)
            ORDER BY part_index
            """,
            logger: app.logger
        )
        var values: [TestPatchRow] = []
        for try await row in rows {
            let value = try row.decode(
                (String, Int32, String).self
            )
            values.append(
                TestPatchRow(
                    messageID: value.0,
                    partIndex: value.1,
                    diff: value.2
                )
            )
        }
        return values
    }

    func messageBody(
        messageID: String
    ) async throws -> String? {
        let rows = try await app.postgres.query(
            """
            SELECT body
            FROM messages
            WHERE message_id = \(messageID)
            """,
            logger: app.logger
        )
        for try await row in rows {
            return try row.decode(String.self)
        }
        return nil
    }

    func mailingListBlobOID(
        messageID: String,
        mailingListID: Int64? = nil
    ) async throws -> String? {
        let targetMailingListID =
            mailingListID ?? self.mailingListID

        let rows = try await app.postgres.query(
            """
            SELECT link.archive_blob_oid
            FROM messages_mailing_lists
                AS link
            JOIN messages AS message
              ON message.id =
                    link.message_id
            WHERE message.message_id =
                    \(messageID)
              AND link.mailing_list_id =
                    \(targetMailingListID)
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(
                String?.self
            )
        }

        return nil
    }

    func threadMetadata(
        messageID: String
    ) async throws -> TestThreadMetadata? {
        let rows = try await app.postgres.query(
            """
            SELECT
                thread.subject,
                thread.last_updated_at
            FROM threads AS thread
            JOIN messages AS message
              ON message.thread_id =
                    thread.id
            WHERE message.message_id =
                    \(messageID)
            """,
            logger: app.logger
        )

        for try await row in rows {
            let value = try row.decode(
                (String?, Date).self
            )

            return TestThreadMetadata(
                subject: value.0,
                lastUpdatedAt: value.1
            )
        }

        return nil
    }

    func threadRootMessageID(
        messageID: String
    ) async throws -> String? {
        let rows = try await app.postgres.query(
            """
            SELECT thread.root_message_id
            FROM threads AS thread
            JOIN messages AS message
              ON message.thread_id = thread.id
            WHERE message.message_id = \(messageID)
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(String.self)
        }

        return nil
    }

    func insertInvalidPromotedThread() async throws -> TestPromotedThread {
        let missingRootMessageID =
            "\(prefix)-unrelated-missing@example.com"
        let promotedRootMessageID =
            "\(prefix)-unrelated-child@example.com"
        let threadRows = try await app.postgres.query(
            """
            INSERT INTO threads (
                root_message_id,
                promoted_from_message_id,
                subject,
                last_updated_at
            ) VALUES (
                \(promotedRootMessageID),
                \(missingRootMessageID),
                'Promoted child',
                now()
            )
            RETURNING id
            """,
            logger: app.logger
        )
        var threadID: Int64?
        for try await row in threadRows {
            threadID = try row.decode(Int64.self)
        }
        let value = try #require(threadID)

        try await execute(
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
                \(missingRootMessageID), \(value), NULL,
                ARRAY[]::text[], 'Late parent', 'Late parent', now(), '', false
            ),
            (
                \(promotedRootMessageID), \(value), \(missingRootMessageID),
                ARRAY[\(missingRootMessageID)]::text[], 'Child',
                'Promoted child', now(), '', false
            )
            """
        )

        return TestPromotedThread(
            threadID: value,
            promotedRootMessageID: promotedRootMessageID
        )
    }

    func rootMessageID(
        threadID: Int64
    ) async throws -> String? {
        let rows = try await app.postgres.query(
            "SELECT root_message_id FROM threads WHERE id = \(threadID)",
            logger: app.logger
        )
        for try await row in rows {
            return try row.decode(String.self)
        }
        return nil
    }

    func threadCount() async throws -> Int64 {
        let rows = try await app.postgres.query(
            """
            SELECT count(*)::bigint
            FROM threads
            WHERE root_message_id LIKE
                \(prefix + "%")
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(
                Int64.self
            )
        }

        return 0
    }

    func expectPatchTimestampMetadata(messageID: String, seconds: Double?) async throws {
        let rows = try await app.postgres.query(
            """
            SELECT message.sent_at, thread.last_updated_at, patchset.sent_at,
                   lineage.first_sent_at, lineage.latest_sent_at
            FROM messages AS message
            JOIN threads AS thread ON thread.id = message.thread_id
            JOIN patchsets AS patchset ON patchset.cover_letter_message_id = message.message_id
            JOIN patchset_lineage_state AS state ON state.patchset_id = patchset.id
            JOIN patch_lineages AS lineage ON lineage.id = state.lineage_id
            WHERE message.message_id = \(messageID)
            """,
            logger: app.logger
        )
        var count = 0
        let expected = seconds.map { Date(timeIntervalSince1970: $0) }
        for try await row in rows {
            count += 1
            let value = try row.decode((Date?, Date, Date?, Date?, Date?).self)
            #expect(value.0 == expected)
            #expect(value.1 == (expected ?? Date(timeIntervalSince1970: 0)))
            #expect(value.2 == expected)
            #expect(value.3 == expected)
            #expect(value.4 == expected)
        }
        #expect(count == 1)
    }

    func reconcilePendingLineages() async throws {
        try await app.postgres.withTransaction(
            logger: app.logger
        ) { connection in
            let rows = try await connection.query(
                """
                SELECT work.patchset_id
                FROM patch_lineage_work_items AS work
                JOIN patchsets AS patchset
                  ON patchset.id = work.patchset_id
                WHERE work.mailing_list_id = \(mailingListID)
                ORDER BY patchset.sent_at ASC NULLS FIRST,
                         patchset.id
                """,
                logger: app.logger
            )
            var patchSetIDs: [Int64] = []
            for try await row in rows {
                patchSetIDs.append(try row.decode(Int64.self))
            }

            for patchSetID in patchSetIDs {
                _ = try await PostgresPatchLineageService()
                    .reconcile(
                        patchSetID: patchSetID,
                        connection: connection,
                        logger: app.logger
                    )
            }

            let deleted = try await connection.query(
                """
                DELETE FROM patch_lineage_work_items
                WHERE mailing_list_id = \(mailingListID)
                """,
                logger: app.logger
            )
            for try await _ in deleted {}
        }
    }

    func lineageWorkCount(
        mailingListID: Int64? = nil
    ) async throws -> Int64 {
        let targetMailingListID =
            mailingListID ?? self.mailingListID
        let rows = try await app.postgres.query(
            """
            SELECT count(*)::bigint
            FROM patch_lineage_work_items
            WHERE mailing_list_id = \(targetMailingListID)
            """,
            logger: app.logger
        )
        for try await row in rows {
            return try row.decode(Int64.self)
        }
        return 0
    }

    func lineageState(
        messageID: String
    ) async throws -> TestLineageState? {
        let rows = try await app.postgres.query(
            """
            SELECT
                state.lineage_id,
                state.match_source,
                state.phase,
                state.revision,
                state.change_id
            FROM patchset_lineage_state AS state
            JOIN patchsets AS patchset
              ON patchset.id = state.patchset_id
            LEFT JOIN patches AS patch
              ON patch.patchset_id = patchset.id
            WHERE patchset.cover_letter_message_id =
                    \(messageID)
               OR patch.message_id = \(messageID)
            ORDER BY state.patchset_id
            LIMIT 1
            """,
            logger: app.logger
        )

        for try await row in rows {
            let value = try row.decode(
                (
                    Int64,
                    String,
                    String,
                    Int32,
                    String?
                ).self
            )

            return TestLineageState(
                lineageID: value.0,
                source: value.1,
                phase: value.2,
                revision: value.3,
                changeID: value.4
            )
        }

        return nil
    }

    func patchSetExists(
        id: Int64
    ) async throws -> Bool {
        let rows = try await app.postgres.query(
            """
            SELECT EXISTS (
                SELECT 1
                FROM patchsets
                WHERE id = \(id)
            )
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(Bool.self)
        }

        return false
    }

    func insertAnchorlessPatchSet(
        threadID: Int64,
        subject: String,
        sentAt: Date
    ) async throws -> Int64 {
        let rows = try await app.postgres.query(
            """
            INSERT INTO patchsets (
                thread_id,
                subject,
                author,
                sent_at,
                total_parts,
                subject_index,
                parser_version
            )
            VALUES (
                \(threadID),
                \(subject),
                'Legacy Test <legacy@example.com>',
                \(sentAt),
                6,
                4,
                3
            )
            RETURNING id
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(Int64.self)
        }

        throw PostgresPatchIngestError.missingPatchSet
    }

    func patchSetHasLineageState(
        id: Int64
    ) async throws -> Bool {
        let rows = try await app.postgres.query(
            """
            SELECT EXISTS (
                SELECT 1
                FROM patchset_lineage_state
                WHERE patchset_id = \(id)
            )
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(Bool.self)
        }

        return false
    }

    func patchSetCount(
        subject: String
    ) async throws -> Int64 {
        let rows = try await app.postgres.query(
            """
            SELECT count(*)::bigint
            FROM patchsets
            WHERE subject = \(subject)
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(Int64.self)
        }

        return 0
    }

    func lineageExists(
        id: Int64
    ) async throws -> Bool {
        let rows = try await app.postgres.query(
            """
            SELECT EXISTS (
                SELECT 1
                FROM patch_lineages
                WHERE id = \(id)
            )
            """,
            logger: app.logger
        )

        for try await row in rows {
            return try row.decode(Bool.self)
        }

        return false
    }
}

private extension DatabaseFixture {
    func rebuildRevisionLineages() async throws {
        let repository = PostgresMaintenanceRepository(client: app.postgres)
        let list = try #require(try await repository.mailingList(archiveGroup: prefix, logger: app.logger))
        let run = try await repository.createManualRun(
            mailingList: list, operation: .patchLineage, mode: .full, logger: app.logger
        )
        do {
            let stage = try #require(run.stages.first)
            try await repository.initializePatchSetTargets(stage: stage, logger: app.logger)
            let targets = try await repository.pendingPatchSetTargets(stageID: stage.id, limit: 250, logger: app.logger)
            try await app.postgres.withTransaction(logger: app.logger) { connection in
                for target in targets {
                    _ = try await PostgresPatchLineageService().reconcile(
                        patchSetID: target.patchSetID, forceRematch: target.forceRematch,
                        rebuildStageID: stage.id, connection: connection, logger: app.logger
                    )
                    try await repository.markPatchSetProcessed(
                        stageID: stage.id, mailingListID: mailingListID, patchSetID: target.patchSetID,
                        connection: connection, logger: app.logger
                    )
                }
            }
        } catch {
            try? await execute("DELETE FROM maintenance_runs WHERE id = \(run.id)")
            throw error
        }
        try await execute("DELETE FROM maintenance_runs WHERE id = \(run.id)")
    }
}

@Suite("Revision link lineage tests", .serialized)
struct RevisionLinkLineageTests {
    @Test("Sparse and ambiguous histories are independent of import order", arguments:
        [false, true], [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]])
    func resolvesHistoryInEveryOrder(ambiguous: Bool, order: [Int]) async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let first = try fixture.message(
                    number: 1, subject: "[PATCH v1 0/2] \(fixture.prefix): old",
                    dateHeader: "1 Sep 2026 12:00:00 +0000"
                )
                let second = try fixture.message(
                    number: 2, subject: ambiguous
                        ? "[PATCH v1 0/2] \(fixture.prefix): unrelated"
                        : "[PATCH v2 0/2] \(fixture.prefix): old",
                    dateHeader: "2 Sep 2026 12:00:00 +0000"
                )
                var history = "v1: https://lore.kernel.org/bpf/\(first.parsed.message.messageID)/"
                if ambiguous { history += "\nv1: https://lore.kernel.org/bpf/\(second.parsed.message.messageID)/" }
                let third = try fixture.message(
                    number: 3, subject: "[PATCH v3 0/2] \(fixture.prefix): renamed",
                    dateHeader: "3 Sep 2026 12:00:00 +0000", body: history
                )
                let messages = [first, second, third]
                var cursor: String?
                for index in order {
                    let message = messages[index]
                    _ = try await PostgresIngestService(client: app.postgres).ingestBatch(
                        [message], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                        expectedPreviousCommitOID: cursor, logger: app.logger
                    )
                    cursor = message.commitOID
                    try await fixture.reconcilePendingLineages()
                }
                // Check convergence before a rebuild can mask late-import bugs.
                var ids: Set<Int64> = []
                for message in messages {
                    let state = try #require(try await fixture.lineageState(messageID: message.parsed.message.messageID))
                    ids.insert(state.lineageID)
                }
                #expect(ids.count == (ambiguous ? 3 : 1))
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Late history targets cannot override the referrer's stronger match", arguments: ["change-id", "reply-chain"])
    func preservesReferrerPrecedence(rule: String) async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let late = try fixture.message(
                    number: 1, subject: "[PATCH v1 0/2] \(fixture.prefix): ignored history",
                    dateHeader: "1 Sep 2026 12:00:00 +0000"
                )
                let trailer = rule == "change-id" ? "\nchange-id: \(fixture.prefix)-identity" : ""
                let anchor = try fixture.message(
                    number: 2, subject: "[PATCH v2 0/2] \(fixture.prefix): anchor",
                    dateHeader: "2 Sep 2026 12:00:00 +0000", body: trailer
                )
                let referrer = try fixture.message(
                    number: 3, inReplyTo: rule == "reply-chain" ? anchor.parsed.message.messageID : nil,
                    subject: "[PATCH v3 0/2] \(fixture.prefix): renamed",
                    dateHeader: "3 Sep 2026 12:00:00 +0000",
                    body: "v1: https://lore.kernel.org/bpf/\(late.parsed.message.messageID)/" + trailer
                )
                var cursor: String?
                for message in [anchor, referrer, late] {
                    _ = try await PostgresIngestService(client: app.postgres).ingestBatch(
                        [message], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                        expectedPreviousCommitOID: cursor, logger: app.logger
                    )
                    cursor = message.commitOID
                    try await fixture.reconcilePendingLineages()
                }
                let anchorState = try #require(try await fixture.lineageState(messageID: anchor.parsed.message.messageID))
                let referrerState = try #require(try await fixture.lineageState(messageID: referrer.parsed.message.messageID))
                let lateState = try #require(try await fixture.lineageState(messageID: late.parsed.message.messageID))
                #expect(referrerState.lineageID == anchorState.lineageID)
                #expect(referrerState.source == rule)
                #expect(lateState.lineageID != anchorState.lineageID)
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Encoded NUL history does not abort a lineage maintenance batch")
    func ignoresMalformedHistoryInBatch() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let malformed = try fixture.message(
                    number: 1, subject: "[PATCH v2 0/2] \(fixture.prefix): malformed",
                    body: "v1: https://lore.kernel.org/bpf/bad%00@example.com/"
                )
                let valid = try fixture.message(number: 2, subject: "[PATCH v1 0/2] \(fixture.prefix): valid")
                _ = try await PostgresIngestService(client: app.postgres).ingestBatch(
                    [malformed, valid], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil, logger: app.logger
                )
                try await fixture.rebuildRevisionLineages()
                #expect(try await fixture.lineageState(messageID: malformed.parsed.message.messageID)?.source == "singleton")
                #expect(try await fixture.lineageState(messageID: valid.parsed.message.messageID)?.source == "singleton")
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Renamed BPF series converges in both import orders and repeated full rebuilds", arguments: [false, true])
    func linksRenamedSeries(reverse: Bool) async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                // Preserve both v4 submissions; v3/v5 need not be linked directly
                // from v9 to remain part of its lineage.
                let versions = [1, 2, 3, 4, 4, 5, 7, 8, 9]
                let ids = versions.indices.map { "\(fixture.prefix)-\($0)@example.com" }
                let messages = try versions.enumerated().map { index, version in
                    let history = versions.enumerated().filter { $0.offset < index && [1, 2, 4, 7, 8].contains($0.element) }
                        .filter { $0.element < version && $0.offset != 4 }
                        .map { "v\($0.element):\nhttps://lore.kernel.org/bpf/\(ids[$0.offset])/T/#t" }
                        .joined(separator: "\n")
                    return try fixture.message(
                        number: index + 1, messageID: ids[index],
                        subject: "[PATCH bpf-next v\(version) 0/3] \(fixture.prefix): reclaim\(version < 8 ? "/OOM" : "")",
                        dateHeader: "\(index + 1) Sep 2026 12:00:00 +0000", body: history
                    )
                }
                let service = PostgresIngestService(client: app.postgres)
                var cursor: String?
                for message in reverse ? Array(messages.reversed()) : messages {
                    _ = try await service.ingestBatch(
                        [message], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                        expectedPreviousCommitOID: cursor, logger: app.logger
                    )
                    cursor = message.commitOID
                    try await fixture.reconcilePendingLineages()
                }
                for pass in 0..<3 {
                    if pass > 0 { try await fixture.rebuildRevisionLineages() }
                    try await app.testing().test(.GET, "/api/v1/threads/\(ids[8])/patch-lineages") { response async throws in
                        #expect(response.status == .ok)
                        let result = try response.content.decode(PatchLineageCollectionView.self)
                        #expect(result.items.count == 1)
                        let lineage = try #require(result.items.first)
                        #expect(lineage.revisions.map(\.revision) == [9, 8, 7, 5, 4, 4, 3, 2, 1])
                        #expect(Set(lineage.revisions.compactMap(\.coverLetterMessageId)) == Set(ids))
                        #expect(lineage.revisions.first?.matchSource == "revision-link")
                    }
                }
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("A version link repairs existing split groups and records evidence")
    func repairsExistingSplit() async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let versions = [1, 7, 8, 9]
                let messages = try versions.enumerated().map { index, version in
                    try fixture.message(
                        number: index + 1,
                        subject: "[PATCH v\(version) 0/3] \(fixture.prefix): reclaim\(version < 8 ? "/OOM" : "")",
                        dateHeader: "\(index + 1) Sep 2026 12:00:00 +0000"
                    )
                }
                _ = try await PostgresIngestService(client: app.postgres).ingestBatch(
                    messages, mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil, logger: app.logger
                )
                try await fixture.reconcilePendingLineages()
                let before = try #require(try await fixture.lineageState(messageID: messages[0].parsed.message.messageID))
                let split = try #require(try await fixture.lineageState(messageID: messages[3].parsed.message.messageID))
                #expect(before.lineageID != split.lineageID)
                let references = [messages[1].parsed.message.messageID, messages[2].parsed.message.messageID]
                let body = "v7:\nhttps://lore.kernel.org/bpf/\(references[0])/#r\nv8:\nhttps://lore.kernel.org/bpf/\(references[1])/"
                let newestID = messages[3].parsed.message.messageID
                let updated = try await app.postgres.query(
                    "UPDATE messages SET body = \(body) WHERE message_id = \(newestID)", logger: app.logger
                )
                for try await _ in updated {}
                // Reconcile only v9: v8 must move with its existing group.
                let rows = try await app.postgres.query(
                    "SELECT id FROM patchsets WHERE cover_letter_message_id = \(newestID)", logger: app.logger
                )
                for try await row in rows {
                    let id = try row.decode(Int64.self)
                    for _ in 0..<2 {
                        _ = try await app.postgres.withTransaction(logger: app.logger) { connection in
                            try await PostgresPatchLineageService().reconcile(
                                patchSetID: id, connection: connection, logger: app.logger
                            )
                        }
                    }
                }
                for message in messages {
                    #expect(try await fixture.lineageState(messageID: message.parsed.message.messageID)?.lineageID == before.lineageID)
                }
                #expect(try await fixture.lineageExists(id: split.lineageID) == false)
                let evidence = try await app.postgres.query(
                    """
                    SELECT state.match_evidence->'referenceMessageIds' = to_jsonb(\(references.sorted())::text[]),
                           (SELECT count(*)::bigint FROM patch_lineage_events AS event
                            WHERE event.patchset_id = state.patchset_id AND event.match_source = 'revision-link'),
                           (SELECT count(*)::bigint FROM patch_lineage_events AS event
                            WHERE event.previous_lineage_id = \(split.lineageID)
                              AND event.match_source = 'revision-link'
                              AND event.match_evidence->'viaPatchsetId' = to_jsonb(state.patchset_id)
                              AND event.match_evidence->'referenceRevisions' = '[7,8]'::jsonb
                              AND event.match_evidence->'referenceMessageIds' = to_jsonb(\(references.sorted())::text[]))
                    FROM patchset_lineage_state AS state JOIN patchsets AS patchset ON patchset.id = state.patchset_id
                    WHERE patchset.cover_letter_message_id = \(newestID)
                    """, logger: app.logger
                )
                var count = 0
                for try await row in evidence {
                    count += 1
                    let value = try row.decode((Bool, Int64, Int64).self)
                    #expect(value.0)
                    #expect(value.1 == 1)
                    // Both v8's move and v9's assignment identify v9 as the
                    // source, along with the exact claimed target revisions.
                    #expect(value.2 == 2)
                }
                #expect(count == 1)
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }

    @Test("Invalid or conflicting history cannot join lineages", arguments: [
        "ordinary-link", "wrong-version", "future-date", "different-author", "change-id", "manual", "ambiguous",
        "group-manual", "group-change-id"
    ])
    func rejectsUnsafeLinks(reason: String) async throws {
        try await withApp(configure: configure) { app in
            let fixture = try await DatabaseFixture(app: app)
            do {
                let first = try fixture.message(
                    number: 1, subject: "[PATCH v1 0/2] \(fixture.prefix): old",
                    dateHeader: "1 Sep 2026 12:00:00 +0000",
                    body: reason.contains("change-id") ? "change-id: \(fixture.prefix)-old" : ""
                )
                let other = try fixture.message(
                    number: 2,
                    subject: reason.hasPrefix("group-")
                        ? "[PATCH v2 0/2] \(fixture.prefix): old"
                        : "[PATCH v1 0/2] \(fixture.prefix): unrelated",
                    dateHeader: "2 Sep 2026 12:00:00 +0000",
                    body: reason == "group-change-id" ? "change-id: \(fixture.prefix)-conflicting" : ""
                )
                let service = PostgresIngestService(client: app.postgres)
                _ = try await service.ingestBatch(
                    [first, other], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: nil, logger: app.logger
                )
                try await fixture.reconcilePendingLineages()
                let original = try #require(try await fixture.lineageState(messageID: first.parsed.message.messageID))
                if reason.hasPrefix("group-") {
                    #expect(try await fixture.lineageState(messageID: other.parsed.message.messageID)?.lineageID == original.lineageID)
                }
                if reason == "group-manual" {
                    let rows = try await app.postgres.query(
                        """
                        UPDATE patchset_lineage_state SET manual_lock = true, match_source = 'manual'
                        WHERE patchset_id IN (SELECT id FROM patchsets WHERE cover_letter_message_id = \(other.parsed.message.messageID))
                        """, logger: app.logger
                    )
                    for try await _ in rows {}
                }
                if reason == "manual" || reason == "different-author" {
                    let query: PostgresQuery = reason == "manual"
                        ? "UPDATE patchset_lineage_state SET manual_lock = true, match_source = 'manual' WHERE lineage_id = \(original.lineageID)"
                        : "UPDATE patchset_lineage_state SET author_email = 'other@example.com' WHERE lineage_id = \(original.lineageID)"
                    let rows = try await app.postgres.query(query, logger: app.logger)
                    for try await _ in rows {}
                }
                let label = reason == "ordinary-link" ? "Link" : (reason == "wrong-version" ? "v2" : "v1")
                var body = "\(label): https://lore.kernel.org/bpf/\(first.parsed.message.messageID)/"
                if reason == "change-id" { body += "\nchange-id: \(fixture.prefix)-new" }
                if reason == "ambiguous" { body += "\nv1: https://lore.kernel.org/bpf/\(other.parsed.message.messageID)/" }
                let newest = try fixture.message(
                    number: 3, subject: "[PATCH v3 0/2] \(fixture.prefix): new",
                    dateHeader: reason == "future-date" ? "31 Aug 2026 12:00:00 +0000" : "3 Sep 2026 12:00:00 +0000",
                    body: body
                )
                _ = try await service.ingestBatch(
                    [newest], mailingListID: fixture.mailingListID, epoch: fixture.epoch,
                    expectedPreviousCommitOID: other.commitOID, logger: app.logger
                )
                try await fixture.reconcilePendingLineages()
                let result = try #require(try await fixture.lineageState(messageID: newest.parsed.message.messageID))
                #expect(result.source == "singleton")
                #expect(result.lineageID != original.lineageID)
                #expect(try await fixture.lineageState(messageID: first.parsed.message.messageID)?.lineageID == original.lineageID)
                if reason == "manual" {
                    try await fixture.rebuildRevisionLineages()
                    #expect(try await fixture.lineageState(messageID: first.parsed.message.messageID)?.source == "manual")
                    #expect(try await fixture.lineageState(messageID: first.parsed.message.messageID)?.lineageID == original.lineageID)
                }
            } catch {
                try? await fixture.remove()
                throw error
            }
            try await fixture.remove()
        }
    }
}

@Test("Change-id links independent patch revisions")
func linksPatchRevisionsByChangeID() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let body =
                """
                Change description.

                change-id: nexus-lineage-change
                base-commit: 0123456789abcdef

                diff --git a/file b/file
                --- a/file
                +++ b/file
                @@ -1 +1 @@
                -old
                +new
                """
            let first = try fixture.message(
                number: 1,
                subject:
                    "[PATCH v1] net: repair path",
                body: body
            )
            let second = try fixture.message(
                number: 2,
                subject:
                    "[PATCH v2] net: repair path",
                dateHeader:
                    "Thu, 20 Aug 2026 12:00:00 -0400",
                body: body
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [first, second],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            try await fixture.reconcilePendingLineages()

            let firstState = try #require(
                try await fixture.lineageState(
                    messageID:
                        first.parsed.message.messageID
                )
            )
            let secondState = try #require(
                try await fixture.lineageState(
                    messageID:
                        second.parsed.message.messageID
                )
            )

            #expect(
                firstState.lineageID
                    == secondState.lineageID
            )
            #expect(
                secondState.source == "change-id"
            )
            #expect(secondState.phase == "PATCH")
            #expect(secondState.revision == 2)
            #expect(
                secondState.changeID
                    == "nexus-lineage-change"
            )

            try await app.testing().test(
                .GET,
                "/api/v1/patch-lineages/\(secondState.lineageID)"
            ) { response async throws in
                #expect(response.status == .ok)
                let value = try response.content.decode(
                    PatchLineageDetailView.self
                )
                #expect(value.revisions.count == 2)
                #expect(
                    value.revisions.map(\.revision)
                        == [2, 1]
                )
            }

            let rootMessageID =
                second.parsed.message.messageID

            try await app.testing().test(
                .GET,
                "/api/v1/threads/\(rootMessageID)/patch-lineages"
            ) { response async throws in
                #expect(response.status == .ok)
                let value = try response.content.decode(
                    PatchLineageCollectionView.self
                )
                #expect(value.items.count == 1)
                #expect(
                    value.items.first?.id
                        == secondState.lineageID
                )
            }
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test("Direct reroll reply links a renamed revision")
func linksPatchRevisionsByReplyChain() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let body =
                """
                diff --git a/file b/file
                --- a/file
                +++ b/file
                @@ -1 +1 @@
                -old
                +new
                """
            let first = try fixture.message(
                number: 1,
                subject:
                    "[RFC v1] mm: prototype allocator",
                body: body
            )
            let second = try fixture.message(
                number: 2,
                inReplyTo:
                    first.parsed.message.messageID,
                subject:
                    "[PATCH v1] mm: introduce allocator",
                dateHeader:
                    "Thu, 20 Aug 2026 12:00:00 -0400",
                body: body
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [first, second],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            try await fixture.reconcilePendingLineages()

            let firstState = try #require(
                try await fixture.lineageState(
                    messageID:
                        first.parsed.message.messageID
                )
            )
            let secondState = try #require(
                try await fixture.lineageState(
                    messageID:
                        second.parsed.message.messageID
                )
            )

            #expect(
                firstState.lineageID
                    == secondState.lineageID
            )
            #expect(
                secondState.source
                    == "reply-chain"
            )
            #expect(firstState.phase == "RFC")
            #expect(secondState.phase == "PATCH")
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test("Subject and author link unthreaded revisions")
func linksPatchRevisionsBySubjectAndAuthor()
    async throws
{
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let body =
                """
                diff --git a/file b/file
                --- a/file
                +++ b/file
                @@ -1 +1 @@
                -old
                +new
                """
            let first = try fixture.message(
                number: 1,
                subject:
                    "[PATCH v1 net] \(fixture.prefix): repair path",
                body: body
            )
            let second = try fixture.message(
                number: 2,
                subject:
                    "[PATCH v2 net] \(fixture.prefix): repair path",
                dateHeader:
                    "Thu, 20 Aug 2026 12:00:00 -0400",
                body: body
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [first, second],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            try await fixture.reconcilePendingLineages()

            let firstState = try #require(
                try await fixture.lineageState(
                    messageID:
                        first.parsed.message.messageID
                )
            )
            let secondState = try #require(
                try await fixture.lineageState(
                    messageID:
                        second.parsed.message.messageID
                )
            )

            #expect(
                firstState.lineageID
                    == secondState.lineageID
            )
            #expect(
                secondState.source
                    == "subject-author"
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Batch resolves each person once and preserves typed links"
)
func batchesPeopleAndRecipients() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let sharedEmail =
                "\(fixture.prefix)-shared@example.com"

            let first = try fixture.message(
                number: 1,
                to: [
                    "First Name <\(sharedEmail)>",
                    "Duplicate Name <\(sharedEmail.uppercased())>",
                ],
                cc: [
                    "Cc Name <\(sharedEmail)>"
                ]
            )

            let second = try fixture.message(
                number: 2,
                to: [
                    "Final Name <\(sharedEmail.uppercased())>"
                ]
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [first, second],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            let people = try await fixture.people(
                email: sharedEmail
            )

            #expect(people.count == 1)
            #expect(
                people.first?.name
                    == "Final Name"
            )
            #expect(
                people.first?.email
                    == sharedEmail
            )

            let firstRecipients =
                try await fixture.recipients(
                    messageID:
                        first.parsed.message
                        .messageID
                )

            #expect(
                firstRecipients.count == 2
            )
            #expect(
                firstRecipients.contains {
                    $0.type
                        == RecipientType
                        .to
                        .rawValue
                }
            )
            #expect(
                firstRecipients.contains {
                    $0.type
                        == RecipientType
                        .cc
                        .rawValue
                }
            )

            let secondRecipients =
                try await fixture.recipients(
                    messageID:
                        second.parsed.message
                        .messageID
                )

            #expect(
                secondRecipients.count == 1
            )
            #expect(
                secondRecipients.first?.type
                    == RecipientType
                    .to
                    .rawValue
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Last occurrence of a Message-ID supplies final recipients"
)
func lastMessageOccurrenceWins() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let messageID =
                "\(fixture.prefix)-duplicate@example.com"

            let oldEmail =
                "\(fixture.prefix)-old@example.com"

            let newEmail =
                "\(fixture.prefix)-new@example.com"

            let first = try fixture.message(
                number: 1,
                messageID: messageID,
                to: [
                    "Old Recipient <\(oldEmail)>"
                ]
            )

            let second = try fixture.message(
                number: 2,
                messageID: messageID,
                cc: [
                    "New Recipient <\(newEmail)>"
                ]
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [first, second],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            let recipients =
                try await fixture.recipients(
                    messageID: messageID
                )

            #expect(recipients.count == 1)
            #expect(
                recipients.first?.email
                    == newEmail.lowercased()
            )
            #expect(
                recipients.first?.type
                    == RecipientType
                    .cc
                    .rawValue
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Last empty recipient set removes previous links"
)
func emptyRecipientSetRemovesLinks() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let messageID =
                "\(fixture.prefix)-empty@example.com"

            let recipientEmail =
                "\(fixture.prefix)-removed@example.com"

            let first = try fixture.message(
                number: 1,
                messageID: messageID,
                to: [
                    "Removed <\(recipientEmail)>"
                ]
            )

            let second = try fixture.message(
                number: 2,
                messageID: messageID
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [first, second],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            #expect(
                try await fixture.recipients(
                    messageID: messageID
                ).isEmpty
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Reply before parent replaces placeholder in one thread"
)
func resolvesPlaceholderInBatch() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let rootMessageID =
                "\(fixture.prefix)-root@example.com"

            let replyMessageID =
                "\(fixture.prefix)-reply@example.com"

            let reply = try fixture.message(
                number: 1,
                messageID: replyMessageID,
                inReplyTo: rootMessageID
            )

            let root = try fixture.message(
                number: 2,
                messageID: rootMessageID
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [reply, root],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            let rootState =
                try #require(
                    try await fixture.messageState(
                        messageID:
                            rootMessageID
                    )
                )

            let replyState =
                try #require(
                    try await fixture.messageState(
                        messageID:
                            replyMessageID
                    )
                )

            #expect(
                rootState.threadID
                    == replyState.threadID
            )
            #expect(!rootState.isPlaceholder)
            #expect(
                try await fixture.threadCount()
                    == 1
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "A parent arriving after root promotion restores the original root"
)
func resolvesPromotedRootInLaterBatch() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let rootMessageID =
                "\(fixture.prefix)-root@example.com"
            let reply = try fixture.message(
                number: 1,
                inReplyTo: rootMessageID,
                subject: "First available"
            )
            let service = PostgresIngestService(
                client: app.postgres
            )
            let results = try await service.ingestBatch(
                [reply],
                mailingListID: fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )
            let threadID = try #require(
                results.first?.threadID
            )

            try await PostgresThreadRootService(
                client: app.postgres
            ).finalizeEligibleRoots(
                threadIDs: [threadID],
                logger: app.logger
            )
            #expect(
                try await fixture.threadRootMessageID(
                    messageID: rootMessageID
                ) == reply.parsed.message.messageID
            )

            let root = try fixture.message(
                number: 2,
                messageID: rootMessageID,
                subject: "Late root"
            )
            _ = try await service.ingestBatch(
                [root],
                mailingListID: fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: reply.commitOID,
                logger: app.logger
            )

            #expect(
                try await fixture.threadRootMessageID(
                    messageID: rootMessageID
                ) == rootMessageID
            )
            #expect(
                try await fixture.threadMetadata(
                    messageID: rootMessageID
                )?.subject == "Late root"
            )
            #expect(try await fixture.threadCount() == 1)
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Ingest reconciliation does not inspect unrelated promoted threads"
)
func scopesPromotionReconciliationToAffectedThreads() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let unrelated = try await fixture
                .insertInvalidPromotedThread()
            let message = try fixture.message(number: 1)

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [message],
                mailingListID: fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            #expect(
                try await fixture.rootMessageID(
                    threadID: unrelated.threadID
                ) == unrelated.promotedRootMessageID
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Trailing Message-IDs remap a series without changing the old series"
)
func remapsPublicInboxDuplicateMessageIDs() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let oldCoverMessageID =
                "\(fixture.prefix)-old-cover@example.com"
            let oldPartOneMessageID =
                "\(fixture.prefix)-old-part-1@example.com"
            let oldPartTwoMessageID =
                "\(fixture.prefix)-old-part-2@example.com"
            let reusedCoverMessageID =
                "\(fixture.prefix)-reused-cover@example.com"
            let reusedPartOneMessageID =
                "\(fixture.prefix)-reused-part-1@example.com"

            let patchBody =
                """
                diff --git a/file b/file
                --- a/file
                +++ b/file
                @@ -1 +1 @@
                -old
                +new
                """

            let oldCover = try fixture.message(
                number: 1,
                messageID: oldCoverMessageID,
                subject: "[PATCH net 0/4] old series",
                dateHeader:
                    "Tue, 21 Jan 2020 12:40:27 +0000"
            )
            let oldPartOne = try fixture.message(
                number: 2,
                messageID: oldPartOneMessageID,
                inReplyTo: oldCoverMessageID,
                subject: "[PATCH net 1/4] old part one",
                dateHeader:
                    "Tue, 21 Jan 2020 12:40:28 +0000",
                body: patchBody
            )
            let oldPartTwo = try fixture.message(
                number: 3,
                messageID: oldPartTwoMessageID,
                inReplyTo: oldCoverMessageID,
                subject: "[PATCH net 2/4] old part two",
                dateHeader:
                    "Tue, 21 Jan 2020 12:40:29 +0000",
                body: patchBody
            )
            let oldPartThree = try fixture.message(
                number: 4,
                messageID: reusedCoverMessageID,
                inReplyTo: oldCoverMessageID,
                subject: "[PATCH net 3/4] old part three",
                dateHeader:
                    "Tue, 21 Jan 2020 12:40:30 +0000",
                body: patchBody
            )
            let oldPartFour = try fixture.message(
                number: 5,
                messageID: reusedPartOneMessageID,
                inReplyTo: oldCoverMessageID,
                subject: "[PATCH net 4/4] old part four",
                dateHeader:
                    "Tue, 21 Jan 2020 12:40:31 +0000",
                body: patchBody
            )

            let service = PostgresIngestService(
                client: app.postgres
            )

            _ = try await service.ingestBatch(
                [
                    oldCover,
                    oldPartOne,
                    oldPartTwo,
                    oldPartThree,
                    oldPartFour,
                ],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            let newCoverMessageID =
                "\(fixture.prefix)-20210219090439@z"
            let newPartOneMessageID =
                "\(fixture.prefix)-20210219090440@z"
            let newPartTwoMessageID =
                "\(fixture.prefix)-20210219090441@z"
            let newPartThreeMessageID =
                "\(fixture.prefix)-20210219090442@z"
            let newPartFourMessageID =
                "\(fixture.prefix)-20210219090443@z"

            let newPartOne = try fixture.message(
                number: 10,
                messageID: reusedPartOneMessageID,
                additionalMessageIDs: [
                    newPartOneMessageID
                ],
                inReplyTo: reusedCoverMessageID,
                references: [reusedCoverMessageID],
                subject: "[PATCH net-next 1/4] new part one",
                dateHeader:
                    "Fri, 19 Feb 2021 09:04:40 +0000",
                body: patchBody
            )
            let newPartTwo = try fixture.message(
                number: 11,
                messageID:
                    "\(fixture.prefix)-reused-part-2@example.com",
                additionalMessageIDs: [
                    newPartTwoMessageID
                ],
                inReplyTo: reusedCoverMessageID,
                references: [reusedCoverMessageID],
                subject: "[PATCH net-next 2/4] new part two",
                dateHeader:
                    "Fri, 19 Feb 2021 09:04:41 +0000",
                body: patchBody
            )
            let newPartThree = try fixture.message(
                number: 12,
                messageID:
                    "\(fixture.prefix)-reused-part-3@example.com",
                additionalMessageIDs: [
                    newPartThreeMessageID
                ],
                inReplyTo: reusedCoverMessageID,
                references: [reusedCoverMessageID],
                subject: "[PATCH net-next 3/4] new part three",
                dateHeader:
                    "Fri, 19 Feb 2021 09:04:42 +0000",
                body: patchBody
            )
            let newPartFour = try fixture.message(
                number: 13,
                messageID:
                    "\(fixture.prefix)-reused-part-4@example.com",
                additionalMessageIDs: [
                    newPartFourMessageID
                ],
                inReplyTo: reusedCoverMessageID,
                references: [reusedCoverMessageID],
                subject: "[PATCH net-next 4/4] new part four",
                dateHeader:
                    "Fri, 19 Feb 2021 09:04:43 +0000",
                body: patchBody
            )
            let newCover = try fixture.message(
                number: 14,
                messageID: reusedCoverMessageID,
                additionalMessageIDs: [
                    newCoverMessageID
                ],
                subject:
                    "[PATCH net-next 0/4] new series",
                dateHeader:
                    "Fri, 19 Feb 2021 09:04:39 +0000"
            )

            _ = try await service.ingestBatch(
                [
                    newPartOne,
                    newPartTwo,
                    newPartThree,
                    newPartFour,
                    newCover,
                ],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID:
                    oldPartFour.commitOID,
                logger: app.logger
            )

            let oldPatchSet = try #require(
                try await fixture.patchSetState(
                    messageID: oldCoverMessageID
                )
            )
            let newPatchSet = try #require(
                try await fixture.patchSetState(
                    messageID: newCoverMessageID
                )
            )

            #expect(oldPatchSet.id != newPatchSet.id)
            #expect(
                oldPatchSet.threadID
                    != newPatchSet.threadID
            )
            #expect(oldPatchSet.totalParts == 4)
            #expect(oldPatchSet.receivedParts == 4)
            #expect(oldPatchSet.status == "Complete")
            #expect(newPatchSet.totalParts == 4)
            #expect(newPatchSet.receivedParts == 4)
            #expect(newPatchSet.status == "Complete")
            #expect(
                newPatchSet.coverLetterMessageID
                    == newCoverMessageID
            )

            #expect(
                try await fixture.patchMessageIDs(
                    patchSetID: oldPatchSet.id
                ) == [
                    oldPartOneMessageID,
                    oldPartTwoMessageID,
                    reusedCoverMessageID,
                    reusedPartOneMessageID,
                ]
            )
            #expect(
                try await fixture.patchMessageIDs(
                    patchSetID: newPatchSet.id
                ) == [
                    newPartOneMessageID,
                    newPartTwoMessageID,
                    newPartThreeMessageID,
                    newPartFourMessageID,
                ]
            )

            let storedNewPartOne = try #require(
                try await fixture.storedMessage(
                    messageID: newPartOneMessageID
                )
            )
            let storedNewCover = try #require(
                try await fixture.storedMessage(
                    messageID: newCoverMessageID
                )
            )
            let storedOldPartFour = try #require(
                try await fixture.storedMessage(
                    messageID: reusedPartOneMessageID
                )
            )

            #expect(
                storedNewPartOne.inReplyTo
                    == newCoverMessageID
            )
            #expect(
                storedNewPartOne.references
                    == [newCoverMessageID]
            )
            #expect(
                storedNewPartOne.threadID
                    == storedNewCover.threadID
            )
            #expect(
                storedOldPartFour.subject
                    == "[PATCH net 4/4] old part four"
            )
            #expect(
                storedOldPartFour.threadID
                    == oldPatchSet.threadID
            )
            #expect(
                try await fixture.cursor()
                    == newCover.commitOID
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Thread merge remaps cached message state"
)
func remapsCachedThreadAfterMerge() async throws {
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let messageA =
                "\(fixture.prefix)-a@example.com"

            let messageB =
                "\(fixture.prefix)-b@example.com"

            let messageC =
                "\(fixture.prefix)-c@example.com"

            let root =
                "\(fixture.prefix)-root@example.com"

            let initialA = try fixture.message(
                number: 1,
                messageID: messageA
            )

            let b = try fixture.message(
                number: 2,
                messageID: messageB,
                inReplyTo: root
            )

            let movedA = try fixture.message(
                number: 3,
                messageID: messageA,
                inReplyTo: messageB
            )

            let c = try fixture.message(
                number: 4,
                messageID: messageC,
                inReplyTo: messageA
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [
                    initialA,
                    b,
                    movedA,
                    c,
                ],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            let aState = try #require(
                try await fixture.messageState(
                    messageID: messageA
                )
            )

            let bState = try #require(
                try await fixture.messageState(
                    messageID: messageB
                )
            )

            let cState = try #require(
                try await fixture.messageState(
                    messageID: messageC
                )
            )

            #expect(
                aState.threadID
                    == bState.threadID
            )
            #expect(
                bState.threadID
                    == cState.threadID
            )
            #expect(
                try await fixture.threadCount()
                    == 1
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}

@Test(
    "Batch preserves association and thread metadata ordering"
)
func batchesAssociationsAndThreadMetadata()
    async throws
{
    try await withApp(
        configure: configure
    ) { app in
        let fixture = try await DatabaseFixture(
            app: app
        )

        do {
            let rootMessageID =
                "\(fixture.prefix)-metadata-root@example.com"

            let replyMessageID =
                "\(fixture.prefix)-metadata-reply@example.com"

            let firstRoot = try fixture.message(
                number: 1,
                messageID: rootMessageID,
                subject: "Initial root subject",
                dateHeader:
                    "Tue, 18 Aug 2026 10:00:00 -0400"
            )

            let reply = try fixture.message(
                number: 2,
                messageID: replyMessageID,
                inReplyTo: rootMessageID,
                subject: "Later reply subject",
                dateHeader:
                    "Tue, 18 Aug 2026 14:00:00 -0400"
            )

            let finalRoot = try fixture.message(
                number: 3,
                messageID: rootMessageID,
                subject: "Final root subject",
                dateHeader:
                    "Tue, 18 Aug 2026 12:00:00 -0400"
            )

            _ = try await PostgresIngestService(
                client: app.postgres
            ).ingestBatch(
                [
                    firstRoot,
                    reply,
                    finalRoot,
                ],
                mailingListID:
                    fixture.mailingListID,
                epoch: fixture.epoch,
                expectedPreviousCommitOID: nil,
                logger: app.logger
            )

            #expect(
                try await fixture
                    .mailingListBlobOID(
                        messageID:
                            rootMessageID
                    )
                    == finalRoot.blobOID
            )

            #expect(
                try await fixture
                    .mailingListBlobOID(
                        messageID:
                            replyMessageID
                    )
                    == reply.blobOID
            )

            let metadata = try #require(
                try await fixture.threadMetadata(
                    messageID: rootMessageID
                )
            )

            #expect(
                metadata.subject
                    == "Final root subject"
            )

            let replyDate = try #require(
                reply.parsed.message.date
            )

            #expect(
                metadata.lastUpdatedAt
                    == replyDate
            )
        } catch {
            try? await fixture.remove()
            throw error
        }

        try await fixture.remove()
    }
}
