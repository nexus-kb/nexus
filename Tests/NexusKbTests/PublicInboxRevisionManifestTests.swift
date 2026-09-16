@testable import NexusKb
import Foundation
import Testing

@Suite("Public-inbox revision manifest tests")
struct PublicInboxRevisionManifestTests {
    @Test("Manifest preserves oldest-to-newest order")
    func preservesOrder() throws {
        let repository = try TemporaryGitRepository()
        let first = try repository.commit("first")
        let second = try repository.commit("second")
        let third = try repository.commit("third")

        let manifest = try repository.subject
            .makeRevisionManifest(
                after: nil,
                through: third
            )
        defer { manifest.remove() }

        #expect(
            try manifest.nextBatch(maximumCount: 10)
                == [first, second, third]
        )
        #expect(
            try manifest.nextBatch(maximumCount: 10)
                .isEmpty
        )
    }

    @Test("Manifest excludes the durable cursor")
    func excludesCursor() throws {
        let repository = try TemporaryGitRepository()
        let first = try repository.commit("first")
        let second = try repository.commit("second")
        let third = try repository.commit("third")

        let manifest = try repository.subject
            .makeRevisionManifest(
                after: first,
                through: third
            )
        defer { manifest.remove() }

        #expect(
            try manifest.nextBatch(maximumCount: 10)
                == [second, third]
        )
    }

    @Test("Fixed target excludes commits added later")
    func fixedTargetSnapshot() throws {
        let repository = try TemporaryGitRepository()
        let first = try repository.commit("first")
        let target = try repository.commit("target")
        _ = try repository.commit("later")

        let manifest = try repository.subject
            .makeRevisionManifest(
                after: nil,
                through: target
            )
        defer { manifest.remove() }

        #expect(
            try manifest.nextBatch(maximumCount: 10)
                == [first, target]
        )
    }

    @Test("Completed and older snapshots are empty")
    func completedSnapshots() throws {
        let repository = try TemporaryGitRepository()
        let first = try repository.commit("first")
        let second = try repository.commit("second")

        let completed = try repository.subject
            .makeRevisionManifest(
                after: second,
                through: second
            )
        defer { completed.remove() }

        #expect(
            try completed.nextBatch(maximumCount: 1)
                .isEmpty
        )

        let passed = try repository.subject
            .makeRevisionManifest(
                after: second,
                through: first
            )
        defer { passed.remove() }

        #expect(
            try passed.nextBatch(maximumCount: 1)
                .isEmpty
        )
    }

    @Test("Diverged cursor and target are rejected")
    func rejectsDivergedHistory() throws {
        let repository = try TemporaryGitRepository()
        let root = try repository.commit("root")

        try repository.run([
            "checkout",
            "-b",
            "side",
            root,
        ])
        let side = try repository.commit("side")

        try repository.run([
            "checkout",
            "master",
        ])
        let master = try repository.commit("master")

        #expect(
            throws:
                PublicInboxArchiveError
                .cursorAndTipHaveDiverged(
                    cursor: side,
                    tip: master
                )
        ) {
            try repository.subject
                .makeRevisionManifest(
                    after: side,
                    through: master
                )
        }
    }

    @Test("Large manifest is consumed in bounded batches")
    func boundedBatches() throws {
        let url = FileManager.default
            .temporaryDirectory
            .appendingPathComponent(
                "nexus-kb-manifest-test-\(UUID().uuidString)"
            )

        let values = (0..<25_001).map {
            String(format: "%040x", $0)
        }

        try Data(
            (values.joined(separator: "\n") + "\n")
                .utf8
        ).write(to: url)

        let manifest = try PublicInboxRevisionManifest(
            url: url
        )
        defer { manifest.remove() }

        let first = try manifest.nextBatch(
            maximumCount: 10_000
        )
        let second = try manifest.nextBatch(
            maximumCount: 10_000
        )
        let third = try manifest.nextBatch(
            maximumCount: 10_000
        )

        #expect(first.count == 10_000)
        #expect(second.count == 10_000)
        #expect(third.count == 5_001)
        #expect(first.first == values.first)
        #expect(third.last == values.last)
        #expect(Set(first + second + third).count == 25_001)
        #expect(
            try manifest.nextBatch(maximumCount: 1)
                .isEmpty
        )
    }

    @Test("Configured archive batch size is ten thousand")
    func configuredBatchSize() {
        #expect(
            PublicInboxIngestConfiguration
                .defaultBatchSize == 10_000
        )
        #expect(
            PublicInboxIngestConfiguration
                .batchSizeRange == 1...10_000
        )
    }

    @Test("Archive loader preserves messages and deletions")
    func loadsMessagesAndDeletions() throws {
        let repository = try TemporaryGitRepository()
        let first = try repository.commit("first")
        let deletion = try repository.delete("deleted")
        let secondDeletion = try repository.delete(
            "also deleted"
        )
        let third = try repository.commit("third")

        let entries = try repository.subject
            .loadEntries(
                commitOIDs: [
                    first,
                    deletion,
                    secondDeletion,
                    third,
                ]
            )

        #expect(entries.count == 4)

        guard case .message(let firstMessage) =
                entries[0]
        else {
            Issue.record("Expected first message")
            return
        }

        #expect(firstMessage.commitOID == first)
        #expect(
            firstMessage.rawMessage
                == Data("first".utf8)
        )

        #expect(
            entries[2] == .deletion(
                commitOID: secondDeletion,
                blobOID: try repository.blobOID(
                    commit: secondDeletion,
                    path: "d"
                )
            )
        )

        #expect(
            entries[1] == .deletion(
                commitOID: deletion,
                blobOID: try repository.blobOID(
                    commit: deletion,
                    path: "d"
                )
            )
        )

        guard case .message(let thirdMessage) =
                entries[3]
        else {
            Issue.record("Expected third message")
            return
        }

        #expect(thirdMessage.commitOID == third)
        #expect(
            thirdMessage.rawMessage
                == Data("third".utf8)
        )
    }

    @Test("Archive committer time corrects future Date before ingestion")
    func preparesArchiveTimestamp() throws {
        let repository = try TemporaryGitRepository()
        let commit = try repository.commit("""
            From: sender@example.com
            Message-ID: <future@example.com>
            Date: Mon, 18 Jun 2085 15:57:19 +0000
            Subject: Future clock

            Body
            """)
        let entry = try #require(repository.subject.loadEntries(commitOIDs: [commit]).first)
        guard case .message(let prepared) = try PublicInboxEntryPreparation.prepare(entry) else {
            Issue.record("Expected prepared message")
            return
        }
        // Deliberately differs from Git's author time (1_500_000_000).
        #expect(prepared.parsed.message.date == Date(timeIntervalSince1970: 1_600_000_000))
    }

    @Test("Archive loader rejects a commit without m or d")
    func rejectsInvalidArchiveEntry() throws {
        let repository = try TemporaryGitRepository()
        _ = try repository.commit("first")
        let invalid = try repository.invalidCommit()

        #expect(
            throws:
                PublicInboxArchiveError
                .invalidArchiveEntry(invalid)
        ) {
            try repository.subject.loadEntries(
                commitOIDs: [invalid]
            )
        }
    }

    @Test("Archive loader rejects a commit with both m and d")
    func rejectsAmbiguousArchiveEntry() throws {
        let repository = try TemporaryGitRepository()
        _ = try repository.commit("first")
        let invalid = try repository.ambiguousCommit()

        #expect(
            throws:
                PublicInboxArchiveError
                .invalidArchiveEntry(invalid)
        ) {
            try repository.subject.loadEntries(
                commitOIDs: [invalid]
            )
        }
    }
}

private final class TemporaryGitRepository {
    let url: URL

    var subject: PublicInboxEpochRepository {
        PublicInboxEpochRepository(
            epoch: PublicInboxEpoch(
                number: 0,
                repositoryURL: url
            )
        )
    }

    init() throws {
        url = FileManager.default
            .temporaryDirectory
            .appendingPathComponent(
                "nexus-kb-git-test-\(UUID().uuidString)",
                isDirectory: true
            )

        try FileManager.default.createDirectory(
            at: url,
            withIntermediateDirectories: true
        )

        try run([
            "init",
            "-b",
            "master",
        ])
        try run([
            "config",
            "user.name",
            "Nexus KB Test",
        ])
        try run([
            "config",
            "user.email",
            "nexus-kb-test@example.com",
        ])
    }

    deinit {
        try? FileManager.default.removeItem(
            at: url
        )
    }

    func commit(_ value: String) throws -> String {
        try? FileManager.default.removeItem(
            at: url.appendingPathComponent("d")
        )

        try Data(value.utf8).write(
            to: url.appendingPathComponent("m")
        )

        try run([
            "add",
            "--all",
        ])
        try run([
            "commit",
            "--date=@1500000000 +0000",
            "-m",
            value,
        ])

        return try text([
            "rev-parse",
            "HEAD",
        ])
    }

    func delete(_ value: String) throws -> String {
        try? FileManager.default.removeItem(
            at: url.appendingPathComponent("m")
        )

        try Data(value.utf8).write(
            to: url.appendingPathComponent("d")
        )

        try run([
            "add",
            "--all",
        ])
        try run([
            "commit",
            "-m",
            "delete",
        ])

        return try text([
            "rev-parse",
            "HEAD",
        ])
    }

    func invalidCommit() throws -> String {
        try? FileManager.default.removeItem(
            at: url.appendingPathComponent("m")
        )
        try? FileManager.default.removeItem(
            at: url.appendingPathComponent("d")
        )

        try Data("invalid".utf8).write(
            to: url.appendingPathComponent("x")
        )

        try run([
            "add",
            "--all",
        ])
        try run([
            "commit",
            "-m",
            "invalid",
        ])

        return try text([
            "rev-parse",
            "HEAD",
        ])
    }

    func ambiguousCommit() throws -> String {
        try Data("deleted".utf8).write(
            to: url.appendingPathComponent("d")
        )

        try run([
            "add",
            "--all",
        ])
        try run([
            "commit",
            "-m",
            "ambiguous",
        ])

        return try text([
            "rev-parse",
            "HEAD",
        ])
    }

    func blobOID(
        commit: String,
        path: String
    ) throws -> String {
        try text([
            "rev-parse",
            "\(commit):\(path)",
        ])
    }

    func run(_ arguments: [String]) throws {
        _ = try execute(arguments)
    }

    private func text(_ arguments: [String]) throws -> String {
        String(
            decoding: try execute(arguments),
            as: UTF8.self
        ).trimmingCharacters(
            in: .whitespacesAndNewlines
        )
    }

    private func execute(
        _ arguments: [String]
    ) throws -> Data {
        let process = Process()
        let output = Pipe()
        let error = Pipe()

        process.executableURL = URL(
            fileURLWithPath: "/usr/bin/git"
        )
        process.arguments = [
            "-C",
            url.path,
        ] + arguments
        var environment = ProcessInfo.processInfo.environment
        environment["GIT_COMMITTER_DATE"] = "@1600000000 +0000"
        process.environment = environment
        process.standardOutput = output
        process.standardError = error

        try process.run()

        let outputData = output
            .fileHandleForReading
            .readDataToEndOfFile()
        let errorData = error
            .fileHandleForReading
            .readDataToEndOfFile()

        process.waitUntilExit()

        guard process.terminationStatus == 0 else {
            throw TemporaryGitRepositoryError.gitFailed(
                String(
                    decoding: errorData,
                    as: UTF8.self
                )
            )
        }

        return outputData
    }
}

private enum TemporaryGitRepositoryError: Error {
    case gitFailed(String)
}
