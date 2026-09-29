import Foundation

enum MainlineGitError: Error, CustomStringConvertible, Sendable {
    case failed([String], Int32, String)

    var description: String {
        switch self {
        case .failed(let arguments, let status, let output):
            return "git \(arguments.joined(separator: " ")) exited \(status): \(output)"
        }
    }
}

struct MainlineGit: Sendable {
    struct Commit: Sendable {
        let oid: String
        let subject: String
        let message: String
        let patchID: String?
        let patchIDNoRenames: String?
    }

    let repositoryPath: String

    // File-backed process I/O cannot deadlock when Git writes output while it
    // is still consuming a large patch. All blocking work stays off NIO loops.
    func run(_ arguments: [String], input: Data? = nil) async throws -> String {
        try await Task.detached(priority: .utility) {
            let directory = FileManager.default.temporaryDirectory
                .appendingPathComponent("nexus-mainline-\(UUID())")
            try FileManager.default.createDirectory(
                at: directory, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: directory) }
            let outputURL = directory.appendingPathComponent("stdout")
            let errorURL = directory.appendingPathComponent("stderr")
            let inputURL = directory.appendingPathComponent("stdin")
            try Data().write(to: outputURL)
            try Data().write(to: errorURL)
            try (input ?? Data()).write(to: inputURL)
            let output = try FileHandle(forWritingTo: outputURL)
            let error = try FileHandle(forWritingTo: errorURL)
            let source = try FileHandle(forReadingFrom: inputURL)
            defer {
                try? output.close()
                try? error.close()
                try? source.close()
            }
            let process = Process()
            process.executableURL = URL(fileURLWithPath: "/usr/bin/git")
            // The host owns the bare clone; trust only this configured path.
            process.arguments =
                ["-c", "safe.directory=\(repositoryPath)", "--git-dir", repositoryPath] + arguments
            process.standardOutput = output
            process.standardError = error
            process.standardInput = source
            try process.run()
            process.waitUntilExit()
            guard process.terminationStatus == 0 else {
                let message = String(decoding: try Data(contentsOf: errorURL), as: UTF8.self)
                throw MainlineGitError.failed(arguments, process.terminationStatus, message)
            }
            return String(decoding: try Data(contentsOf: outputURL), as: UTF8.self)
        }.value
    }

    func isAncestor(_ ancestor: String, of descendant: String) async throws -> Bool {
        do {
            _ = try await run(["merge-base", "--is-ancestor", ancestor, descendant])
            return true
        } catch MainlineGitError.failed(_, 1, _) {
            return false
        }
    }

    func patchID(diff: String) async throws -> String? {
        let value = try await run(["patch-id", "--stable"], input: Data(diff.utf8))
        return value.split(whereSeparator: \.isWhitespace).first.map(String.init)
    }

    /// Preserve the first result of hashing each mail independently. Git also
    /// recognizes object IDs inside mail as record boundaries; isolate those
    /// inputs rather than letting them impersonate another batch record.
    func patchIDs(diffs: [String]) async throws -> [String?] {
        var result = [String?](repeating: nil, count: diffs.count)
        var fallback: [Int] = []
        var markers: [String: Int] = [:]
        var input = ""
        for (offset, diff) in diffs.enumerated() {
            if diff.range(
                of: #"(?m)^(?:commit |From )?[0-9a-fA-F]{40}"#,
                options: .regularExpression) != nil
            {
                fallback.append(offset)
                continue
            }
            let hex = String(offset + 1, radix: 16)
            let marker = String(repeating: "0", count: 40 - hex.count) + hex
            markers[marker] = offset
            input += "commit \(marker)\n\(diff)\n"
        }
        if !markers.isEmpty {
            let output = try await run(["patch-id", "--stable"], input: Data(input.utf8))
            var seen: Set<Int> = []
            for line in output.split(separator: "\n") {
                let fields = line.split(separator: " ")
                guard fields.count == 2, let offset = markers[String(fields[1])] else {
                    // Git can emit additional results under the zero ID after
                    // a mail signature or malformed hunk. Only the first
                    // result tagged with our marker belongs to that mail.
                    continue
                }
                if !seen.insert(offset).inserted { fallback.append(offset) }
                result[offset] = String(fields[0])
            }
            fallback += markers.values.filter { !seen.contains($0) }
        }
        for offset in Set(fallback) {
            try Task.checkCancellation()
            result[offset] = try await patchID(diff: diffs[offset])
        }
        return result
    }

    func commits(_ oids: [String]) async throws -> [Commit] {
        guard !oids.isEmpty else { return [] }
        let metadata = try await run(
            ["log", "--no-walk=unsorted", "--format=%H%x00%s%x00%B%x00"] + oids)
        let arguments = [
            "diff-tree", "--stdin", "--root", "-p", "--full-index", "--binary",
            "--no-ext-diff", "--no-textconv", "--diff-algorithm=myers", "--no-color",
        ]
        let input = Data((oids.joined(separator: "\n") + "\n").utf8)
        let diffs = try await run(arguments + ["--find-renames=50%", "-l0"], input: input)
        let fingerprints = try await patchIDs(diff: diffs)
        var noRenames: [String: String] = [:]
        // Mail may describe the same move as either a rename or delete/add.
        if diffs.contains("\nrename from ") {
            let alternate = try await run(arguments + ["--no-renames"], input: input)
            noRenames = try await patchIDs(diff: alternate)
        }
        let fields = metadata.components(separatedBy: "\0")
        return stride(from: 0, to: fields.count - 1, by: 3).map { offset in
            let oid = fields[offset].trimmingCharacters(in: .whitespacesAndNewlines)
            return Commit(
                oid: oid, subject: fields[offset + 1], message: fields[offset + 2],
                patchID: fingerprints[oid],
                patchIDNoRenames: noRenames[oid] == fingerprints[oid] ? nil : noRenames[oid])
        }
    }

    private func patchIDs(diff: String) async throws -> [String: String] {
        let ids = try await run(["patch-id", "--stable"], input: Data(diff.utf8))
        var result: [String: String] = [:]
        for line in ids.split(separator: "\n") {
            let fields = line.split(separator: " ")
            if fields.count == 2 { result[String(fields[1])] = String(fields[0]) }
        }
        return result
    }

    static func messageReferences(in message: String) -> Set<String> {
        var result: Set<String> = []
        for line in message.split(separator: "\n") {
            guard let colon = line.firstIndex(of: ":") else { continue }
            let key = line[..<colon].lowercased()
            let value = line[line.index(after: colon)...].trimmingCharacters(in: .whitespaces)
            if key == "message-id" {
                let id = value.trimmingCharacters(in: CharacterSet(charactersIn: "<>"))
                if validMessageID(id) { result.insert(id) }
            } else if key == "link", let url = URLComponents(string: value),
                url.scheme == "https" || url.scheme == "http",
                ["lore.kernel.org", "patch.msgid.link"].contains(url.host?.lowercased() ?? ""),
                url.user == nil, url.password == nil, url.port == nil
            {
                let path = url.percentEncodedPath.split(separator: "/").map(String.init)
                // lore supports /ID, /r/ID and /list/ID, optionally /raw or /T;
                // b4 also uses the patch.msgid.link Message-ID redirector.
                let encodedID = path.first { ($0.removingPercentEncoding ?? $0).contains("@") }
                if let id = encodedID?.removingPercentEncoding, validMessageID(id) {
                    result.insert(id)
                }
            }
        }
        return result
    }

    private static func validMessageID(_ value: String) -> Bool {
        value.contains("@") && !value.contains { $0.isWhitespace || $0 == "<" || $0 == ">" }
            && value.rangeOfCharacter(from: .controlCharacters) == nil
    }
}
