//
//  IngestMessageParser.swift
//  NexusKb
//
//  Created by Tanuj Ravi Rao on 8/13/26.
//

import Foundation
import MailParser

enum IngestMessageParserError:
    Error,
    Sendable,
    Equatable
{
    case unparseableMessage
    case missingMessageID
}

struct IngestMailbox:
    Sendable,
    Equatable,
    Hashable,
    Codable
{
    let name: String?
    let address: String

    var displayString: String {
        guard let name, !name.isEmpty else {
            return address
        }

        let escapedName = name.replacingOccurrences(
            of: "\"",
            with: "\\\""
        )

        return "\"\(escapedName)\" <\(address)>"
    }
}

struct IngestMailMessage:
    Sendable,
    Equatable,
    Codable
{
    let messageID: String
    let subject: String
    let from: IngestMailbox?
    let to: [IngestMailbox]
    let cc: [IngestMailbox]
    let date: Date?
    let inReplyTo: String?
    let references: [String]
    let textBody: String
}

struct ParsedIngestMessage: Sendable, Equatable {
    let message: IngestMailMessage
    let messageIDAliases: [String]
    let author: IngestMailbox
    let patch: ParsedPatchMetadata

    init(
        message: IngestMailMessage,
        messageIDAliases: [String] = [],
        author: IngestMailbox,
        patch: ParsedPatchMetadata
    ) {
        self.message = message
        self.messageIDAliases = messageIDAliases
        self.author = author
        self.patch = patch
    }

    var authorDisplayString: String {
        author.displayString
    }

    var toDisplayString: String {
        message.to
            .map(\.displayString)
            .joined(separator: ", ")
    }

    var ccDisplayString: String {
        message.cc
            .map(\.displayString)
            .joined(separator: ", ")
    }
}

struct ParsedPatchMetadata: Sendable, Equatable {
    static let parserVersion: Int32 = 3

    let partIndex: Int32
    let totalParts: Int32
    let version: Int32?
    let isPatchOrCover: Bool
    let diff: String?
}

struct IngestMessageParser: Sendable {
    private let messageParser = MessageParser()

    func parse(
        _ data: Data,
        archiveTimestamp: Date? = nil
    ) throws -> ParsedIngestMessage {
        guard let parsedMessage = messageParser.parse(data)
        else {
            throw IngestMessageParserError
                .unparseableMessage
        }

        let messageIDs = parsedMessage
            .messageIDs
            .compactMap(LegacyMessageID.canonicalize)

        guard let messageID = messageIDs.last
        else {
            throw IngestMessageParserError
                .missingMessageID
        }

        var seenMessageIDs = Set([messageID])
        let messageIDAliases = messageIDs
            .dropLast()
            .filter {
                seenMessageIDs.insert($0).inserted
            }

        let from = IngestAddressProjector
            .mailboxes(
                in: parsedMessage,
                headerName: "From"
            )
            .first

        let to = IngestAddressProjector
            .mailboxes(
                in: parsedMessage,
                headerName: "To"
            )

        let cc = IngestAddressProjector
            .mailboxes(
                in: parsedMessage,
                headerName: "Cc"
            )

        let message = IngestMailMessage(
            messageID: messageID,
            subject:
                parsedMessage.subject
                ?? "(no subject)",
            from: from,
            to: to,
            cc: cc,
            date: Self.effectiveDate(
                sentAt: parsedMessage.date?.foundationDate,
                archiveTimestamp: archiveTimestamp
            ),
            inReplyTo: parsedMessage
                .inReplyToIDs
                .compactMap(
                    LegacyMessageID.canonicalize
                )
                .first,
            references: parsedMessage
                .referenceIDs
                .compactMap(
                    LegacyMessageID.canonicalize
                ),
            textBody:
                parsedMessage.bodyText(at: 0)
                ?? ""
        )

        return ParsedIngestMessage(
            message: message,
            messageIDAliases:
                Array(messageIDAliases),
            author: resolvedAuthor(
                for: parsedMessage,
                sender: from
            ),
            patch: PatchSubjectParser.parse(
                subject: message.subject,
                body: message.textBody
            )
        )
    }

    /// Keep normal sender dates, including delayed delivery. The archive's
    /// Received-preferred committer time bounds future sender clocks and fills
    /// missing/broken dates. Pre-1991 dates predate this Linux knowledge base.
    /// If both clocks are unusable, leave the date unknown rather than inventing
    /// a delivery time. Raw mail and Git metadata remain available for review.
    static func effectiveDate(
        sentAt: Date?, archiveTimestamp: Date?, now: Date = Date()
    ) -> Date? {
        guard let archiveTimestamp else { return sentAt }
        let earliestDate = Date(timeIntervalSince1970: 662_688_000) // 1991-01-01 UTC
        guard archiveTimestamp >= earliestDate, archiveTimestamp <= now else {
            guard let sentAt, sentAt >= earliestDate, sentAt <= now else { return nil }
            return sentAt
        }
        guard let sentAt, sentAt >= earliestDate else { return archiveTimestamp }
        return min(sentAt, archiveTimestamp)
    }

    private func resolvedAuthor(
        for message: Message,
        sender parsedSender: IngestMailbox?
    ) -> IngestMailbox {
        let sender = parsedSender ?? IngestMailbox(
            name: nil,
            address: "unknown@localhost"
        )

        guard let original = message
            .headerValues(
                named: "X-Original-From"
            )
            .compactMap(
                IngestAddressProjector.text
            )
            .flatMap(
                IngestAddressProjector.parseList
            )
            .first
        else {
            return sender
        }

        return B4Alias.matches(
            alias: sender.address,
            real: original.address
        ) ? original : sender
    }

    private enum B4Alias {
        static func matches(
            alias: String,
            real: String
        ) -> Bool {
            let alias = alias.lowercased()

            guard alias.hasPrefix("devnull+"),
                  let atIndex = alias.lastIndex(of: "@"),
                  atIndex > alias.index(
                      alias.startIndex,
                      offsetBy: 8
                  )
            else {
                return false
            }

            let encodedAddress = alias[
                alias.index(
                    alias.startIndex,
                    offsetBy: 8
                )..<atIndex
            ]
            let domain = alias[
                alias.index(after: atIndex)...
            ]

            guard domain == "kernel.org"
                    || domain == "linux.dev"
            else {
                return false
            }

            let realAddress = real
                .lowercased()
                .replacingOccurrences(
                    of: "@",
                    with: "."
                )

            return encodedAddress == realAddress
        }
    }
}

enum IngestAddressProjector {
    static func mailboxes(
        in message: Message,
        headerName: String
    ) -> [IngestMailbox] {
        message.headerValues(named: headerName)
            .flatMap { value -> [IngestMailbox] in
                guard case .address(let address) = value
                else {
                    return []
                }

                return mailboxes(in: address)
            }
    }

    static func parseList(
        _ value: String
    ) -> [IngestMailbox] {
        mailboxes(
            in: AddressParser().parseList(value)
        )
    }

    static func text(
        from value: HeaderValue
    ) -> String? {
        switch value {
        case .text(let text):
            return text
        case .textList(let values):
            return values.first
        default:
            return nil
        }
    }

    private static func mailboxes(
        in address: Address
    ) -> [IngestMailbox] {
        address.flattened.compactMap(project)
    }

    private static func project(
        _ mailbox: MailAddress
    ) -> IngestMailbox? {
        guard let address = mailbox.address?
            .trimmingCharacters(
                in: .whitespacesAndNewlines
            ),
            let atIndex = address.lastIndex(of: "@"),
            atIndex != address.startIndex,
            address.index(after: atIndex)
                != address.endIndex
        else {
            return nil
        }

        let name = mailbox.name?
            .trimmingCharacters(
                in: .whitespacesAndNewlines
            )

        return IngestMailbox(
            name: name?.isEmpty == false
                ? name
                : nil,
            address: address
        )
    }
}

private enum LegacyMessageID {
    static func canonicalize(
        _ value: String
    ) -> String? {
        var result = ""
        var commentDepth = 0
        var isEscaped = false

        for character in value {
            if isEscaped {
                if commentDepth == 0,
                    !character.isWhitespace
                {
                    result.append(character)
                }

                isEscaped = false
                continue
            }

            if character == "\\" {
                if commentDepth == 0 {
                    result.append(character)
                }

                isEscaped = true
                continue
            }

            if character == "(" {
                commentDepth += 1
                continue
            }

            if character == ")",
                commentDepth > 0
            {
                commentDepth -= 1
                continue
            }

            if commentDepth == 0,
                !character.isWhitespace
            {
                result.append(character)
            }
        }

        guard commentDepth == 0,
            !result.isEmpty
        else {
            return nil
        }

        return result
    }
}

private enum PatchSubjectParser {
    static func parse(
        subject: String,
        body: String
    ) -> ParsedPatchMetadata {
        let (index, total) = partPosition(
            in: subject
        )
        let version = version(in: subject)
        let lowerSubject = subject.lowercased()
        let trimmedSubject = lowerSubject
            .trimmingCharacters(
                in: .whitespacesAndNewlines
            )

        let hasDiff = body.contains("diff --git")
        || (
            body.contains("--- ")
            && body.contains("+++ ")
            && body.contains("@@ -")
        )

        let isSeries = total > 1 || index == 0
        let isPatchOrCover =
        !isReply(
            trimmedSubject,
            lowerSubject: lowerSubject
        )
        && (
            trimmedSubject.contains("patch")
            || trimmedSubject.contains("rfc")
            || hasDiff
            || isSeries
        )

        return ParsedPatchMetadata(
            partIndex: index,
            totalParts: max(1, total),
            version: version,
            isPatchOrCover: isPatchOrCover,
            diff: hasDiff && index != 0
            ? body
            : nil
        )
    }

    private static func isReply(
        _ subject: String,
        lowerSubject: String
    ) -> Bool {
        let prefixes = [
            "re:",
            "fwd:",
            "forwarded:",
            "aw:",
            "wg:",
            "回复:",
            "回复：",
            "答复:",
            "答复：",
            "[reproducer]",
        ]

        return prefixes.contains(
            where: subject.hasPrefix
        )
        || lowerSubject.contains("(was ")
        || lowerSubject.contains("(was:")
    }

    private static func partPosition(
        in subject: String
    ) -> (Int32, Int32) {
        let patterns = [
            #"(?i)\[.*?\b(?:PATCH|RFC|RESEND|v\d+)\b.*?(\d+)/(\d+).*?\]"#,
            #"(?i)\b(?:PATCH|RFC|RESEND)\s+(\d+)/(\d+)\b"#,
        ]

        for pattern in patterns {
            if let values = firstIntegerCaptures(
                pattern: pattern,
                in: subject,
                count: 2
            ) {
                return (values[0], values[1])
            }
        }

        let cleaned = subject.replacingOccurrences(
            of: #"\[.*?\]"#,
            with: "",
            options: .regularExpression
        )

        if let values = firstIntegerCaptures(
            pattern: #"^\s*(\d+)/(\d+)\b"#,
            in: cleaned,
            count: 2
        ) {
            return (values[0], values[1])
        }

        return (1, 1)
    }

    private static func version(
        in subject: String
    ) -> Int32? {
        let pattern =
            #"(?i)(?:\[[^\]]*?\bv(\d+)\b[^\]]*?\]|^\s*v(\d+)\b|PATCH\W*v(\d+)\b)"#

        guard let expression = try? NSRegularExpression(
            pattern: pattern
        ) else {
            return nil
        }

        let source = subject as NSString

        guard let match = expression.firstMatch(
            in: subject,
            range: NSRange(
                location: 0,
                length: source.length
            )
        ) else {
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

    private static func firstIntegerCaptures(
        pattern: String,
        in value: String,
        count: Int
    ) -> [Int32]? {
        guard let expression = try? NSRegularExpression(
            pattern: pattern
        ) else {
            return nil
        }

        let source = value as NSString

        guard let match = expression.firstMatch(
            in: value,
            range: NSRange(
                location: 0,
                length: source.length
            )
        ),
        match.numberOfRanges > count
        else {
            return nil
        }

        let values = (1...count).compactMap {
            capture -> Int32? in

            let range = match.range(at: capture)

            guard range.location != NSNotFound else {
                return nil
            }

            return Int32(
                source.substring(with: range)
            )
        }

        return values.count == count
            ? values
            : nil
    }
}
