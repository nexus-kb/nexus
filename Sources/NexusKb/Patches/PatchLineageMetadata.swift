import Foundation

struct PatchLineageMetadata:
    Sendable,
    Equatable
{
    enum Phase:
        String,
        Sendable,
        Equatable
    {
        case rfc = "RFC"
        case patch = "PATCH"
    }

    let phase: Phase
    let revision: Int32
    let revisionExplicit: Bool
    let isResend: Bool
    let displaySubject: String
    let normalizedSubject: String
    let changeID: String?
    let baseCommit: String?
    let revisionLinks: [RevisionLink]

    struct RevisionLink: Sendable, Equatable, Hashable {
        let revision: Int32
        let messageID: String
    }
}

enum PatchLineageMetadataParser {
    static func parse(
        subject: String,
        body: String
    ) -> PatchLineageMetadata {
        let collapsed = subject.replacingOccurrences(
            of: #"\s+"#,
            with: " ",
            options: .regularExpression
        ).trimmingCharacters(
            in: .whitespacesAndNewlines
        )

        var remaining = removeReplyPrefixes(
            from: collapsed
        )
        var tokens: [String] = []

        while remaining.hasPrefix("["),
              let close = remaining.firstIndex(
                of: "]"
              )
        {
            let content = remaining[
                remaining.index(
                    after: remaining.startIndex
                )..<close
            ]

            tokens.append(
                contentsOf: subjectTokens(
                    String(content)
                )
            )

            remaining = String(
                remaining[
                    remaining.index(after: close)...
                ]
            ).trimmingCharacters(
                in: .whitespacesAndNewlines
            )
        }

        let lowerTokens = tokens.map {
            $0.lowercased()
        }
        let phase: PatchLineageMetadata.Phase =
            lowerTokens.contains {
                $0.hasPrefix("rfc")
            }
            ? .rfc
            : .patch

        let versionToken = lowerTokens.first {
            token in

            guard token.hasPrefix("v") else {
                return false
            }

            return !token.dropFirst().isEmpty
                && token.dropFirst().allSatisfy(
                    \.isNumber
                )
        }
        let revision = versionToken.flatMap {
            Int32($0.dropFirst())
        } ?? 1
        let displaySubject = remaining.isEmpty
            ? collapsed
            : remaining

        return PatchLineageMetadata(
            phase: phase,
            revision: max(1, revision),
            revisionExplicit:
                versionToken != nil,
            isResend: lowerTokens.contains {
                $0.hasPrefix("resend")
            },
            displaySubject: displaySubject,
            normalizedSubject:
                normalize(displaySubject),
            changeID: trailer(
                named: "change-id",
                in: body
            ),
            baseCommit: trailer(
                named: "base-commit",
                in: body
            ),
            revisionLinks: revisionLinks(in: body)
        )
    }

    private static func revisionLinks(
        in body: String
    ) -> [PatchLineageMetadata.RevisionLink] {
        // Only explicit history entries, never arbitrary Link: trailers,
        // prerequisite trailers, quoted mail, or URLs in a patch diff.
        let text = body.components(separatedBy: "\ndiff --git ").first ?? body
        let pattern = #"(?im)^[ \t]*v([1-9][0-9]*):[ \t]*(?:\r?\n[ \t]*)?(https?://lore\.kernel\.org/[^\s<>]+)[ \t]*\r?$"#
        guard let expression = try? NSRegularExpression(pattern: pattern) else {
            return []
        }
        let source = text as NSString
        var seen: Set<PatchLineageMetadata.RevisionLink> = []
        return expression.matches(
            in: text, range: NSRange(location: 0, length: source.length)
        ).compactMap { match in
            guard let revision = Int32(source.substring(with: match.range(at: 1))),
                  let url = URLComponents(string: source.substring(with: match.range(at: 2))),
                  url.host?.lowercased() == "lore.kernel.org",
                  url.user == nil, url.password == nil, url.port == nil
            else { return nil }
            let path = url.percentEncodedPath.split(separator: "/")
            guard (2...3).contains(path.count),
                  path.count == 2 || ["T", "t", "raw"].contains(String(path[2])),
                  let messageID = String(path[1]).removingPercentEncoding,
                  messageID.contains("@"),
                  messageID.rangeOfCharacter(from: .controlCharacters) == nil,
                  !messageID.contains(where: { $0.isWhitespace || $0 == "<" || $0 == ">" })
            else { return nil }
            let link = PatchLineageMetadata.RevisionLink(revision: revision, messageID: messageID)
            return seen.insert(link).inserted ? link : nil
        }
    }

    private static func subjectTokens(
        _ content: String
    ) -> [String] {
        content.replacingOccurrences(
            of: #"(?i)PATCH(?=v\d+)"#,
            with: "PATCH ",
            options: .regularExpression
        ).split {
            $0.isWhitespace
                || $0 == ","
                || $0 == ";"
        }.map(String.init)
    }

    private static func removeReplyPrefixes(
        from subject: String
    ) -> String {
        var value = subject
        let prefixes = [
            "re:",
            "fwd:",
            "aw:",
            "wg:",
            "forwarded:",
        ]

        while let prefix = prefixes.first(
            where: {
                value.lowercased()
                    .hasPrefix($0)
            }
        ) {
            value = String(
                value.dropFirst(prefix.count)
            ).trimmingCharacters(
                in: .whitespacesAndNewlines
            )
        }

        return value
    }

    private static func normalize(
        _ subject: String
    ) -> String {
        subject.replacingOccurrences(
            of: #"\s+"#,
            with: " ",
            options: .regularExpression
        ).trimmingCharacters(
            in: .whitespacesAndNewlines
        ).folding(
            options: [
                .caseInsensitive,
                .diacriticInsensitive,
            ],
            locale: Locale(
                identifier: "en_US_POSIX"
            )
        ).lowercased()
    }

    private static func trailer(
        named name: String,
        in body: String
    ) -> String? {
        let pattern =
            #"(?im)^"#
            + NSRegularExpression
                .escapedPattern(for: name)
            + #":[ \t]+([^ \t\r\n]+)[ \t]*$"#

        guard let expression =
                try? NSRegularExpression(
                    pattern: pattern
                )
        else {
            return nil
        }

        let source = body as NSString
        let range = NSRange(
            location: 0,
            length: source.length
        )

        guard let match = expression.firstMatch(
            in: body,
            range: range
        ),
        match.numberOfRanges > 1
        else {
            return nil
        }

        return source.substring(
            with: match.range(at: 1)
        )
    }
}
