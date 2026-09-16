import Foundation
import Testing
@testable import NexusKb

@Test
func requiresMessageIDOnlyAtNexusBoundary() {
    let raw = Data(
        """
        From: person@example.com
        Subject: Message without an identifier

        Body
        """.utf8
    )

    #expect(
        throws: IngestMessageParserError
            .missingMessageID
    ) {
        try IngestMessageParser().parse(raw)
    }
}

@Test
func reportsMessagesWithoutHeadersAsUnparseable() {
    #expect(
        throws: IngestMessageParserError
            .unparseableMessage
    ) {
        try IngestMessageParser().parse(Data())
    }
}

@Test
func canonicalizesLegacyThreadIdentifiersForIngest() throws {
    let raw = Data(
        """
        From: Mark Richards <m.richards@utoronto.ca>
        Message-ID: <200203040330.g243URr05337@3 (NXDOMAIN) >
        In-Reply-To: <parent@example.com (legacy comment)>
        References: <root @example.com> <parent@example.com (legacy comment)>
        Subject: Re: Invalid @home email addresses

        Body
        """.utf8
    )

    let parsed = try IngestMessageParser().parse(raw)

    #expect(
        parsed.message.messageID
            == "200203040330.g243URr05337@3"
    )
    #expect(
        parsed.message.inReplyTo
            == "parent@example.com"
    )
    #expect(
        parsed.message.references == [
            "root@example.com",
            "parent@example.com",
        ]
    )
}

@Test
func selectsTheLastUsableMessageIDAcrossListsAndRepeatedHeaders() throws {
    let raw = Data(
        """
        Message-ID: <> <first @example.com> <second@example.com>
        Message-ID: <last@example.com>

        Body
        """.utf8
    )

    let parsed = try IngestMessageParser().parse(raw)

    #expect(parsed.message.messageID == "last@example.com")
    #expect(
        parsed.messageIDAliases == [
            "first@example.com",
            "second@example.com",
        ]
    )
}

@Test
func projectsRepeatedAndGroupedConcreteRecipients() throws {
    let raw = Data(
        """
        From: A Display Name Without An Address
        From: Sender <sender@example.com>
        To: First <first@example.com>, Missing Address
        To: Kernel Group: Second <second@example.com>, third@example.com;
        Cc: Undisclosed:;
        Cc: Fourth <fourth@example.com>
        Message-ID: <address-projection@example.com>
        Subject: Address projection

        Body
        """.utf8
    )

    let parsed = try IngestMessageParser().parse(raw)

    #expect(
        parsed.message.from
            == IngestMailbox(
                name: "Sender",
                address: "sender@example.com"
            )
    )
    #expect(
        parsed.message.to.map(\.address) == [
            "first@example.com",
            "second@example.com",
            "third@example.com",
        ]
    )
    #expect(
        parsed.message.cc.map(\.address)
            == ["fourth@example.com"]
    )
}

@Test
func resolvesB4AliasFromOriginalSender() throws {
    let raw = Data(
        """
        From: devnull+real.example.com@kernel.org
        X-Original-From: Real Person <real@example.com>
        Message-ID: <b4-alias@example.com>
        Subject: Alias

        Body
        """.utf8
    )

    let parsed = try IngestMessageParser().parse(raw)

    #expect(
        parsed.author
            == IngestMailbox(
                name: "Real Person",
                address: "real@example.com"
            )
    )
}

@Test
func appliesNexusProjectionFallbacks() throws {
    let raw = Data(
        """
        Message-ID: <fallbacks@example.com>

        Body
        """.utf8
    )

    let parsed = try IngestMessageParser().parse(raw)

    #expect(parsed.message.subject == "(no subject)")
    #expect(
        parsed.author
            == IngestMailbox(
                name: nil,
                address: "unknown@localhost"
            )
    )
    #expect(parsed.message.textBody == "Body")
    #expect(ParsedPatchMetadata.parserVersion == 3)
}

@Test
func projectsTimezoneAwareDateAsAbsoluteDate() throws {
    let raw = Data(
        """
        Message-ID: <date@example.com>
        Date: Tue, 18 Aug 2026 12:00:00 -0400

        Body
        """.utf8
    )

    let parsed = try IngestMessageParser().parse(raw)
    let expected = Date(
        timeIntervalSince1970: 1_787_068_800
    )

    #expect(parsed.message.date == expected)
}

@Test("Patch lineage metadata parses b4 fields")
func parsesPatchLineageMetadata() {
    let value = PatchLineageMetadataParser.parse(
        subject:
            "Re: [PATCHv4 RESEND net-next 0/3] net: repair packet path",
        body:
            """
            Cover letter.

            prerequisite-change-id: unrelated-series:v2
            change-id: packet-path-20260821-a1b2c3
            base-commit: 0123456789abcdef
            """
    )

    #expect(value.phase == .patch)
    #expect(value.revision == 4)
    #expect(value.revisionExplicit)
    #expect(value.isResend)
    #expect(
        value.displaySubject
            == "net: repair packet path"
    )
    #expect(
        value.normalizedSubject
            == "net: repair packet path"
    )
    #expect(
        value.changeID
            == "packet-path-20260821-a1b2c3"
    )
    #expect(
        value.baseCommit
            == "0123456789abcdef"
    )
}

@Test("Patch lineage metadata distinguishes inferred RFC revision")
func parsesInferredRFCRevision() {
    let value = PatchLineageMetadataParser.parse(
        subject:
            "[RFC memory-management]  MM:   New allocator ",
        body: ""
    )

    #expect(value.phase == .rfc)
    #expect(value.revision == 1)
    #expect(!value.revisionExplicit)
    #expect(!value.isResend)
    #expect(
        value.displaySubject
            == "MM: New allocator"
    )
    #expect(
        value.normalizedSubject
            == "mm: new allocator"
    )
}

@Test("Patch lineage metadata preserves non-leading brackets")
func preservesMeaningfulSubjectBrackets() {
    let value = PatchLineageMetadataParser.parse(
        subject:
            "[PATCH v2] docs: explain array[index]",
        body: ""
    )

    #expect(
        value.displaySubject
            == "docs: explain array[index]"
    )
}

@Test("Archive dates bound sender clocks without moving delayed mail forward", arguments: [
    ("Date: Sun, 13 Sep 2020 12:26:39 +0000", 1_599_999_999.0),
    ("Date: Sun, 13 Sep 2020 12:26:40 +0000", 1_600_000_000.0),
    ("Date: Sun, 13 Sep 2020 12:26:41 +0000", 1_600_000_000.0),
    ("Date: Wed, 01 Jan 2014 00:00:00 +0000", 1_388_534_400.0),
    ("Date: Thu, 01 Jan 1970 00:00:00 +0000", 1_600_000_000.0),
    ("Date: Tue, 01 Jan 1991 00:00:00 +0000", 662_688_000.0),
    ("Date: Mon, 31 Dec 1990 23:59:59 +0000", 1_600_000_000.0),
    ("Date: broken", 1_600_000_000.0),
    ("", 1_600_000_000.0),
])
func normalizesArchiveDates(_ input: (String, Double)) throws {
    let raw = Data("Message-ID: <date@example.com>\n\(input.0)\n\nBody".utf8)
    let parsed = try IngestMessageParser().parse(
        raw, archiveTimestamp: Date(timeIntervalSince1970: 1_600_000_000)
    )
    #expect(parsed.message.date == Date(timeIntervalSince1970: input.1))
}

@Test("Absent or unusable archive dates do not invent a receipt time")
func preservesDateWithoutArchiveEvidence() {
    let sentAt = Date(timeIntervalSince1970: 1_600_000_000)
    #expect(IngestMessageParser.effectiveDate(sentAt: sentAt, archiveTimestamp: nil) == sentAt)
    #expect(IngestMessageParser.effectiveDate(sentAt: nil, archiveTimestamp: nil) == nil)
    #expect(IngestMessageParser.effectiveDate(
        sentAt: sentAt, archiveTimestamp: Date(timeIntervalSince1970: 0)
    ) == sentAt)
    #expect(IngestMessageParser.effectiveDate(
        sentAt: Date(timeIntervalSince1970: 600_000_000),
        archiveTimestamp: Date(timeIntervalSince1970: 600_000_000)
    ) == nil)
    #expect(IngestMessageParser.effectiveDate(
        sentAt: sentAt.addingTimeInterval(1), archiveTimestamp: sentAt.addingTimeInterval(2),
        now: sentAt
    ) == nil)
    #expect(IngestMessageParser.effectiveDate(
        sentAt: sentAt, archiveTimestamp: sentAt.addingTimeInterval(1), now: sentAt
    ) == sentAt)
}
