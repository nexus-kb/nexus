import Foundation
import PostgresNIO
import Vapor

struct PostgresPatchLineageReadRepository:
    Sendable
{
    let client: PostgresClient

    func lineage(
        id: Int64,
        logger: Logger
    ) async throws -> PatchLineageDetail? {
        let rows = try await client.query(
            """
            SELECT
                lineage.id,
                lineage.canonical_subject,
                lineage.first_sent_at,
                lineage.latest_sent_at,
                patchset.id,
                thread.root_message_id,
                patchset.cover_letter_message_id,
                COALESCE(
                    patchset.subject,
                    state.display_subject
                ),
                patchset.author,
                patchset.sent_at,
                patchset.status,
                patchset.total_parts,
                patchset.received_parts,
                state.phase,
                state.revision,
                state.revision_explicit,
                state.is_resend,
                state.change_id,
                state.base_commit,
                state.match_source,
                state.match_confidence,
                mailing_data.names,
                mailing_data.archive_groups
            FROM patch_lineages AS lineage
            JOIN patchset_lineage_state AS state
              ON state.lineage_id = lineage.id
            JOIN patchsets AS patchset
              ON patchset.id = state.patchset_id
            JOIN threads AS thread
              ON thread.id = patchset.thread_id
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(
                        array_agg(
                            mailing_data_row.name
                            ORDER BY
                                mailing_data_row.archive_group
                        ),
                        ARRAY[]::text[]
                    ) AS names,
                    COALESCE(
                        array_agg(
                            mailing_data_row.archive_group
                            ORDER BY
                                mailing_data_row.archive_group
                        ),
                        ARRAY[]::text[]
                    ) AS archive_groups
                FROM (
                    SELECT DISTINCT
                        mailing_list.name,
                        mailing_list.archive_group
                    FROM messages AS message
                    JOIN messages_mailing_lists AS link
                      ON link.message_id = message.id
                    JOIN mailing_lists AS mailing_list
                      ON mailing_list.id =
                            link.mailing_list_id
                    WHERE message.thread_id =
                            patchset.thread_id
                ) AS mailing_data_row
            ) AS mailing_data ON true
            WHERE lineage.id = \(id)
            ORDER BY
                patchset.sent_at DESC NULLS LAST,
                patchset.id DESC
            """,
            logger: logger
        )

        var lineageID: Int64?
        var subject = ""
        var firstSentAt: Date?
        var latestSentAt: Date?
        var revisions:
            [PatchLineageRevisionSummary] = []

        for try await row in rows {
            let cells = Array(row)

            if lineageID == nil {
                lineageID = try cells[0]
                    .decode(Int64.self)
                subject = try cells[1]
                    .decode(String.self)
                firstSentAt = try cells[2]
                    .decode(Date?.self)
                latestSentAt = try cells[3]
                    .decode(Date?.self)
            }

            let mailingNames = try cells[21]
                .decode([String].self)
            let mailingGroups = try cells[22]
                .decode([String].self)

            revisions.append(
                PatchLineageRevisionSummary(
                    patchSetID: try cells[4]
                        .decode(Int64.self),
                    rootMessageID: try cells[5]
                        .decode(String.self),
                    coverLetterMessageID:
                        try cells[6]
                        .decode(String?.self),
                    subject: try cells[7]
                        .decode(String.self),
                    author: try cells[8]
                        .decode(String?.self),
                    sentAt: try cells[9]
                        .decode(Date?.self),
                    status: try cells[10]
                        .decode(String.self),
                    totalParts: try cells[11]
                        .decode(Int32.self),
                    receivedParts: try cells[12]
                        .decode(Int32.self),
                    phase: try cells[13]
                        .decode(String.self),
                    revision: try cells[14]
                        .decode(Int32.self),
                    revisionExplicit: try cells[15]
                        .decode(Bool.self),
                    isResend: try cells[16]
                        .decode(Bool.self),
                    changeID: try cells[17]
                        .decode(String?.self),
                    baseCommit: try cells[18]
                        .decode(String?.self),
                    matchSource: try cells[19]
                        .decode(String.self),
                    matchConfidence: try cells[20]
                        .decode(Int32.self),
                    mailingLists:
                        zipMailingLists(
                            names: mailingNames,
                            archiveGroups:
                                mailingGroups
                        )
                )
            )
        }

        guard let lineageID else {
            return nil
        }

        return PatchLineageDetail(
            id: lineageID,
            subject: subject,
            firstSentAt: firstSentAt,
            latestSentAt: latestSentAt,
            revisions: revisions
        )
    }

    func lineages(
        rootMessageID: MessageIdentifier,
        logger: Logger
    ) async throws -> [PatchLineageDetail] {
        let rows = try await client.query(
            """
            SELECT DISTINCT state.lineage_id
            FROM threads AS thread
            JOIN patchsets AS patchset
              ON patchset.thread_id = thread.id
            JOIN patchset_lineage_state AS state
              ON state.patchset_id = patchset.id
            WHERE thread.id = (
                SELECT message.thread_id
                FROM messages AS message
                WHERE message.message_id =
                        \(rootMessageID.value)
            )
            ORDER BY state.lineage_id
            """,
            logger: logger
        )
        var ids: [Int64] = []

        for try await row in rows {
            ids.append(
                try row.decode(Int64.self)
            )
        }

        var values: [PatchLineageDetail] = []
        values.reserveCapacity(ids.count)

        for id in ids {
            if let value = try await lineage(
                id: id,
                logger: logger
            ) {
                values.append(value)
            }
        }

        return values
    }

    private func zipMailingLists(
        names: [String],
        archiveGroups: [String]
    ) -> [MailingListSummary] {
        zip(names, archiveGroups).map {
            MailingListSummary(
                name: $0,
                archiveGroup: $1
            )
        }
    }
}
