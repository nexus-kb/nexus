import Foundation
import Vapor

struct PatchLineageRevisionView: Content {
    let patchsetId: Int64
    let rootMessageId: String
    let coverLetterMessageId: String?
    let subject: String
    let author: String?
    let sentAt: Date?
    let status: String
    let totalParts: Int32
    let receivedParts: Int32
    let phase: String
    let revision: Int32
    let revisionExplicit: Bool
    let isResend: Bool
    let changeId: String?
    let baseCommit: String?
    let matchSource: String
    let matchConfidence: Int32
    let mailingLists: [MailingListView]
    let mainline: MainlineStatusView?

    init(_ value: PatchLineageRevisionSummary, mainline: MainlineStatusView? = nil) {
        patchsetId = value.patchSetID
        rootMessageId = value.rootMessageID
        coverLetterMessageId =
            value.coverLetterMessageID
        subject = value.subject
        author = value.author
        sentAt = value.sentAt
        status = value.status.lowercased()
        totalParts = value.totalParts
        receivedParts = value.receivedParts
        phase = value.phase
        revision = value.revision
        revisionExplicit =
            value.revisionExplicit
        isResend = value.isResend
        changeId = value.changeID
        baseCommit = value.baseCommit
        matchSource = value.matchSource
        matchConfidence =
            value.matchConfidence
        mailingLists = value.mailingLists.map(
            MailingListView.init
        )
        self.mainline = mainline
    }
}

struct PatchLineageDetailView: Content {
    let id: Int64
    let subject: String
    let firstSentAt: Date?
    let latestSentAt: Date?
    let revisions: [PatchLineageRevisionView]

    init(_ value: PatchLineageDetail, mainline: [Int64: MainlineStatusView] = [:]) {
        id = value.id
        subject = value.subject
        firstSentAt = value.firstSentAt
        latestSentAt = value.latestSentAt
        revisions = value.revisions.map {
            PatchLineageRevisionView($0, mainline: mainline[$0.patchSetID])
        }
    }
}

struct PatchLineageCollectionView: Content {
    let items: [PatchLineageDetailView]

    init(_ values: [PatchLineageDetail], mainline: [Int64: MainlineStatusView] = [:]) {
        items = values.map { PatchLineageDetailView($0, mainline: mainline) }
    }
}
