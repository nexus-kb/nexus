import Foundation
import Vapor

struct MainlinePatchCommitView: Content, Sendable {
    let oid: String
    let subject: String
    let firstRelease: String?
    let matchKind: String
}

struct MainlinePatchView: Content, Sendable {
    let partIndex: Int
    let messageId: String
    let subject: String
    let commits: [MainlinePatchCommitView]
}

struct MainlineStatusView: Content, Sendable {
    let state: String
    let checkedAt: Date?
    let coverageStart: String?
    let indexedTip: String?
    let totalParts: Int
    let matchedParts: Int
    let firstRelease: String?
    let patches: [MainlinePatchView]
}

struct MainlineSubmissionView: Content, Sendable {
    let messageId: String
    let subject: String
    let rootMessageId: String
    let patchsetId: Int64
    let lineageId: Int64?
    let revision: Int?
    let partIndex: Int
    let matchKind: String
}

struct MainlineCommitView: Content, Sendable {
    let oid: String
    let subject: String
    let firstRelease: String?
    let submissions: [MainlineSubmissionView]
    let references: [String]
}

struct MainlineIndexStatusView: Content, Sendable {
    let baseRef: String?
    let coverageStart: String?
    let indexedTip: String?
    let checkedAt: Date?
    let completed: Bool
    let lastError: String?
}
