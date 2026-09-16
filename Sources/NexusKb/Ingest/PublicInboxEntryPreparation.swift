import Foundation

enum PublicInboxEntryPreparationError:
    Error,
    Sendable,
    Equatable
{
    case messageParseFailed(
        commitOID: String,
        blobOID: String,
        error: String
    )
}

enum PublicInboxEntryPreparation {
    static func prepare(
        _ entry: PublicInboxArchiveEntry,
        parser: IngestMessageParser = IngestMessageParser()
    ) throws -> PreparedPublicInboxArchiveEntry {
        switch entry {
        case .deletion(let commitOID, let blobOID):
            return .deletion(
                commitOID: commitOID,
                blobOID: blobOID
            )
        case .message(let message):
            do {
                return .message(
                    PreparedPublicInboxMessage(
                        commitOID: message.commitOID,
                        blobOID: message.blobOID,
                        parsed: try parser.parse(
                            message.rawMessage,
                            archiveTimestamp: message.archiveTimestamp
                        )
                    )
                )
            } catch IngestMessageParserError.missingMessageID {
                return .skipped(
                    commitOID: message.commitOID,
                    blobOID: message.blobOID
                )
            } catch {
                throw PublicInboxEntryPreparationError.messageParseFailed(
                    commitOID: message.commitOID,
                    blobOID: message.blobOID,
                    error: String(reflecting: error)
                )
            }
        }
    }
}
