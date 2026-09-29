import Foundation
import Queues
import Vapor

struct MainlineSyncJob: AsyncJob {
    struct Payload: Codable, Sendable { let queueJobID: String }
    func nextRetryIn(attempt: Int) -> Int { min(60, max(1, attempt) * 5) }
    func dequeue(_ context: QueueContext, _ payload: Payload) async throws {
        let lease = PostgresQueueJobLease(
            client: context.application.postgres, jobID: payload.queueJobID,
            ownerID: context.application.postgresQueueLeaseOwner, logger: context.logger)
        try await lease.start()
        do {
            let service = MainlineIndexService(
                client: context.application.postgres,
                repositoryPath: Environment.get("MAINLINE_REPO_PATH") ?? "/opt/nexus/mainline.git",
                baseRef: Environment.get("MAINLINE_BASE_REF") ?? "v2.6.12")
            try await service.run(logger: context.logger) { try await lease.assertOwned() }
            await lease.stop()
        } catch {
            await lease.stop()
            throw error
        }
    }
}
