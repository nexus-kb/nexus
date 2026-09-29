import Queues
import Vapor

struct MainlineController {
    func show(_ request: Request) async throws -> MainlineCommitView {
        guard let id = request.parameters.get("commitID") else {
            throw Abort(.badRequest, reason: "Missing commit hash")
        }
        guard
            let value = try await MainlineReadRepository(client: request.postgres).commit(
                prefix: id, logger: request.logger)
        else { throw Abort(.notFound, reason: "Commit not found in indexed mainline history") }
        return value
    }
    func sync(_ request: Request) async throws -> Response {
        let id = JobIdentifier()
        try await request.queue.dispatch(
            MainlineSyncJob.self, .init(queueJobID: id.string), maxRetryCount: 3, id: id)
        return Response(status: .accepted)
    }
    func status(_ request: Request) async throws -> MainlineIndexStatusView {
        try await MainlineReadRepository(client: request.postgres).indexStatus(
            logger: request.logger)
    }
}
