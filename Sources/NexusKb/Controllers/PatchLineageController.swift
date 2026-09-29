import Vapor

struct PatchLineageController {
    func show(
        _ req: Request
    ) async throws -> PatchLineageDetailView {
        guard let rawID = req.parameters.get(
            "lineageID"
        ),
        let lineageID = Int64(rawID),
        lineageID > 0
        else {
            throw Abort(
                .badRequest,
                reason: "Invalid patch lineage identifier"
            )
        }

        guard let value =
                try await repository(req)
                .lineage(
                    id: lineageID,
                    logger: req.logger
                )
        else {
            throw Abort(
                .notFound,
                reason: "Patch lineage not found"
            )
        }

        let mainline = try await MainlineReadRepository(client: req.postgres)
            .statuses(patchsetIDs: value.revisions.map(\.patchSetID), logger: req.logger)
        return PatchLineageDetailView(value, mainline: mainline)
    }

    func forThread(
        _ req: Request
    ) async throws -> PatchLineageCollectionView {
        let rootMessageID = try req.messageIdentifier(
            parameter: "rootMessageID"
        )
        let values = try await repository(req)
            .lineages(
                rootMessageID: rootMessageID,
                logger: req.logger
            )

        let mainline = try await MainlineReadRepository(client: req.postgres)
            .statuses(patchsetIDs: values.flatMap { $0.revisions.map(\.patchSetID) }, logger: req.logger)
        return PatchLineageCollectionView(values, mainline: mainline)
    }

    private func repository(
        _ req: Request
    ) -> PostgresPatchLineageReadRepository {
        PostgresPatchLineageReadRepository(
            client: req.postgres
        )
    }
}
