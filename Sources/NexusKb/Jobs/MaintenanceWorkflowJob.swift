import Foundation
import PostgresNIO
import Queues
import Vapor

enum MaintenanceWorkflowError: Error, Sendable {
    case missingArchivePath(Int64)
}

struct MaintenanceWorkflowJob: AsyncJob {
    struct Payload: Codable, Sendable {
        let runID: UUID
        let queueJobID: String
    }

    func nextRetryIn(attempt: Int) -> Int {
        min(60, 5 * max(1, attempt))
    }

    func dequeue(
        _ context: QueueContext,
        _ payload: Payload
    ) async throws {
        let repository = PostgresMaintenanceRepository(
            client: context.application.postgres
        )
        let run = try await repository.run(
            id: payload.runID,
            logger: context.logger
        )

        guard run.state != .succeeded,
              run.state != .failed
        else {
            return
        }

        if try await repository.hasEarlierConflict(
            run: run,
            logger: context.logger
        ) {
            let queueJobID = JobIdentifier()
            try await context.queue.dispatch(
                Self.self,
                .init(
                    runID: payload.runID,
                    queueJobID: queueJobID.string
                ),
                maxRetryCount: 3,
                delayUntil: Date(timeIntervalSinceNow: 5),
                id: queueJobID
            )
            return
        }

        guard try await repository.claimRun(
            payload.runID,
            queueJobID: payload.queueJobID,
            logger: context.logger
        ) else {
            return
        }

        let lease = PostgresQueueJobLease(
            client: context.application.postgres,
            jobID: payload.queueJobID,
            ownerID: context.application.postgresQueueLeaseOwner,
            logger: context.logger
        )
        try await lease.start()

        do {
            try await repository.markRunRunning(
                payload.runID,
                logger: context.logger
            )

            let current = try await repository.run(
                id: payload.runID,
                logger: context.logger
            )
            for stage in current.stages where stage.state != .succeeded {
                try await lease.assertOwned()
                try await repository.markStageRunning(
                    stage.id,
                    logger: context.logger
                )

                switch stage.operation {
                case .ingest:
                    try await executeIngest(
                        stage: stage,
                        repository: repository,
                        lease: lease,
                        context: context
                    )
                case .patchLineage:
                    try await executePatchLineage(
                        stage: stage,
                        repository: repository,
                        lease: lease,
                        context: context
                    )
                }

                try await repository.markStageSucceeded(
                    stage.id,
                    logger: context.logger
                )
            }

            if current.kind == .grokmirror {
                try await PostgresThreadRootService(
                    client: context.application.postgres
                ).finalizeEligibleRoots(
                    mailingListIDs: current.stages
                        .filter {
                            $0.operation == .ingest
                        }
                        .map(\.mailingListID),
                    logger: context.logger
                )
            }

            try await repository.markRunSucceeded(
                payload.runID,
                logger: context.logger
            )
            await lease.stop()
        } catch {
            await lease.stop()
            throw error
        }
    }

    func error(
        _ context: QueueContext,
        _ error: any Error,
        _ payload: Payload
    ) async throws {
        try await PostgresMaintenanceRepository(
            client: context.application.postgres
        ).markRunFailed(
            payload.runID,
            error: String(reflecting: error),
            logger: context.logger
        )
    }

    private func executeIngest(
        stage: MaintenanceStageRecord,
        repository: PostgresMaintenanceRepository,
        lease: PostgresQueueJobLease,
        context: QueueContext
    ) async throws {
        guard let archivePath = stage.archivePath else {
            throw MaintenanceWorkflowError
                .missingArchivePath(stage.mailingListID)
        }

        var targets = try await repository.epochTargets(
            stageID: stage.id,
            logger: context.logger
        )

        if targets.isEmpty {
            let archive = PublicInboxArchive(
                rootURL: URL(
                    fileURLWithPath: archivePath,
                    isDirectory: true
                )
            )
            let epochs = try await context.application.threadPool
                .runIfActive {
                    try archive.discoverEpochs().map { epoch in
                        MaintenanceEpochTarget(
                            epoch: epoch.number,
                            repositoryPath: epoch.repositoryURL.path,
                            targetTipOID: try PublicInboxEpochRepository(
                                epoch: epoch
                            ).tipOID(),
                            processedItems: 0,
                            completed: false
                        )
                    }
                }
            try await repository.insertEpochTargets(
                stageID: stage.id,
                targets: epochs,
                logger: context.logger
            )
            targets = epochs
        }

        if stage.mode == .full && !stage.resetCompleted {
            try await PostgresIngestService(
                client: context.application.postgres
            ).resetMailingList(
                mailingListID: stage.mailingListID,
                stageID: stage.id,
                logger: context.logger
            )
        }

        let store = PostgresIngestService(
            client: context.application.postgres
        )

        for target in targets where !target.completed {
            try await lease.assertOwned()
            let epoch = PublicInboxEpoch(
                number: target.epoch,
                repositoryURL: URL(
                    fileURLWithPath: target.repositoryPath,
                    isDirectory: true
                )
            )
            let epochRepository = PublicInboxEpochRepository(epoch: epoch)
            var expectedCursor = try await store.archiveCursor(
                mailingListID: stage.mailingListID,
                epoch: target.epoch,
                logger: context.logger
            )
            let cursorSnapshot = expectedCursor
            let manifest = try await context.application.threadPool
                .runIfActive {
                    try epochRepository.makeRevisionManifest(
                        after: cursorSnapshot,
                        through: target.targetTipOID
                    )
                }
            defer { manifest.remove() }
            var epochProcessed = 0

            while true {
                try await lease.assertOwned()
                let commitOIDs = try await context.application.threadPool
                    .runIfActive {
                        try manifest.nextBatch(
                            maximumCount:
                                PublicInboxIngestConfiguration.defaultBatchSize
                        )
                    }
                guard !commitOIDs.isEmpty else { break }

                let entries = try await context.application.threadPool
                    .runIfActive {
                        let parser = IngestMessageParser()
                        return try epochRepository.loadEntries(
                            commitOIDs: commitOIDs
                        ).map {
                            try PublicInboxEntryPreparation.prepare(
                                $0,
                                parser: parser
                            )
                        }
                    }
                _ = try await store.ingestBatch(
                    entries,
                    mailingListID: stage.mailingListID,
                    epoch: target.epoch,
                    expectedPreviousCommitOID: expectedCursor,
                    maintenanceStageID: stage.id,
                    logger: context.logger
                )
                expectedCursor = entries.last?.commitOID ?? expectedCursor
                epochProcessed += entries.count
            }

            try await repository.completeEpoch(
                stageID: stage.id,
                epoch: target.epoch,
                processedItems: epochProcessed,
                logger: context.logger
            )
        }
    }

    private func executePatchLineage(
        stage: MaintenanceStageRecord,
        repository: PostgresMaintenanceRepository,
        lease: PostgresQueueJobLease,
        context: QueueContext
    ) async throws {
        try await repository.initializePatchSetTargets(
            stage: stage,
            logger: context.logger
        )

        while true {
            try await lease.assertOwned()
            let targets = try await repository.pendingPatchSetTargets(
                stageID: stage.id,
                limit: 250,
                logger: context.logger
            )
            guard !targets.isEmpty else { return }

            try await context.application.postgres.withTransaction(
                logger: context.logger
            ) { connection in
                for target in targets {
                    do {
                        _ = try await PostgresPatchLineageService()
                            .reconcile(
                                patchSetID: target.patchSetID,
                                forceRematch: target.forceRematch,
                                rebuildStageID:
                                    stage.mode == .full ? stage.id : nil,
                                connection: connection,
                                logger: context.logger
                            )
                    } catch PostgresPatchLineageError.missingPatchSet {
                        context.logger.warning(
                            "Skipped patchset without lineage source message",
                            metadata: [
                                "patchset-id": "\(target.patchSetID)"
                            ]
                        )
                    }
                    try await repository.markPatchSetProcessed(
                        stageID: stage.id,
                        mailingListID: stage.mailingListID,
                        patchSetID: target.patchSetID,
                        connection: connection,
                        logger: context.logger
                    )
                }
            }
        }
    }
}
