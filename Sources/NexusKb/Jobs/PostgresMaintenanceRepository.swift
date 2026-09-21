import Foundation
import PostgresNIO
import Vapor

enum MaintenanceRepositoryError: Error, Sendable {
    case missingRun(UUID)
    case missingStage(UUID)
}

struct MaintenanceEpochTarget: Sendable {
    let epoch: Int32
    let repositoryPath: String
    let targetTipOID: String
    let processedItems: Int64
    let completed: Bool
}

struct MaintenancePatchSetTarget: Sendable {
    let patchSetID: Int64
    let forceRematch: Bool
}

struct PostgresMaintenanceRepository: Sendable {
    let client: PostgresClient

    func mailingList(
        archiveGroup: String,
        logger: Logger
    ) async throws -> MaintenanceMailingList? {
        let rows = try await client.query(
            """
            SELECT id, name, archive_group, archive_path
            FROM mailing_lists
            WHERE archive_group = \(archiveGroup)
            """,
            logger: logger
        )

        for try await row in rows {
            let value = try row.decode(
                (Int64, String, String, String?).self
            )
            return MaintenanceMailingList(
                id: value.0,
                name: value.1,
                archiveGroup: value.2,
                archivePath: value.3
            )
        }

        return nil
    }

    func presentMailingLists(
        logger: Logger
    ) async throws -> [MaintenanceMailingList] {
        let rows = try await client.query(
            """
            SELECT id, name, archive_group, archive_path
            FROM mailing_lists
            WHERE archive_path IS NOT NULL
              AND btrim(archive_path) <> ''
            ORDER BY archive_group
            """,
            logger: logger
        )
        var values: [MaintenanceMailingList] = []

        for try await row in rows {
            let value = try row.decode(
                (Int64, String, String, String?).self
            )
            values.append(
                MaintenanceMailingList(
                    id: value.0,
                    name: value.1,
                    archiveGroup: value.2,
                    archivePath: value.3
                )
            )
        }

        return values
    }

    func createManualRun(
        mailingList: MaintenanceMailingList,
        operation: MaintenanceStageOperation,
        mode: MaintenanceMode,
        logger: Logger
    ) async throws -> MaintenanceRunRecord {
        let runID = UUID()
        let stageID = UUID()
        let kind: MaintenanceRunKind =
            operation == .ingest ? .ingest : .patchLineage

        try await client.withTransaction(
            logger: logger
        ) { connection in
            try await execute(
                """
                INSERT INTO maintenance_runs (
                    id, kind, trigger
                ) VALUES (
                    \(runID), \(kind.rawValue),
                    \(MaintenanceTrigger.operator.rawValue)
                )
                """,
                connection: connection,
                logger: logger
            )
            try await insertStage(
                id: stageID,
                runID: runID,
                mailingListID: mailingList.id,
                position: 0,
                operation: operation,
                mode: mode,
                connection: connection,
                logger: logger
            )
        }

        return try await run(id: runID, logger: logger)
    }

    func createGrokmirrorRun(
        mailingLists: [MaintenanceMailingList],
        logger: Logger
    ) async throws -> MaintenanceRunRecord {
        let runID = UUID()

        try await client.withTransaction(
            logger: logger
        ) { connection in
            try await execute(
                """
                INSERT INTO maintenance_runs (
                    id, kind, trigger
                ) VALUES (
                    \(runID),
                    \(MaintenanceRunKind.grokmirror.rawValue),
                    \(MaintenanceTrigger.grokmirror.rawValue)
                )
                """,
                connection: connection,
                logger: logger
            )

            for (index, mailingList) in mailingLists.enumerated() {
                try await insertStage(
                    id: UUID(),
                    runID: runID,
                    mailingListID: mailingList.id,
                    position: Int32(index),
                    operation: .ingest,
                    mode: .incremental,
                    connection: connection,
                    logger: logger
                )
            }

            for (index, mailingList) in mailingLists.enumerated() {
                try await insertStage(
                    id: UUID(),
                    runID: runID,
                    mailingListID: mailingList.id,
                    position: Int32(mailingLists.count + index),
                    operation: .patchLineage,
                    mode: .incremental,
                    connection: connection,
                    logger: logger
                )
            }
        }

        return try await run(id: runID, logger: logger)
    }

    func run(
        id: UUID,
        logger: Logger
    ) async throws -> MaintenanceRunRecord {
        let rows = try await client.query(
            """
            SELECT
                sequence, kind, trigger, state, error,
                created_at, started_at, finished_at
            FROM maintenance_runs
            WHERE id = \(id)
            """,
            logger: logger
        )

        for try await row in rows {
            let cells = Array(row)
            return MaintenanceRunRecord(
                id: id,
                sequence: try cells[0].decode(Int64.self),
                kind: try decode(
                    MaintenanceRunKind.self,
                    try cells[1].decode(String.self)
                ),
                trigger: try decode(
                    MaintenanceTrigger.self,
                    try cells[2].decode(String.self)
                ),
                state: try decode(
                    MaintenanceRunState.self,
                    try cells[3].decode(String.self)
                ),
                error: try cells[4].decode(String?.self),
                createdAt: try cells[5].decode(Date.self),
                startedAt: try cells[6].decode(Date?.self),
                finishedAt: try cells[7].decode(Date?.self),
                stages: try await stages(
                    runID: id,
                    logger: logger
                )
            )
        }

        throw MaintenanceRepositoryError.missingRun(id)
    }

    func recentRuns(
        limit: Int,
        logger: Logger
    ) async throws -> [MaintenanceRunRecord] {
        let rows = try await client.query(
            """
            SELECT id
            FROM maintenance_runs
            ORDER BY created_at DESC, sequence DESC
            LIMIT \(limit)
            """,
            logger: logger
        )
        var ids: [UUID] = []
        for try await row in rows {
            ids.append(try row.decode(UUID.self))
        }

        var values: [MaintenanceRunRecord] = []
        for id in ids {
            values.append(try await run(id: id, logger: logger))
        }
        return values
    }

    func hasEarlierConflict(
        run: MaintenanceRunRecord,
        logger: Logger
    ) async throws -> Bool {
        let rows = try await client.query(
            """
            SELECT EXISTS (
                SELECT 1
                FROM maintenance_runs AS earlier
                JOIN maintenance_run_stages AS earlier_stage
                  ON earlier_stage.run_id = earlier.id
                JOIN maintenance_run_stages AS current_stage
                  ON current_stage.run_id = \(run.id)
                 AND current_stage.mailing_list_id =
                        earlier_stage.mailing_list_id
                WHERE earlier.sequence < \(run.sequence)
                  AND earlier.state IN ('queued', 'running')
            )
            """,
            logger: logger
        )
        for try await row in rows {
            return try row.decode(Bool.self)
        }
        return false
    }

    func markRunRunning(
        _ runID: UUID,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE maintenance_runs
            SET state = 'running',
                started_at = COALESCE(started_at, now()),
                error = NULL
            WHERE id = \(runID)
              AND state IN ('queued', 'running')
            """,
            logger: logger
        )
    }

    func claimRun(
        _ runID: UUID,
        queueJobID: String,
        logger: Logger
    ) async throws -> Bool {
        let rows = try await client.query(
            """
            UPDATE maintenance_runs
            SET active_queue_job_id = \(queueJobID)
            WHERE id = \(runID)
              AND state IN ('queued', 'running')
              AND (
                    active_queue_job_id IS NULL
                    OR active_queue_job_id = \(queueJobID)
              )
            RETURNING id
            """,
            logger: logger
        )
        for try await _ in rows {
            return true
        }
        return false
    }

    func markRunSucceeded(
        _ runID: UUID,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE maintenance_runs
            SET state = 'succeeded', finished_at = now(), error = NULL
            WHERE id = \(runID)
            """,
            logger: logger
        )
    }

    func markRunFailed(
        _ runID: UUID,
        error: String,
        logger: Logger
    ) async throws {
        try await client.withTransaction(logger: logger) { connection in
            try await execute(
                """
                UPDATE maintenance_run_stages
                SET state = CASE
                        WHEN state = 'running' THEN 'failed'
                        ELSE 'cancelled'
                    END,
                    error = CASE
                        WHEN state = 'running' THEN \(error)
                        ELSE error
                    END,
                    finished_at = now()
                WHERE run_id = \(runID)
                  AND state IN ('queued', 'running')
                """,
                connection: connection,
                logger: logger
            )
            try await execute(
                """
                UPDATE maintenance_runs
                SET state = 'failed', error = \(error), finished_at = now()
                WHERE id = \(runID)
                """,
                connection: connection,
                logger: logger
            )
        }
    }

    func markStageRunning(
        _ stageID: UUID,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE maintenance_run_stages
            SET state = 'running',
                started_at = COALESCE(started_at, now()),
                error = NULL
            WHERE id = \(stageID)
              AND state IN ('queued', 'running')
            """,
            logger: logger
        )
    }

    func markStageSucceeded(
        _ stageID: UUID,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE maintenance_run_stages
            SET state = 'succeeded', finished_at = now(), error = NULL
            WHERE id = \(stageID)
            """,
            logger: logger
        )
    }

    func epochTargets(
        stageID: UUID,
        logger: Logger
    ) async throws -> [MaintenanceEpochTarget] {
        let rows = try await client.query(
            """
            SELECT epoch, repository_path, target_tip_oid,
                   processed_items, completed
            FROM maintenance_stage_epoch_targets
            WHERE stage_id = \(stageID)
            ORDER BY epoch
            """,
            logger: logger
        )
        var values: [MaintenanceEpochTarget] = []
        for try await row in rows {
            let value = try row.decode(
                (Int32, String, String, Int64, Bool).self
            )
            values.append(
                MaintenanceEpochTarget(
                    epoch: value.0,
                    repositoryPath: value.1,
                    targetTipOID: value.2,
                    processedItems: value.3,
                    completed: value.4
                )
            )
        }
        return values
    }

    func insertEpochTargets(
        stageID: UUID,
        targets: [MaintenanceEpochTarget],
        logger: Logger
    ) async throws {
        try await client.withTransaction(logger: logger) { connection in
            for target in targets {
                try await execute(
                    """
                    INSERT INTO maintenance_stage_epoch_targets (
                        stage_id, epoch, repository_path, target_tip_oid
                    ) VALUES (
                        \(stageID), \(target.epoch),
                        \(target.repositoryPath), \(target.targetTipOID)
                    )
                    ON CONFLICT (stage_id, epoch) DO NOTHING
                    """,
                    connection: connection,
                    logger: logger
                )
            }
        }
    }

    func completeEpoch(
        stageID: UUID,
        epoch: Int32,
        processedItems: Int,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE maintenance_stage_epoch_targets
            SET completed = true,
                processed_items = processed_items + \(processedItems)
            WHERE stage_id = \(stageID) AND epoch = \(epoch)
            """,
            logger: logger
        )
    }

    func affectedThreadIDs(
        runID: UUID,
        logger: Logger
    ) async throws -> [Int64] {
        let rows = try await client.query(
            """
            SELECT DISTINCT target.thread_id
            FROM maintenance_stage_thread_targets AS target
            JOIN maintenance_run_stages AS stage
              ON stage.id = target.stage_id
            WHERE stage.run_id = \(runID)
              AND stage.operation = 'ingest'
            ORDER BY target.thread_id
            """,
            logger: logger
        )
        var values: [Int64] = []
        for try await row in rows {
            values.append(try row.decode(Int64.self))
        }
        return values
    }

    func hasCompleteThreadTargets(
        runID: UUID,
        logger: Logger
    ) async throws -> Bool {
        let rows = try await client.query(
            """
            SELECT COALESCE(bool_and(thread_targets_complete), true)
            FROM maintenance_run_stages
            WHERE run_id = \(runID)
              AND operation = 'ingest'
            """,
            logger: logger
        )
        for try await row in rows {
            return try row.decode(Bool.self)
        }
        return true
    }

    func initializePatchSetTargets(
        stage: MaintenanceStageRecord,
        logger: Logger
    ) async throws {
        try await client.withTransaction(logger: logger) { connection in
            if stage.mode == .incremental {
                try await execute(
                    """
                    INSERT INTO maintenance_stage_patchset_targets (
                        stage_id, patchset_id, position, force_rematch
                    )
                    SELECT \(stage.id), work.patchset_id,
                           row_number() OVER (
                               ORDER BY patchset.sent_at ASC NULLS FIRST,
                                        patchset.id
                           ) - 1,
                           false
                    FROM patch_lineage_work_items AS work
                    JOIN patchsets AS patchset
                      ON patchset.id = work.patchset_id
                    WHERE work.mailing_list_id = \(stage.mailingListID)
                    ON CONFLICT (stage_id, patchset_id) DO NOTHING
                    """,
                    connection: connection,
                    logger: logger
                )
            } else {
                try await execute(
                    """
                    INSERT INTO maintenance_stage_patchset_targets (
                        stage_id, patchset_id, position, force_rematch
                    )
                    SELECT \(stage.id), candidate.patchset_id,
                           row_number() OVER (
                               ORDER BY candidate.sent_at ASC NULLS FIRST,
                                        candidate.patchset_id
                           ) - 1,
                           NOT COALESCE(state.manual_lock, false)
                    FROM (
                    SELECT DISTINCT patchset.id AS patchset_id, patchset.sent_at
                    FROM patchsets AS patchset
                    LEFT JOIN messages AS cover
                      ON cover.message_id = patchset.cover_letter_message_id
                    LEFT JOIN messages_mailing_lists AS cover_link
                      ON cover_link.message_id = cover.id
                     AND cover_link.mailing_list_id = \(stage.mailingListID)
                    LEFT JOIN patches AS patch ON patch.patchset_id = patchset.id
                    LEFT JOIN messages AS patch_message
                      ON patch_message.message_id = patch.message_id
                    LEFT JOIN messages_mailing_lists AS patch_link
                      ON patch_link.message_id = patch_message.id
                     AND patch_link.mailing_list_id = \(stage.mailingListID)
                    WHERE cover_link.message_id IS NOT NULL
                       OR patch_link.message_id IS NOT NULL

                    UNION

                    SELECT patchset.id, patchset.sent_at
                    FROM patch_lineage_work_items AS work
                    JOIN patchsets AS patchset
                      ON patchset.id = work.patchset_id
                    WHERE work.mailing_list_id = \(stage.mailingListID)
                    ) AS candidate
                    LEFT JOIN patchset_lineage_state AS state
                      ON state.patchset_id = candidate.patchset_id
                    ON CONFLICT (stage_id, patchset_id) DO NOTHING
                    """,
                    connection: connection,
                    logger: logger
                )
            }
            try await execute(
                """
                UPDATE maintenance_run_stages
                SET total_items = (
                    SELECT count(*)::bigint
                    FROM maintenance_stage_patchset_targets
                    WHERE stage_id = \(stage.id)
                )
                WHERE id = \(stage.id)
                """,
                connection: connection,
                logger: logger
            )
        }
    }

    func pendingPatchSetTargets(
        stageID: UUID,
        limit: Int,
        logger: Logger
    ) async throws -> [MaintenancePatchSetTarget] {
        let rows = try await client.query(
            """
            SELECT patchset_id, force_rematch
            FROM maintenance_stage_patchset_targets
            WHERE stage_id = \(stageID) AND NOT processed
            ORDER BY position
            LIMIT \(limit)
            """,
            logger: logger
        )
        var values: [MaintenancePatchSetTarget] = []
        for try await row in rows {
            let value = try row.decode((Int64, Bool).self)
            values.append(
                MaintenancePatchSetTarget(
                    patchSetID: value.0,
                    forceRematch: value.1
                )
            )
        }
        return values
    }

    func markPatchSetProcessed(
        stageID: UUID,
        mailingListID: Int64,
        patchSetID: Int64,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        try await execute(
            """
            UPDATE maintenance_stage_patchset_targets
            SET processed = true
            WHERE stage_id = \(stageID)
              AND patchset_id = \(patchSetID)
            """,
            connection: connection,
            logger: logger
        )
        try await execute(
            """
            DELETE FROM patch_lineage_work_items
            WHERE mailing_list_id = \(mailingListID)
              AND patchset_id = \(patchSetID)
            """,
            connection: connection,
            logger: logger
        )
        try await execute(
            """
            UPDATE maintenance_run_stages
            SET processed_items = processed_items + 1
            WHERE id = \(stageID)
            """,
            connection: connection,
            logger: logger
        )
    }

    private func stages(
        runID: UUID,
        logger: Logger
    ) async throws -> [MaintenanceStageRecord] {
        let rows = try await client.query(
            """
            SELECT stage.id, stage.mailing_list_id,
                   mailing_list.name, mailing_list.archive_group,
                   mailing_list.archive_path, stage.position,
                   stage.operation, stage.mode, stage.state,
                   stage.processed_items, stage.total_items,
                   stage.current_epoch, stage.reset_completed,
                   stage.error, stage.started_at, stage.finished_at
            FROM maintenance_run_stages AS stage
            JOIN mailing_lists AS mailing_list
              ON mailing_list.id = stage.mailing_list_id
            WHERE stage.run_id = \(runID)
            ORDER BY stage.position
            """,
            logger: logger
        )
        var values: [MaintenanceStageRecord] = []

        for try await row in rows {
            let cells = Array(row)
            values.append(
                MaintenanceStageRecord(
                    id: try cells[0].decode(UUID.self),
                    mailingListID: try cells[1].decode(Int64.self),
                    mailingListName: try cells[2].decode(String.self),
                    archiveGroup: try cells[3].decode(String.self),
                    archivePath: try cells[4].decode(String?.self),
                    position: try cells[5].decode(Int32.self),
                    operation: try decode(
                        MaintenanceStageOperation.self,
                        try cells[6].decode(String.self)
                    ),
                    mode: try decode(
                        MaintenanceMode.self,
                        try cells[7].decode(String.self)
                    ),
                    state: try decode(
                        MaintenanceStageState.self,
                        try cells[8].decode(String.self)
                    ),
                    processedItems: try cells[9].decode(Int64.self),
                    totalItems: try cells[10].decode(Int64?.self),
                    currentEpoch: try cells[11].decode(Int32?.self),
                    resetCompleted: try cells[12].decode(Bool.self),
                    error: try cells[13].decode(String?.self),
                    startedAt: try cells[14].decode(Date?.self),
                    finishedAt: try cells[15].decode(Date?.self)
                )
            )
        }
        return values
    }

    private func insertStage(
        id: UUID,
        runID: UUID,
        mailingListID: Int64,
        position: Int32,
        operation: MaintenanceStageOperation,
        mode: MaintenanceMode,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        try await execute(
            """
            INSERT INTO maintenance_run_stages (
                id, run_id, mailing_list_id, position, operation, mode
            ) VALUES (
                \(id), \(runID), \(mailingListID), \(position),
                \(operation.rawValue), \(mode.rawValue)
            )
            """,
            connection: connection,
            logger: logger
        )
    }

    private func decode<T: RawRepresentable>(
        _ type: T.Type,
        _ value: String
    ) throws -> T where T.RawValue == String {
        guard let decoded = T(rawValue: value) else {
            throw DecodingError.dataCorrupted(
                .init(
                    codingPath: [],
                    debugDescription: "Invalid \(T.self): \(value)"
                )
            )
        }
        return decoded
    }

    private func execute(
        _ query: PostgresQuery,
        logger: Logger
    ) async throws {
        let rows = try await client.query(query, logger: logger)
        for try await _ in rows {}
    }

    private func execute(
        _ query: PostgresQuery,
        connection: PostgresConnection,
        logger: Logger
    ) async throws {
        let rows = try await connection.query(query, logger: logger)
        for try await _ in rows {}
    }
}
