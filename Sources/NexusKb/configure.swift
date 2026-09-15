import Vapor
import Queues

/// configures your application
func configure(_ app: Application) async throws {
    // configure postgres
    try await app.configurePostgres()
    
    // configure Vapor Queues using Postgres
    let queueLeaseOwner = UUID()

    app.postgresQueueLeaseOwner =
        queueLeaseOwner

    app.queues.use(
        custom: PostgresQueuesDriver(
            client: app.postgres,
            leaseOwner: queueLeaseOwner
        )
    )
    
    app.queues.configuration.workerCount = 1
    app.queues.add(MaintenanceWorkflowJob())
    
    // register routes
    try routes(app)
}
