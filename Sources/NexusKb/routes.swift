import Vapor

func routes(_ app: Application) throws {
    let api = app.grouped(
        "api",
        "v1"
    )
    let admin = api.grouped("admin")
    let maintenanceController =
        AdminMaintenanceController()

    admin.post(
        "mailing-lists",
        ":archiveGroup",
        "ingest",
        use: maintenanceController.startIngest
    )
    admin.post(
        "mailing-lists",
        ":archiveGroup",
        "patch-lineage",
        use: maintenanceController.startPatchLineage
    )
    admin.post(
        "webhooks",
        "grokmirror",
        use: maintenanceController.grokmirror
    )
    admin.get(
        "operations",
        use: maintenanceController.index
    )
    admin.get(
        "operations",
        ":runID",
        use: maintenanceController.show
    )
    let threadController =
        ThreadController()
    let messageController =
        MessageController()
    let referenceDataController =
        ReferenceDataController()
    let patchLineageController =
        PatchLineageController()
    let searchController =
        SearchController()

    api.get(
        "threads",
        use: threadController.index
    )

    api.get(
        "search",
        use: searchController.index
    )

    api.get(
        "threads",
        ":rootMessageID",
        "patch-lineages",
        use: patchLineageController.forThread
    )

    api.get(
        "threads",
        ":rootMessageID",
        "messages",
        use: threadController.messages
    )

    api.get(
        "threads",
        ":rootMessageID",
        use: threadController.show
    )

    api.get(
        "messages",
        ":messageID",
        use: messageController.show
    )

    api.get(
        "patch-lineages",
        ":lineageID",
        use: patchLineageController.show
    )

    api.get(
        "mailing-lists",
        use: referenceDataController
            .mailingLists
    )

    api.get(
        "subsystems",
        use: referenceDataController
            .subsystems
    )
}
