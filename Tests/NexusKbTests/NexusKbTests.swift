@testable import NexusKb
import VaporTesting
import Testing

@Suite("App Tests")
struct NexusKbTests {
    @Test("Vapor does not serve the web interface")
    func vaporDoesNotServeWebInterface() async throws {
        try await withApp { app in
            try routes(app)

            try await app.testing().test(.GET, "/", afterResponse: { response async in
                #expect(response.status == .notFound)
            })

            try await app.testing().test(.GET, "/index.html", afterResponse: { response async in
                #expect(response.status == .notFound)
            })
        }
    }

    @Test("Legacy job routes are removed")
    func legacyJobRoutesAreRemoved() async throws {
        try await withApp(configure: configure) { app in
            try await app.testing().test(.POST, "/jobs/hello", afterResponse: { res async in
                #expect(res.status == .notFound)
            })
        }
    }
}
