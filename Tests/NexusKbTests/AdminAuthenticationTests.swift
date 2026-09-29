@testable import NexusKb
import Testing
import Vapor
import VaporTesting

@Suite("Admin authentication")
struct AdminAuthenticationTests {
    private let token = String(repeating: "0123456789abcdef", count: 4)

    @Test("Invalid configuration cannot register routes")
    func invalidConfiguration() async throws {
        for invalid in [nil, "", "short", String(repeating: "a", count: 63),
                        String(repeating: "a", count: 65), String(repeating: "G", count: 64),
                        String(repeating: "a", count: 63) + "\n"] as [String?] {
            try await withApp { app in
                #expect(throws: Abort.self) { try routes(app, adminToken: invalid) }
                #expect(app.routes.all.isEmpty)
            }
        }
    }

    @Test("Every admin route rejects unauthenticated requests before accessing services")
    func protectsAllAdminRoutes() async throws {
        let endpoints: [(HTTPMethod, String)] = [
            (.POST, "mainline/sync"), (.GET, "mainline"),
            (.POST, "mailing-lists/bpf/ingest"),
            (.POST, "mailing-lists/bpf/patch-lineage"),
            (.POST, "webhooks/grokmirror"), (.GET, "operations"),
            (.GET, "operations/00000000-0000-0000-0000-000000000001"),
        ]
        // No database or queue is configured: reaching those services is a test failure.
        try await withApp { app in
            try routes(app, adminToken: token)
            for authorization in [nil, "Bearer", "Bearer ", "Basic \(token)",
                                  "Bearer f\(token.dropFirst())", "Bearer \(token.dropLast())0",
                                  "Bearer \(token.dropLast())", "Bearer \(token)x"] as [String?] {
                var headers = HTTPHeaders()
                if let authorization { headers.add(name: .authorization, value: authorization) }
                // Neither a query token nor spoofed loopback forwarding bypasses authentication.
                headers.add(name: "X-Forwarded-For", value: "127.0.0.1")
                for (method, path) in endpoints {
                    try await app.testing().test(
                        method, "/api/v1/admin/\(path)?token=\(token)", headers: headers
                    ) { response async in
                        #expect(response.status == .unauthorized)
                    }
                }
            }
        }
    }

    @Test("Valid bearer token authenticates only its request and leaves public routes open")
    func authenticatesRequests() async throws {
        try await withApp { app in
            let protected = app.grouped(
                try AdminTokenAuthenticator(token: token), AdminIdentity.guardMiddleware())
            protected.get("protected") { request -> String in
                _ = try request.auth.require(AdminIdentity.self)
                return "authenticated"
            }
            app.get("public") { _ in "public" }
            var headers = HTTPHeaders()
            headers.bearerAuthorization = .init(token: token)
            try await app.testing().test(.GET, "/protected", headers: headers) { response async in
                #expect(response.status == .ok)
                #expect(response.body.string == "authenticated")
            }
            try await app.testing().test(.GET, "/protected") { response async in
                #expect(response.status == .unauthorized)
            }
            try await app.testing().test(.GET, "/public") { response async in
                #expect(response.status == .ok)
                #expect(response.body.string == "public")
            }
        }
    }
}
