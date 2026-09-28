@testable import NexusKb
import Vapor
import VaporTesting
import Testing

@Suite("Read API routing tests")
struct ReadAPIRoutingTests {
    @Test("Thread timing preserves responses and errors")
    func threadTiming() async throws {
        try await withApp { app in
            let timed = app.grouped(ThreadLoadTiming())
            timed.get("timed") { _ async throws -> String in
                try await Task.sleep(for: .milliseconds(10))
                return "message body"
            }
            timed.get("failed") { _ async throws -> String in
                throw Abort(.notFound, reason: "Thread not found")
            }

            try await app.testing().test(.GET, "/timed") { response async throws in
                #expect(response.status == .ok)
                #expect(response.body.string == "message body")
                let header = try #require(response.headers.first(name: "Server-Timing"))
                #expect(header.hasPrefix("thread_load;dur="))
                let elapsed = try #require(Double(header.dropFirst("thread_load;dur=".count)))
                #expect(elapsed >= 10)
            }
            try await app.testing().test(.GET, "/failed") { response async in
                #expect(response.status == .notFound)
                #expect(response.body.string.contains("Thread not found"))
            }
        }
    }

    @Test(
        "Encoded Message-ID remains one path component"
    )
    func encodedMessageIDPath() async throws {
        let messageID =
            "message/path?query#fragment%value@example.com"
        let encoded = try #require(
            messageID.addingPercentEncoding(
                withAllowedCharacters:
                    .messageIDPathAllowed
            )
        )

        try await withApp { app in
            app.get(
                "echo",
                ":messageID"
            ) { req async throws -> String in
                try req.messageIdentifier(
                    parameter: "messageID"
                ).value
            }

            try await app.testing().test(
                .GET,
                "/echo/\(encoded)"
            ) { response async in
                #expect(response.status == .ok)
                #expect(response.body.string == messageID)
            }
        }
    }
}

private extension CharacterSet {
    static var messageIDPathAllowed: CharacterSet {
        var value = CharacterSet.urlPathAllowed
        value.remove(charactersIn: "/?#%")
        return value
    }
}
