import Vapor

struct AdminIdentity: Authenticatable {}

struct AdminTokenAuthenticator: AsyncBearerAuthenticator {
    private let token: [UInt8]

    init(token: String?) throws {
        guard let token,
              token.utf8.count == 64,
              token.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) })
        else {
            throw Abort(.internalServerError, reason:
                "NEXUS_ADMIN_TOKEN must be 64 lowercase hexadecimal characters (openssl rand -hex 32)")
        }
        self.token = Array(token.utf8)
    }

    func authenticate(bearer: BearerAuthorization, for request: Request) async throws {
        if token.secureCompare(to: bearer.token.utf8) {
            request.auth.login(AdminIdentity())
        }
    }
}
