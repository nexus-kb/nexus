import Vapor

/// Timings cover the handler and JSON encoding, but not socket transfer or rendering.
struct ThreadLoadTiming: AsyncMiddleware {
    static func milliseconds(since start: ContinuousClock.Instant) -> Double {
        let duration = start.duration(to: .now).components
        return Double(duration.seconds) * 1_000
            + Double(duration.attoseconds) / 1_000_000_000_000_000
    }

    func respond(
        to request: Request,
        chainingTo next: any AsyncResponder
    ) async throws -> Response {
        let started = ContinuousClock.now
        do {
            let response = try await next.respond(to: request)
            let elapsed = Self.milliseconds(since: started)
            response.headers.add(
                name: "Server-Timing",
                value: "thread_load;dur=\(elapsed)"
            )
            request.logger.info("Thread response encoded", metadata: [
                "duration_ms": .stringConvertible(elapsed),
                "status": .stringConvertible(response.status.code),
                "response_bytes": .stringConvertible(response.body.count)
            ])
            return response
        } catch {
            request.logger.warning("Thread request failed", metadata: [
                "duration_ms": .stringConvertible(Self.milliseconds(since: started))
            ])
            throw error
        }
    }
}
