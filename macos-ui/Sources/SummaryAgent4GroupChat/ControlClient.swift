import Foundation
import Network

struct ControlError: Decodable, Error, LocalizedError {
    let code: String
    let message: String
    let detail: String?
    let retryable: Bool

    var errorDescription: String? {
        detail.map { "\(message): \($0)" } ?? message
    }
}

private struct ControlRequest: Encodable {
    let version = 1
    let id = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    let method: String
    let token: String
    let params: [String: String]
}

private struct ControlResponse<Result: Decodable>: Decodable {
    let result: Result?
    let error: ControlError?
}

struct AgentStatus: Decodable {
    let agent_running: Bool
    let agent_pid: Int?
    let platform: String
    let targets: Int
    let config_path: String
    let working_dir: String
    let llm_configured: Bool
    let image_configured: Bool
    let wxdb_configured: Bool
}

struct AgentAction: Decodable {
    let started: Bool?
    let stopped: Bool?
    let pid: Int?
    let message: String?
}

struct LogTail: Decodable {
    let path: String
    let text: String
}

final class ControlClient {
    private let socketPath: String
    private let token: String

    init(socketPath: String, token: String) {
        self.socketPath = socketPath
        self.token = token
    }

    func call<Result: Decodable>(
        _ method: String,
        params: [String: String] = [:],
        as type: Result.Type
    ) async throws -> Result {
        let request = ControlRequest(method: method, token: token, params: params)
        let requestData = try JSONEncoder().encode(request) + Data([0x0A])
        let connection = NWConnection(to: .unix(path: socketPath), using: .tcp)
        defer { connection.cancel() }

        try await connect(connection)
        try await send(requestData, over: connection)
        let data = try await receiveLine(over: connection)
        let response = try JSONDecoder().decode(ControlResponse<Result>.self, from: data)
        if let error = response.error {
            throw error
        }
        guard let result = response.result else {
            throw CocoaError(.fileReadCorruptFile)
        }
        return result
    }

    private func connect(_ connection: NWConnection) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    continuation.resume()
                case .failed(let error):
                    continuation.resume(throwing: error)
                default:
                    break
                }
            }
            connection.start(queue: .global(qos: .userInitiated))
        }
    }

    private func send(_ data: Data, over connection: NWConnection) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.send(content: data, completion: .contentProcessed { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            })
        }
    }

    private func receiveLine(over connection: NWConnection) async throws -> Data {
        var accumulated = Data()
        while accumulated.count <= 2 * 1024 * 1024 {
            let data = try await receive(over: connection)
            accumulated.append(data)
            if let newline = accumulated.firstIndex(of: 0x0A) {
                return accumulated.prefix(upTo: newline)
            }
        }
        throw CocoaError(.fileReadTooLarge)
    }

    private func receive(over connection: NWConnection) async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) {
                data, _, complete, error in
                if let error {
                    continuation.resume(throwing: error)
                } else if let data, !data.isEmpty {
                    continuation.resume(returning: data)
                } else if complete {
                    continuation.resume(throwing: CocoaError(.fileReadNoSuchFile))
                } else {
                    continuation.resume(throwing: CocoaError(.fileReadUnknown))
                }
            }
        }
    }
}
