//
//  ServerSessionCache.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//

import Foundation

// MARK: - ServerSessionCache

/// Actor that owns the server-side session map.
/// Tracks client protocols the server has accepted during handshake.
public actor ServerSessionCache {

    private var sessions: [MD5Hash: AvroProtocol] = [:]

    public init() {}

    /// Registers a client protocol string under its MD5 hash.
    public func add(hash: MD5Hash, protocolString: String) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        sessions[hash] = try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    /// Returns true if the given hash is a known client.
    public func contains(hash: MD5Hash) -> Bool {
        sessions[hash] != nil
    }

    /// Returns the request schemas for a message in the cached protocol.
    public func requestSchemas(hash: MD5Hash, messageName: String) -> [AvroSchema]? {
        sessions[hash]?.getRequest(messageName: messageName)
    }

    /// Returns the response schema for a message in the cached protocol.
    public func responseSchema(hash: MD5Hash, messageName: String) -> AvroSchema? {
        sessions[hash]?.getResponse(messageName: messageName)
    }

    /// Returns the error schemas for a message in the cached protocol.
    public func errorSchemas(
        hash: MD5Hash,
        messageName: String
    ) -> [String: AvroSchema]? {
        sessions[hash]?.getErrors(messageName: messageName)
    }

    /// Removes the session for the given hash.
    public func remove(for hash: MD5Hash) {
        sessions.removeValue(forKey: hash)
    }

    /// Clears all sessions.
    public func clear() {
        sessions.removeAll()
    }
}
