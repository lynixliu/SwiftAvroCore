//
//  ClientSessionCache.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//

import Foundation

// MARK: - ClientSessionCache

/// Actor that owns the client-side session map.
/// Tracks server protocols the client has learned during handshake.
public actor ClientSessionCache {

    private var sessions: [MD5Hash: AvroProtocol] = [:]

    public init() {}

    /// Registers a server protocol string under its MD5 hash.
    public func add(hash: MD5Hash, protocolString: String) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        sessions[hash] = try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    /// Returns the protocol for the given hash, or nil if not found.
    public func avroProtocol(for hash: MD5Hash) -> AvroProtocol? {
        sessions[hash]
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
