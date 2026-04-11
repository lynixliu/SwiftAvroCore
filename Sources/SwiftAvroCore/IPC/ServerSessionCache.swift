//
//  ServerSessionCache.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
//  Replaces the `[MD5Hash: AvroProtocol]` dictionary that was embedded directly
//  inside MessageResponse. Extracted into a standalone actor for the same
//  reasons as ClientSessionCache.

import Foundation

// MARK: - ServerSessionCache

/// Actor-isolated map of client protocols the server has accepted during handshake.
///
/// Previously this was a plain `[MD5Hash: AvroProtocol]` property on `MessageResponse`.
public actor ServerSessionCache {

    var sessions: [MD5Hash: AvroProtocol] = [:]

    public init() {}

    // MARK: - Mutation

    /// Parses `protocolString` as an ``AvroProtocol`` and stores it under `hash`.
    /// Throws if the string is not valid UTF-8 or valid Avro protocol JSON.
    public func add(hash: MD5Hash, protocolString: String) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        sessions[hash] = try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    /// Removes the session registered under `hash`.
    public func remove(for hash: MD5Hash) {
        sessions.removeValue(forKey: hash)
    }

    /// Removes all sessions.
    public func clear() {
        sessions.removeAll()
    }

    // MARK: - Query

    /// Returns `true` if a session is registered for `hash`.
    public func contains(hash: MD5Hash) -> Bool {
        sessions[hash] != nil
    }

    /// Returns the request schemas for `messageName`, or `nil` if unknown.
    public func requestSchemas(hash: MD5Hash, messageName: String) -> [AvroSchema]? {
        sessions[hash]?.getRequest(messageName: messageName)
    }

    /// Returns the response schema for `messageName`, or `nil` if unknown.
    public func responseSchema(hash: MD5Hash, messageName: String) -> AvroSchema? {
        sessions[hash]?.getResponse(messageName: messageName)
    }

    /// Returns the error schemas keyed by error name, or `nil` if unknown.
    public func errorSchemas(hash: MD5Hash, messageName: String) -> [String: AvroSchema]? {
        sessions[hash]?.getErrors(messageName: messageName)
    }

    // MARK: - Internal synchronous helpers
    // Used only by the MessageResponse backward-compatible façade.
    private var _sessions: [MD5Hash: AvroProtocol] { sessions }

    func syncContains(hash: MD5Hash) -> Bool {
        sessions[hash] != nil
    }

    func syncGet(hash: MD5Hash) -> AvroProtocol? {
        sessions[hash]
    }

    func setDirectly(hash: MD5Hash, proto: AvroProtocol) {
        sessions[hash] = proto
    }
}
