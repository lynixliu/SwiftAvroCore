//
//  ClientSessionCache.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
//  Replaces the `[MD5Hash: AvroProtocol]` dictionary that was embedded directly
//  inside MessageRequest. Extracted into a standalone actor so the session map
//  is properly isolated and can be safely shared across concurrency contexts.

import Foundation

// MARK: - ClientSessionCache

/// Actor-isolated map of server protocols the client has learned during handshake.
///
/// Previously this was a plain `[MD5Hash: AvroProtocol]` property on `MessageRequest`.
/// Promoting it to an actor means the map is safe to access from any task without
/// `@unchecked Sendable` workarounds.
public actor ClientSessionCache {

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

    /// Returns the ``AvroProtocol`` for `hash`, or `nil` if not found.
    public func avroProtocol(for hash: MD5Hash) -> AvroProtocol? {
        sessions[hash]
    }

    /// Returns the request schemas for `messageName` in the protocol registered
    /// under `hash`, or `nil` if the session or message is unknown.
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
    // Used only by the MessageRequest backward-compatible façade, which must
    // perform synchronous lookups from non-async call sites.
    // These are intentionally not public.
    private var _sessions: [MD5Hash: AvroProtocol] { sessions }

    func syncGet(hash: MD5Hash) -> AvroProtocol? {
        sessions[hash]
    }

    func setDirectly(hash: MD5Hash, proto: AvroProtocol) {
        sessions[hash] = proto
    }
}
