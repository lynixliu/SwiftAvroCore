//
//  SessionCache.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
//  Replaces the near-identical ClientSessionCache and ServerSessionCache actors
//  with a single generic SessionCache<Role> actor. The Role phantom type enforces
//  at compile time that client-only and server-only APIs are only reachable on
//  the correct side, while all shared cache logic lives in exactly one place.
//
//  Public API is fully backward-compatible:
//    ClientSessionCache  →  SessionCache<SessionRole.Client>
//    ServerSessionCache  →  SessionCache<SessionRole.Server>
//
//  Both typealiases are preserved so call-sites need no changes.
//

import Foundation

// MARK: - Role marker types

/// Namespace for the phantom types that parameterise ``SessionCache``.
public enum SessionRole {
    /// Marker for the client side of an IPC session.
    public enum Client {}
    /// Marker for the server side of an IPC session.
    public enum Server {}
}

// MARK: - Unified SessionCache actor

/// Actor-isolated map of ``AvroProtocol`` values, keyed by ``MD5Hash``.
///
/// The phantom `Role` type parameter is never stored at runtime; it only
/// restricts which role-specific extension methods are visible to callers.
///
/// Use the ``ClientSessionCache`` or ``ServerSessionCache`` typealiases instead
/// of spelling out the full generic type.
public actor SessionCache<Role> {

    var sessions: [MD5Hash: AvroProtocol] = [:]

    public init() {}

    // MARK: - Mutation (shared)

    /// Parses `protocolString` as an ``AvroProtocol`` and stores it under `hash`.
    /// Throws if the string is not valid UTF-8 or not valid Avro protocol JSON.
    public func add(hash: MD5Hash, protocolString: String) throws {
        let data = Data(protocolString.utf8)
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

    // MARK: - Query (shared)

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
}

// MARK: - Client-side extensions

extension SessionCache where Role == SessionRole.Client {

    /// Returns the ``AvroProtocol`` for `hash`, or `nil` if not found.
    ///
    /// Available on ``ClientSessionCache`` only.
    public func avroProtocol(for hash: MD5Hash) -> AvroProtocol? {
        sessions[hash]
    }
}

// MARK: - Server-side extensions

extension SessionCache where Role == SessionRole.Server {

    /// Returns `true` if a session is registered for `hash`.
    ///
    /// Available on ``ServerSessionCache`` only.
    public func contains(hash: MD5Hash) -> Bool {
        sessions[hash] != nil
    }
}

// MARK: - Typealiases (backward-compatible public API)

/// Actor-isolated map of server protocols the client has learned during handshake.
///
/// Drop-in replacement for the old `ClientSessionCache` actor.
public typealias ClientSessionCache = SessionCache<SessionRole.Client>

/// Actor-isolated map of client protocols the server has accepted during handshake.
///
/// Drop-in replacement for the old `ServerSessionCache` actor.
public typealias ServerSessionCache = SessionCache<SessionRole.Server>
