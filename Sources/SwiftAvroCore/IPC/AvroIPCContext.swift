//  AvroIPCContext.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
//  Refactored: internal `[MD5Hash: AvroProtocol]` dictionary replaced by
//  `SessionCache<SessionRole.Client>` (aliased as `ClientSessionCache`);
//  `MessageRequest` preserved for backward compatibility and delegates to
//  the new `AvroIPCRequest` value type.

import Foundation

// MARK: - AvroIPCContext

/// Everything needed for one IPC session: schemas, metadata defaults,
/// known protocol names, and both client- and server-side session caches.
///
/// `AvroIPCContext` is a `class` because it owns two actor-based caches
/// (`clientCache` and `serverCache`) and is intended to be shared across
/// calls within a single session.
///
/// Typical setup:
/// ```swift
/// let context = AvroIPCContext(
///     requestSchema:  avro.decodeSchema(schema: requestSchemaJson)!,
///     responseSchema: avro.decodeSchema(schema: responseSchemaJson)!,
///     metaSchema:     avro.decodeSchema(schema: metaSchemaJson)!
/// )
/// let client = try avro.makeIPCClient(
///     clientHash:     myHash,
///     clientProtocol: "com.example.MyProtocol",
///     context:        context
/// )
/// ```
public final class AvroIPCContext {

    // MARK: Schemas

    /// Schema used to encode/decode ``HandshakeRequest``.
    public let requestSchema:  AvroSchema

    /// Schema used to encode/decode ``HandshakeResponse``.
    public let responseSchema: AvroSchema

    /// Schema used to encode/decode IPC metadata maps (`[String: [UInt8]]`).
    public let metaSchema:     AvroSchema

    // MARK: Metadata defaults

    /// Default metadata attached to outgoing requests. Defaults to empty map.
    public let requestMeta:  [String: [UInt8]]

    /// Default metadata attached to outgoing responses. Defaults to empty map.
    public let responseMeta: [String: [UInt8]]

    // MARK: Protocol validation

    /// Optional set of recognised protocol names. When non-nil, both
    /// ``AvroIPCRequest`` and ``AvroIPCResponse`` validate against this set
    /// at construction time and throw ``AvroHandshakeError/unknownProtocol(_:)``
    /// if the supplied name is not present.
    public let knownProtocols: Set<String>?

    // MARK: Session caches

    /// Caches client-side handshake sessions (hash → protocol).
    public let clientCache: ClientSessionCache

    /// Caches server-side handshake sessions (hash → protocol).
    public let serverCache: ServerSessionCache

    // MARK: Init

    public init(
        requestSchema:  AvroSchema,
        responseSchema: AvroSchema,
        metaSchema:     AvroSchema,
        requestMeta:    [String: [UInt8]] = [:],
        responseMeta:   [String: [UInt8]] = [:],
        knownProtocols: Set<String>?      = nil
    ) {
        self.requestSchema  = requestSchema
        self.responseSchema = responseSchema
        self.metaSchema     = metaSchema
        self.requestMeta    = requestMeta
        self.responseMeta   = responseMeta
        self.knownProtocols = knownProtocols
        self.clientCache    = ClientSessionCache()
        self.serverCache    = ServerSessionCache()
    }
}
