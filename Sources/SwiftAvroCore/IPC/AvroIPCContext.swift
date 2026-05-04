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

/// Immutable protocol configuration shared across all sessions.
///
/// Create once and reuse across many ``AvroIPCSession`` instances —
/// e.g. a server handling multiple clients with the same protocol.
///
/// ```swift
/// let context = AvroIPCContext(
///     requestSchema:  avro.decodeSchema(schema: requestJson)!,
///     responseSchema: avro.decodeSchema(schema: responseJson)!,
///     metaSchema:     avro.decodeSchema(schema: metaJson)!
/// )
/// ```
public struct AvroIPCContext: Sendable {

    /// Schema used to encode/decode ``HandshakeRequest``.
    public let requestSchema:  AvroSchema

    /// Schema used to encode/decode ``HandshakeResponse``.
    public let responseSchema: AvroSchema

    /// Schema used to encode/decode IPC metadata maps (`[String: [UInt8]]`).
    public let metaSchema:     AvroSchema

    /// Default metadata attached to outgoing requests. Defaults to empty map.
    public let requestMeta:    [String: [UInt8]]

    /// Default metadata attached to outgoing responses. Defaults to empty map.
    public let responseMeta:   [String: [UInt8]]

    /// Optional set of recognised protocol names. When non-nil, both
    /// ``AvroIPCRequest`` and ``AvroIPCResponse`` validate against this set
    /// at construction time and throw ``AvroHandshakeError/unknownProtocol(_:)``
    /// if the supplied name is not present.
    public let knownProtocols: Set<String>?

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
    }

    /// Creates an `AvroIPCContext` using the standard Avro IPC handshake schemas
    /// from ``MessageConstant``. Use this instead of the memberwise initialiser
    /// to avoid schema-order mistakes (e.g. wrong `HandshakeMatch` symbol order).
    public static func standard(
        avro:           Avro,
        requestMeta:    [String: [UInt8]] = [:],
        responseMeta:   [String: [UInt8]] = [:],
        knownProtocols: Set<String>?      = nil
    ) throws -> AvroIPCContext {
        guard let req  = avro.newSchema(schema: MessageConstant.requestSchema),
              let resp = avro.newSchema(schema: MessageConstant.responseSchema),
              let meta = avro.newSchema(schema: MessageConstant.metadataSchema)
        else { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }
        return AvroIPCContext(
            requestSchema:  req,
            responseSchema: resp,
            metaSchema:     meta,
            requestMeta:    requestMeta,
            responseMeta:   responseMeta,
            knownProtocols: knownProtocols
        )
    }
}

// MARK: - AvroIPCSession

/// Per-connection session state: a shared immutable context plus the two
/// actor-based caches that accumulate handshake state over the lifetime
/// of one client–server connection.
///
/// ```swift
/// // Shared across all connections — created once
/// let context = AvroIPCContext(...)
///
/// // Created per connection
/// let session = AvroIPCSession(context: context)
///
/// let client = try avro.makeIPCRequest(clientHash: hash, clientProtocol: proto, session: session)
/// let server = avro.makeIPCResponse(serverHash: hash, serverProtocol: proto)
///
/// let handshake = try client.encodeInitialHandshake(avro: avro, session: session)
/// let (req, responseData, payload) = try await server.resolveHandshake(
///     avro: avro, from: data, session: session
/// )
/// ```
public final class AvroIPCSession: Sendable {

    /// The immutable protocol configuration for this session.
    public let context:     AvroIPCContext

    /// Caches client-side handshake sessions (hash → protocol).
    public let clientCache: ClientSessionCache

    /// Caches server-side handshake sessions (hash → protocol).
    public let serverCache: ServerSessionCache

    public init(context: AvroIPCContext) {
        self.context     = context
        self.clientCache = ClientSessionCache()
        self.serverCache = ServerSessionCache()
    }
}
