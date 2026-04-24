//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
//  Refactored: internal `[MD5Hash: AvroProtocol]` dictionary replaced by
//  `SessionCache<SessionRole.Client>` (aliased as `ClientSessionCache`);
//  `MessageRequest` preserved for backward compatibility and delegates to
//  the new `AvroIPCRequest` value type.
//
//  Changes:
//  1. Metadata Consistency: empty maps `[:]` are always encoded; nil is no longer
//     passed to HandshakeRequest/HandshakeResponse when meta is logically empty.
//  2. Mandatory Encoding: all `try?` replaced with `try` so partial/corrupt
//     IPC messages are impossible — the call fails fast with a clear error.
//  3. Schema Validation: façade now throws `AvroHandshakeError.missingSchema`
//     instead of silently skipping parameters when no schema is found.
//  4. Concurrency Safety: façade read methods snapshot actor state once, up-front,
//     before any decode logic runs.
//

import Foundation

// MARK: - AvroIPCRequest

/// Value-type, `Sendable` client-side IPC handler.
///
/// Stateless with respect to sessions — all session state lives in the
/// ``ClientSessionCache`` (`SessionCache<SessionRole.Client>`) actor passed to each method.
public struct AvroIPCRequest: Sendable {

    public let clientHash:     MD5Hash
    public let clientProtocol: String

    /// Creates an `AvroIPCRequest`.
    ///
    /// - Parameters:
    ///   - clientHash:     MD5 hash of the client's protocol.
    ///   - clientProtocol: Protocol name string sent during retry handshake.
    ///   - context:        Shared IPC context; when ``AvroIPCContext/knownProtocols``
    ///     is non-nil the name is validated immediately and
    ///     ``AvroHandshakeError/unknownProtocol(_:)`` is thrown if unrecognised.
    public init(
        clientHash:     MD5Hash,
        clientProtocol: String,
        context:        AvroIPCContext? = nil
    ) throws {
        if let known = context?.knownProtocols, !known.contains(clientProtocol) {
            throw AvroHandshakeError.unknownProtocol(clientProtocol)
        }
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
    }

    // MARK: - Handshake encoding

    /// Encodes the initial handshake — `clientProtocol` is null on the first
    /// attempt per the Avro IPC specification.
    /// Meta is always sent as an empty map rather than nil for structural consistency.
    public func encodeInitialHandshake(context: AvroIPCContext) throws -> Data {
        let avro = Avro()
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: nil,
            serverHash:     clientHash,
            meta:           context.requestMeta  // always [:] rather than nil when empty
        ))
    }

    /// Encodes a retry handshake that includes the full client protocol string.
    /// Meta is always sent as an empty map rather than nil for structural consistency.
    public func encodeHandshake(
        serverHash: MD5Hash,
        context:    AvroIPCContext
    ) throws -> Data {
        let avro = Avro()
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            serverHash:     serverHash,
            meta:           context.requestMeta  // always [:] rather than nil when empty
        ))
    }

    // MARK: - Handshake decoding

    /// Decodes the server's handshake response and splits off the remaining payload bytes.
    public func decodeHandshakeResponse(
        from data: Data,
        context:   AvroIPCContext
    ) throws -> (HandshakeResponse, Data) {
        let avro = Avro()
        let (response, next): (HandshakeResponse, Int) = try avro.decodeFromContinue(
            from: data, schema: context.responseSchema
        )
        return (response, data.subdata(in: next ..< data.count))
    }

    /// Resolves a handshake response against `cache`.
    ///
    /// - Returns: Follow-up handshake data if the server responded with `.NONE`,
    ///   or `nil` if the handshake is complete (`.BOTH` / `.CLIENT`).
    public func resolveHandshakeResponse(
        _ response: HandshakeResponse,
        cache:      ClientSessionCache,
        context:    AvroIPCContext
    ) async throws -> Data? {
        switch response.match {
        case .NONE:
            guard let serverHash = response.serverHash else {
                throw AvroHandshakeError.noServerHash
            }
            return try encodeHandshake(serverHash: serverHash, context: context)

        case .CLIENT:
            guard let serverHash     = response.serverHash,
                  let serverProtocol = response.serverProtocol else {
                throw AvroHandshakeError.noServerHash
            }
            try await cache.add(hash: serverHash, protocolString: serverProtocol)
            return nil

        case .BOTH:
            return nil
        }
    }

    // MARK: - Call encoding

    /// Encodes a call request: metadata + message name + serialised parameters.
    ///
    /// Meta is always encoded (as empty map when none supplied) for structural consistency.
    /// All parameter encoding is mandatory — throws immediately on any serialisation failure.
    public func encodeCall<T: Codable>(
        messageName: String,
        parameters:  [T],
        serverHash:  MD5Hash,
        cache:       ClientSessionCache,
        context:     AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()

        // Metadata: always encode (empty map when none) — mandatory `try`
        data.append(try avro.encodeFrom(context.requestMeta, schema: context.metaSchema))
        data.append(try avro.encodeFrom(messageName, schema: AvroSchema(type: "string")))

        // Empty message name = ping per Avro IPC spec: no schema lookup, no parameters.
        guard !messageName.isEmpty else { return data }

        // Schema validation: throw if schema is missing rather than silently skipping
        guard let schemas = await cache.requestSchemas(hash: serverHash, messageName: messageName) else {
            throw AvroHandshakeError.missingSchema(messageName)
        }
        for (schema, parameter) in zip(schemas, parameters) {
            // Mandatory encoding: `try` (not `try?`) so partial writes are impossible
            data.append(try avro.encodeFrom(parameter, schema: schema))
        }
        return data
    }

    // MARK: - Response decoding

    /// Decodes a two-way response: metadata + error flag + response/error payload.
    public func decodeResponse<T: Codable>(
        messageName: String,
        from data:   Data,
        serverHash:  MD5Hash,
        cache:       ClientSessionCache,
        context:     AvroIPCContext
    ) async throws -> (ResponseHeader, [T]) {
        let avro = Avro()

        let (hasMeta, metaEnd): (Int, Int) = try avro.decodeFromContinue(
            from: data, schema: AvroSchema(type: "int")
        )
        var meta: [String: [UInt8]]?
        var flagOffset = metaEnd
        if hasMeta != 0 {
            let (m, mEnd): ([String: [UInt8]]?, Int) = try avro.decodeFromContinue(
                from: data.advanced(by: metaEnd), schema: context.metaSchema
            )
            meta       = m
            flagOffset = metaEnd + mEnd
        }

        let (flag, payloadOffset): (Bool, Int) = try avro.decodeFromContinue(
            from: data.advanced(by: flagOffset), schema: AvroSchema(type: "boolean")
        )
        let absolutePayload = flagOffset + payloadOffset
        var params: [T] = []

        if flag {
            if let errorSchemas = await cache.errorSchemas(hash: serverHash, messageName: messageName) {
                var index = absolutePayload
                for (_, errorSchema) in errorSchemas {
                    let (unionIndex, unionEnd): (Int, Int) = try avro.decodeFromContinue(
                        from: data.advanced(by: index), schema: AvroSchema(type: "long")
                    )
                    index += unionEnd
                    let schema = unionIndex > 0 ? errorSchema : AvroSchema(type: "string")
                    let (param, paramEnd): (T, Int) = try avro.decodeFromContinue(
                        from: data.advanced(by: index), schema: schema
                    )
                    params.append(param)
                    index += paramEnd
                }
            }
        } else {
            // Schema validation + mandatory throws: no try? — decode failure aborts cleanly
            guard let responseSchema = await cache.responseSchema(
                hash: serverHash, messageName: messageName
            ) else {
                throw AvroHandshakeError.missingSchema(messageName)
            }
            let param: T = try avro.decodeFrom(
                from: data.advanced(by: absolutePayload), schema: responseSchema
            )
            params.append(param)
        }
        return (ResponseHeader(meta: meta ?? [:], flag: flag), params)
    }
}

// MARK: - MessageRequest (backward-compatible façade)

/// Manages the client side of the Avro IPC handshake and message encoding/decoding.
///
/// Thin adapter: stores the shared `context`, `cache`, and `request` value type,
/// and forwards every call-site method to ``AvroIPCRequest``. Adds only what the
/// value type cannot express: a raw `encodeHandshakeRequest` for hand-built
/// `HandshakeRequest` values, and the dictionary-shaped `sessionCache` view.
final class MessageRequest {
    let context:         AvroIPCContext
    private let avro:    Avro
    private let request: AvroIPCRequest
    private let cache:   ClientSessionCache
    var clientRequest:   HandshakeRequest

    /// Expose the underlying cache as the dictionary expected by tests.
    var sessionCache: [MD5Hash: AvroProtocol] {
        get async { await cache.sessions }
    }

    init(context: AvroIPCContext, clientHash: MD5Hash, clientProtocol: String) throws {
        self.context = context
        self.avro    = Avro()
        self.cache   = ClientSessionCache()
        // AvroIPCRequest validates clientProtocol against context.knownProtocols.
        self.request = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: clientProtocol, context: context
        )
        self.clientRequest = HandshakeRequest(
            clientHash: clientHash,
            clientProtocol: clientProtocol,
            serverHash: clientHash,
            meta:           context.requestMeta
        )
        avro.setSchema(schema: context.requestSchema)
    }

    // MARK: - Handshake

    /// Raw encode for a hand-built `HandshakeRequest` — the one method the
    /// value type does not provide because it owns its own request construction.
    func encodeHandshakeRequest(_ req: HandshakeRequest) throws -> Data {
        try avro.encode(req)
    }

    func initHandshakeRequest() throws -> Data {
        try request.encodeInitialHandshake(context: context)
    }

    func decodeResponse(from data: Data) throws -> (HandshakeResponse, Data) {
        try request.decodeHandshakeResponse(from: data, context: context)
    }

    func resolveHandshakeResponse(_ response: HandshakeResponse) async throws -> Data? {
        try await request.resolveHandshakeResponse(response, cache: cache, context: context)
    }

    // MARK: - Session management

    func addSession(hash: MD5Hash, protocolString: String) async throws {
        try await cache.add(hash: hash, protocolString: protocolString)
    }

    func removeSession(for hash: MD5Hash) async {
        await cache.remove(for: hash)
    }

    func clearSessions() async {
        await cache.clear()
    }

    // MARK: - Call encode/decode

    func writeRequest<T: Codable>(messageName: String, parameters: [T]) async throws -> Data {
        try await request.encodeCall(
            messageName: messageName,
            parameters:  parameters,
            serverHash:  clientRequest.serverHash,
            cache:       cache,
            context:     context
        )
    }

    func readResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        from data:   Data
    ) async throws -> (ResponseHeader, [T]) {
        try await request.decodeResponse(
            messageName: messageName,
            from:        data,
            serverHash:  header.serverHash,
            cache:       cache,
            context:     context
        )
    }
}
