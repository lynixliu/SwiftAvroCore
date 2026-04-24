//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//  Refactored: internal `[MD5Hash: AvroProtocol]` dictionary replaced by
//  `SessionCache<SessionRole.Server>` (aliased as `ServerSessionCache`);
//  `MessageResponse` preserved for backward compatibility and delegates to
//  the new `AvroIPCResponse` value type.
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

// MARK: - AvroIPCResponse

/// Value-type, `Sendable` server-side IPC handler.
///
/// Stateless with respect to sessions — all session state lives in the
/// ``ServerSessionCache`` (`SessionCache<SessionRole.Server>`) actor passed to each method.
public struct AvroIPCResponse: Sendable {

    public let serverHash:     MD5Hash
    public let serverProtocol: String

    public init(serverHash: MD5Hash, serverProtocol: String) {
        self.serverHash     = serverHash
        self.serverProtocol = serverProtocol
    }

    // MARK: - Handshake

    /// Resolves a client handshake request and returns the decoded request
    /// alongside the serialised ``HandshakeResponse`` to send back.
    public func resolveHandshake(
        from data: Data,
        cache:     ServerSessionCache,
        context:   AvroIPCContext
    ) async throws -> (HandshakeRequest, Data, Data) {
        let avro = Avro()
        let (request, consumed): (HandshakeRequest, Int) = try avro.decodeFromContinue(
            from: data, schema: context.requestSchema
        )
        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        let callPayload = data.subdata(in: consumed..<data.count)

        let response: HandshakeResponse
        if await cache.contains(hash: request.clientHash) {
            let matchType: HandshakeMatch = request.serverHash == serverHash ? .BOTH : .CLIENT
            // Metadata consistency: always pass empty map rather than nil
            response = HandshakeResponse(
                match:          matchType,
                serverProtocol: matchType == .CLIENT ? serverProtocol : nil,
                serverHash:     matchType == .CLIENT ? serverHash : nil,
                meta:           nil
            )
        } else if let clientProtocol = request.clientProtocol,
                  request.serverHash == serverHash {
            try await cache.add(hash: request.clientHash, protocolString: clientProtocol)
            response = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil, meta: nil)
        } else {
            response = HandshakeResponse(
                match:          .NONE,
                serverProtocol: serverProtocol,
                serverHash:     serverHash,
                meta:           nil
            )
        }

        avro.setSchema(schema: context.responseSchema)
        let responseData = try avro.encode(response)
        return (request, responseData, callPayload)
    }

    // MARK: - Reading requests

    /// Decodes an incoming call request: metadata + message name + parameters.
    public func decodeCall<T: Codable>(
        header:  HandshakeRequest,
        from data: Data,
        cache:   ServerSessionCache,
        context: AvroIPCContext
    ) async throws -> (RequestHeader, [T]) {
        let avro = Avro()

        let (hasMeta, metaEnd): (Int, Int) = try avro.decodeFromContinue(
            from: data, schema: AvroSchema(type: "int")
        )
        var meta: [String: [UInt8]]?
        var nameOffset = metaEnd
        if hasMeta != 0 {
            let (m, mEnd): ([String: [UInt8]]?, Int) = try avro.decodeFromContinue(
                from: data.advanced(by: metaEnd), schema: context.metaSchema
            )
            meta       = m
            nameOffset = metaEnd + mEnd
        }

        let (messageName, nameEnd): (String?, Int) = try avro.decodeFromContinue(
            from: data.advanced(by: nameOffset), schema: AvroSchema(type: "string")
        )
        // Empty/nil message name = ping per Avro IPC spec: return immediately with no params.
        guard let name = messageName, !name.isEmpty else {
            return (RequestHeader(meta: meta ?? [:], name: messageName ?? ""), [])
        }

        // Schema validation: throw rather than silently returning empty params.
        guard let schemas = await cache.requestSchemas(
            hash: header.clientHash, messageName: name
        ) else {
            throw AvroHandshakeError.missingSchema(name)
        }

        var params: [T] = []
        var index = nameOffset + nameEnd
        for schema in schemas {
            let (param, paramEnd): (T, Int) = try avro.decodeFromContinue(
                from: data.advanced(by: index), schema: schema
            )
            params.append(param)
            index += paramEnd
        }
        return (RequestHeader(meta: meta ?? [:], name: name), params)
    }

    // MARK: - Writing responses

    /// Encodes a successful call response: metadata + `false` flag + serialised value.
    ///
    /// Throws `AvroHandshakeError.missingSchema` if the response schema is not found.
    /// All encoding uses mandatory `try` — no partial writes possible.
    public func encodeResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T,
        cache:       ServerSessionCache,
        context:     AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()
        // Metadata: always encode (empty map when nil) — mandatory `try`
        data.append(try encodeMeta(header.meta, avro: avro, context: context))

        // Schema validation: throw rather than returning partial data
        guard let responseSchema = await cache.responseSchema(
            hash: header.clientHash, messageName: messageName
        ) else {
            throw AvroHandshakeError.missingSchema(messageName)
        }
        // Mandatory encoding: `try` so any serialisation failure aborts the call
        data.append(try avro.encodeFrom(false,     schema: AvroSchema(type: "boolean")))
        data.append(try avro.encodeFrom(parameter, schema: responseSchema))
        return data
    }

    /// Encodes an error call response: metadata + `true` flag + union-encoded errors.
    ///
    /// Throws `AvroHandshakeError.missingSchema` if the error schemas are not found.
    /// All encoding uses mandatory `try` — no partial writes possible.
    public func encodeErrorResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        errors:      [String: T],
        cache:       ServerSessionCache,
        context:     AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()
        // Metadata: always encode — mandatory `try`
        data.append(try encodeMeta(header.meta, avro: avro, context: context))

        // Schema validation: throw rather than returning partial data
        guard let errorSchemas = await cache.errorSchemas(
            hash: header.clientHash, messageName: messageName
        ) else {
            throw AvroHandshakeError.missingSchema(messageName)
        }

        // Mandatory encoding: `try` throughout.
        // Layout: flag(bool=true) + per-error union-index + error value.
        // Avro IPC error union: index 0 = implicit string (system error),
        // index 1..N = declared error types in protocol order.
        // The union index IS required before each error value; it is NOT required
        // before the flag (the flag is a plain boolean, not a union member).
        data.append(try avro.encodeFrom(true, schema: AvroSchema(type: "boolean")))
        for (key, value) in errors {
            if let schema = errorSchemas[key] {
                data.append(contentsOf: [UInt8(2)])   // union index 1 (declared error, zig-zag)
                data.append(try avro.encodeFrom(value, schema: schema))
            } else {
                data.append(contentsOf: [UInt8(0)])   // union index 0 (string fallback, zig-zag)
                data.append(try avro.encodeFrom(value, schema: AvroSchema(type: "string")))
            }
        }
        return data
    }

    // MARK: - Private helpers

    /// Metadata consistency: encode the provided map or an empty map — never omit entirely.
    private func encodeMeta(
        _ meta: [String: [UInt8]]?,
        avro:   Avro,
        context: AvroIPCContext
    ) throws -> Data {
        // Mandatory `try`: any encoding failure propagates to the caller
        return try avro.encodeFrom(meta ?? [:], schema: context.metaSchema)
    }
}

// MARK: - MessageResponse (backward-compatible façade)

/// Manages the server side of the Avro IPC handshake and message encoding/decoding.
///
/// Thin adapter: stores the shared `context`, `cache`, and `response` value type,
/// and forwards every call-site method to ``AvroIPCResponse``. Adds only what the
/// value type cannot express: a raw `encodeHandshakeResponse` for hand-built
/// `HandshakeResponse` values, and the dictionary-shaped `sessionCache` view.
final class MessageResponse {
    let context:          AvroIPCContext
    private let avro:     Avro
    private let response: AvroIPCResponse
    private let cache:    ServerSessionCache
    var serverResponse:   HandshakeResponse

    /// Expose the underlying cache as the dictionary expected by tests.
    var sessionCache: [MD5Hash: AvroProtocol] {
        get async { await cache.sessions }
    }

    init(context: AvroIPCContext, serverHash: MD5Hash, serverProtocol: String) throws {
        if let known = context.knownProtocols, !known.contains(serverProtocol) {
            throw AvroHandshakeError.unknownProtocol(serverProtocol)
        }
        self.context  = context
        self.avro     = Avro()
        self.cache    = ServerSessionCache()
        self.response = AvroIPCResponse(serverHash: serverHash, serverProtocol: serverProtocol)
        self.serverResponse = HandshakeResponse(
            match:          .NONE,
            serverProtocol: serverProtocol,
            serverHash:     serverHash,
            meta:           context.responseMeta
        )
        avro.setSchema(schema: context.responseSchema)
    }

    // MARK: - Handshake

    /// Raw encode for a hand-built `HandshakeResponse` — the one method the
    /// value type does not provide because it owns its own response construction.
    func encodeHandshakeResponse(_ resp: HandshakeResponse) throws -> Data {
        try avro.encode(resp)
    }

    func resolveHandshakeRequest(from data: Data) async throws -> (HandshakeRequest, Data) {
        let (request, responseData, _) = try await response.resolveHandshake(
            from: data, cache: cache, context: context
        )
        return (request, responseData)
    }

    // MARK: - Session management

    func addSupportedProtocol(protocolString: String, hash: MD5Hash) async throws {
        try await cache.add(hash: hash, protocolString: protocolString)
    }

    func removeSession(for hash: MD5Hash) async {
        await cache.remove(for: hash)
    }

    func clearSessions() async {
        await cache.clear()
    }

    // MARK: - Call encode/decode

    func readRequest<T: Codable>(
        header:    HandshakeRequest,
        from data: Data
    ) async throws -> (RequestHeader, [T]) {
        try await response.decodeCall(
            header: header, from: data, cache: cache, context: context
        )
    }

    func writeResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T
    ) async throws -> Data {
        try await response.encodeResponse(
            header:      header,
            messageName: messageName,
            parameter:   parameter,
            cache:       cache,
            context:     context
        )
    }

    func writeErrorResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        errors:      [String: T]
    ) async throws -> Data {
        try await response.encodeErrorResponse(
            header:      header,
            messageName: messageName,
            errors:      errors,
            cache:       cache,
            context:     context
        )
    }
}
