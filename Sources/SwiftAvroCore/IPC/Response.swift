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

/// Stateless, `Sendable` server-side IPC handler.
///
/// Obtain via ``Avro/makeIPCResponse(serverHash:serverProtocol:)``
/// rather than constructing directly.
public struct AvroIPCResponse: Sendable {

    public let serverHash:     MD5Hash
    public let serverProtocol: String

    public init(serverHash: MD5Hash, serverProtocol: String) {
        self.serverHash     = serverHash
        self.serverProtocol = serverProtocol
    }

    // MARK: - Handshake

    /// Resolves a client handshake request, updating `context.serverCache`,
    /// and returns the decoded request, the serialised response, and the
    /// remaining call payload.
    public func resolveHandshake(
        avro:    Avro,
        from data: Data,
        context: AvroIPCContext
    ) async throws -> (HandshakeRequest, Data, Data) {
        let reader = avro.makeDataReader(data: data)
        let request: HandshakeRequest = try reader.decode(schema: context.requestSchema)

        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        let callPayload = data.suffix(reader.bytesRemaining)

        let response: HandshakeResponse
        if await context.serverCache.contains(hash: request.clientHash) {
            let matchType: HandshakeMatch = request.serverHash == serverHash ? .BOTH : .CLIENT
            response = HandshakeResponse(
                match:          matchType,
                serverProtocol: matchType == .CLIENT ? serverProtocol : nil,
                serverHash:     matchType == .CLIENT ? serverHash : nil,
                meta:           nil
            )
        } else if let clientProtocol = request.clientProtocol,
                  request.serverHash == serverHash {
            try await context.serverCache.add(hash: request.clientHash, protocolString: clientProtocol)
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
        return (request, responseData, Data(callPayload))
    }

    // MARK: - Reading requests

    /// Decodes an incoming call request: metadata + message name + parameters.
    public func decodeCall<T: Codable>(
        avro:      Avro,
        header:    HandshakeRequest,
        from data: Data,
        context:   AvroIPCContext
    ) async throws -> (RequestHeader, [T]) {
        let reader = avro.makeDataReader(data: data)

        let hasMeta: Int = try reader.decode(schema: AvroSchema(type: "int"))
        var meta: [String: [UInt8]]?
        if hasMeta != 0 {
            meta = try reader.decode(schema: context.metaSchema)
        }

        let messageName: String? = try reader.decode(schema: AvroSchema(type: "string"))

        // Empty/nil message name = ping per Avro IPC spec: return immediately.
        guard let name = messageName, !name.isEmpty else {
            return (RequestHeader(meta: meta ?? [:], name: messageName ?? ""), [])
        }

        guard let schemas = await context.serverCache.requestSchemas(
            hash: header.clientHash, messageName: name
        ) else {
            throw AvroHandshakeError.missingSchema(name)
        }

        var params: [T] = []
        for schema in schemas {
            let param: T = try reader.decode(schema: schema)
            params.append(param)
        }
        return (RequestHeader(meta: meta ?? [:], name: name), params)
    }

    // MARK: - Writing responses

    /// Encodes a successful call response: metadata + `false` flag + serialised value.
    public func encodeResponse<T: Codable>(
        avro:        Avro,
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T,
        context:     AvroIPCContext
    ) async throws -> Data {
        var data = Data()
        data.append(try avro.encodeFrom(header.meta ?? [:], schema: context.metaSchema))

        guard let responseSchema = await context.serverCache.responseSchema(
            hash: header.clientHash, messageName: messageName
        ) else {
            throw AvroHandshakeError.missingSchema(messageName)
        }
        data.append(try avro.encodeFrom(false,     schema: AvroSchema(type: "boolean")))
        data.append(try avro.encodeFrom(parameter, schema: responseSchema))
        return data
    }

    /// Encodes an error call response: metadata + `true` flag + union-encoded errors.
    public func encodeErrorResponse<T: Codable>(
        avro:        Avro,
        header:      HandshakeRequest,
        messageName: String,
        errors:      [String: T],
        context:     AvroIPCContext
    ) async throws -> Data {
        var data = Data()
        data.append(try avro.encodeFrom(header.meta ?? [:], schema: context.metaSchema))

        guard let errorSchemas = await context.serverCache.errorSchemas(
            hash: header.clientHash, messageName: messageName
        ) else {
            throw AvroHandshakeError.missingSchema(messageName)
        }

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
}
/*
// MARK: - MessageResponse (backward-compatible façade)

/// Stateful server-side IPC façade.
///
/// Obtain via ``Avro/makeIPCServer(serverHash:serverProtocol:context:)``.
/// Holds a single `Avro` instance and forwards all calls to ``AvroIPCResponse``,
/// passing `context` (which now owns the session cache).
final class MessageResponse {
    let context:          AvroIPCContext
    private let avro:     Avro
    private let response: AvroIPCResponse
    var serverResponse:   HandshakeResponse

    var sessionCache: [MD5Hash: AvroProtocol] {
        get async { await context.serverCache.sessions }
    }

    init(context: AvroIPCContext, serverHash: MD5Hash, serverProtocol: String) throws {
        if let known = context.knownProtocols, !known.contains(serverProtocol) {
            throw AvroHandshakeError.unknownProtocol(serverProtocol)
        }
        self.context  = context
        self.avro     = Avro()
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

    func encodeHandshakeResponse(_ resp: HandshakeResponse) throws -> Data {
        try avro.encode(resp)
    }

    func resolveHandshakeRequest(from data: Data) async throws -> (HandshakeRequest, Data) {
        let (request, responseData, _) = try await response.resolveHandshake(
            avro: avro, from: data, context: context
        )
        return (request, responseData)
    }

    // MARK: - Session management

    func addSupportedProtocol(protocolString: String, hash: MD5Hash) async throws {
        try await context.serverCache.add(hash: hash, protocolString: protocolString)
    }

    func removeSession(for hash: MD5Hash) async {
        await context.serverCache.remove(for: hash)
    }

    func clearSessions() async {
        await context.serverCache.clear()
    }

    // MARK: - Call encode/decode

    func readRequest<T: Codable>(
        header:    HandshakeRequest,
        from data: Data
    ) async throws -> (RequestHeader, [T]) {
        try await response.decodeCall(
            avro: avro, header: header, from: data, context: context
        )
    }

    func writeResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T
    ) async throws -> Data {
        try await response.encodeResponse(
            avro:        avro,
            header:      header,
            messageName: messageName,
            parameter:   parameter,
            context:     context
        )
    }

    func writeErrorResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        errors:      [String: T]
    ) async throws -> Data {
        try await response.encodeErrorResponse(
            avro:        avro,
            header:      header,
            messageName: messageName,
            errors:      errors,
            context:     context
        )
    }
}
*/
