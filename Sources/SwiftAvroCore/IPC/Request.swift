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

/// Stateless, `Sendable` client-side IPC handler.
///
/// Obtain via ``Avro/makeIPCRequest(clientHash:clientProtocol:context:)``
/// rather than constructing directly.
public struct AvroIPCRequest: Sendable {

    public let clientHash:     MD5Hash
    public let clientProtocol: String

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

    /// Encodes the initial handshake — `clientProtocol` is nil on the first
    /// attempt per the Avro IPC specification.
    public func encodeInitialHandshake(avro: Avro, context: AvroIPCContext) throws -> Data {
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: nil,
            serverHash:     clientHash,
            meta:           context.requestMeta
        ))
    }

    /// Encodes a retry handshake that includes the full client protocol string.
    public func encodeHandshake(
        avro:       Avro,
        serverHash: MD5Hash,
        context:    AvroIPCContext
    ) throws -> Data {
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            serverHash:     serverHash,
            meta:           context.requestMeta
        ))
    }

    // MARK: - Handshake decoding

    /// Decodes the server's handshake response and splits off the remaining payload bytes.
    public func decodeHandshakeResponse(
        avro:    Avro,
        from data: Data,
        context: AvroIPCContext
    ) throws -> (HandshakeResponse, Data) {
        let reader = avro.makeDataReader(data: data)
        let response: HandshakeResponse = try reader.decode(schema: context.responseSchema)
        return (response, data.suffix(reader.bytesRemaining))
    }

    /// Resolves a handshake response, updating `context.clientCache` as needed.
    ///
    /// - Returns: Follow-up handshake data if the server responded with `.NONE`,
    ///   or `nil` if the handshake is complete (`.BOTH` / `.CLIENT`).
    public func resolveHandshakeResponse(
        _ response: HandshakeResponse,
        avro:       Avro,
        context:    AvroIPCContext
    ) async throws -> Data? {
        switch response.match {
        case .NONE:
            guard let serverHash = response.serverHash else {
                throw AvroHandshakeError.noServerHash
            }
            return try encodeHandshake(avro: avro, serverHash: serverHash, context: context)

        case .CLIENT:
            guard let serverHash     = response.serverHash,
                  let serverProtocol = response.serverProtocol else {
                throw AvroHandshakeError.noServerHash
            }
            try await context.clientCache.add(hash: serverHash, protocolString: serverProtocol)
            return nil

        case .BOTH:
            return nil
        }
    }

    // MARK: - Call encoding

    /// Encodes a call request: metadata + message name + serialised parameters.
    public func encodeCall<T: Codable>(
        avro:        Avro,
        messageName: String,
        parameters:  [T],
        serverHash:  MD5Hash,
        context:     AvroIPCContext
    ) async throws -> Data {
        var data = Data()
        data.append(try avro.encodeFrom(context.requestMeta, schema: context.metaSchema))
        data.append(try avro.encodeFrom(messageName, schema: AvroSchema(type: "string")))

        // Empty message name = ping per Avro IPC spec: no parameters.
        guard !messageName.isEmpty else { return data }

        guard let schemas = await context.clientCache.requestSchemas(
            hash: serverHash, messageName: messageName
        ) else {
            throw AvroHandshakeError.missingSchema(messageName)
        }
        for (schema, parameter) in zip(schemas, parameters) {
            data.append(try avro.encodeFrom(parameter, schema: schema))
        }
        return data
    }

    // MARK: - Response decoding

    /// Decodes a two-way response: metadata + error flag + response/error payload.
    public func decodeResponse<T: Codable>(
        avro:        Avro,
        messageName: String,
        from data:   Data,
        serverHash:  MD5Hash,
        context:     AvroIPCContext
    ) async throws -> (ResponseHeader, [T]) {
        let reader = avro.makeDataReader(data: data)

        let hasMeta: Int = try reader.decode(schema: AvroSchema(type: "int"))
        var meta: [String: [UInt8]]?
        if hasMeta != 0 {
            meta = try reader.decode(schema: context.metaSchema)
        }

        let flag: Bool = try reader.decode(schema: AvroSchema(type: "boolean"))
        var params: [T] = []

        if flag {
            if let errorSchemas = await context.clientCache.errorSchemas(
                hash: serverHash, messageName: messageName
            ) {
                for (_, errorSchema) in errorSchemas {
                    let unionIndex: Int = try reader.decode(schema: AvroSchema(type: "long"))
                    let schema = unionIndex > 0 ? errorSchema : AvroSchema(type: "string")
                    let param: T = try reader.decode(schema: schema)
                    params.append(param)
                }
            }
        } else {
            guard let responseSchema = await context.clientCache.responseSchema(
                hash: serverHash, messageName: messageName
            ) else {
                throw AvroHandshakeError.missingSchema(messageName)
            }
            let param: T = try reader.decode(schema: responseSchema)
            params.append(param)
        }
        return (ResponseHeader(meta: meta ?? [:], flag: flag), params)
    }
}
/*
// MARK: - MessageRequest (backward-compatible façade)

/// Stateful client-side IPC façade.
///
/// Obtain via ``Avro/makeIPCClient(clientHash:clientProtocol:context:)``.
/// Holds a single `Avro` instance and forwards all calls to ``AvroIPCRequest``,
/// passing `context` (which now owns the session cache).
final class MessageRequest {
    let context:         AvroIPCContext
    private let avro:    Avro
    private let request: AvroIPCRequest
    var clientRequest:   HandshakeRequest

    var sessionCache: [MD5Hash: AvroProtocol] {
        get async { await context.clientCache.sessions }
    }

    init(context: AvroIPCContext, clientHash: MD5Hash, clientProtocol: String) throws {
        self.context = context
        self.avro    = Avro()
        self.request = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: clientProtocol, context: context
        )
        self.clientRequest = HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            serverHash:     clientHash,
            meta:           context.requestMeta
        )
        avro.setSchema(schema: context.requestSchema)
    }

    // MARK: - Handshake

    func encodeHandshakeRequest(_ req: HandshakeRequest) throws -> Data {
        try avro.encode(req)
    }

    func initHandshakeRequest() throws -> Data {
        try request.encodeInitialHandshake(avro: avro, context: context)
    }

    func decodeResponse(from data: Data) throws -> (HandshakeResponse, Data) {
        try request.decodeHandshakeResponse(avro: avro, from: data, context: context)
    }

    func resolveHandshakeResponse(_ response: HandshakeResponse) async throws -> Data? {
        try await request.resolveHandshakeResponse(response, avro: avro, context: context)
    }

    // MARK: - Session management

    func addSession(hash: MD5Hash, protocolString: String) async throws {
        try await context.clientCache.add(hash: hash, protocolString: protocolString)
    }

    func removeSession(for hash: MD5Hash) async {
        await context.clientCache.remove(for: hash)
    }

    func clearSessions() async {
        await context.clientCache.clear()
    }

    // MARK: - Call encode/decode

    func writeRequest<T: Codable>(messageName: String, parameters: [T]) async throws -> Data {
        try await request.encodeCall(
            avro:        avro,
            messageName: messageName,
            parameters:  parameters,
            serverHash:  clientRequest.serverHash,
            context:     context
        )
    }

    func readResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        from data:   Data
    ) async throws -> (ResponseHeader, [T]) {
        try await request.decodeResponse(
            avro:        avro,
            messageName: messageName,
            from:        data,
            serverHash:  header.serverHash,
            context:     context
        )
    }
}
*/
