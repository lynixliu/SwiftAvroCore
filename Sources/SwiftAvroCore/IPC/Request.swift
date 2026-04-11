//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
//  Refactored: internal `[MD5Hash: AvroProtocol]` dictionary replaced by
//  `ClientSessionCache` actor; `MessageRequest` preserved for backward
//  compatibility and delegates to the new `AvroIPCRequest` value type.
//

import Foundation

// MARK: - AvroIPCRequest

/// Value-type, `Sendable` client-side IPC handler.
///
/// Stateless with respect to sessions — all session state lives in the
/// ``ClientSessionCache`` actor passed to each method.
///
/// Replaces the session-dictionary logic that was inlined into `MessageRequest`.
public struct AvroIPCRequest: Sendable {

    public let clientHash:     MD5Hash
    public let clientProtocol: String

    public init(clientHash: MD5Hash, clientProtocol: String) {
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
    }

    // MARK: - Handshake encoding

    /// Encodes the initial handshake — `clientProtocol` is null on the first
    /// attempt per the Avro IPC specification.
    public func encodeInitialHandshake(context: AvroIPCContext) throws -> Data {
        let avro = Avro()
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: nil,
            serverHash:     clientHash,
            meta:           context.requestMeta.isEmpty ? nil : context.requestMeta
        ))
    }

    /// Encodes a retry handshake that includes the full client protocol string.
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
            meta:           context.requestMeta.isEmpty ? nil : context.requestMeta
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
    public func encodeCall<T: Codable>(
        messageName: String,
        parameters:  [T],
        serverHash:  MD5Hash,
        cache:       ClientSessionCache,
        context:     AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()

        data.append(try avro.encodeFrom(
            context.requestMeta.isEmpty ? [:] : context.requestMeta,
            schema: context.metaSchema
        ))
        data.append(try avro.encodeFrom(messageName, schema: AvroSchema(type: "string")))

        if let schemas = await cache.requestSchemas(hash: serverHash, messageName: messageName) {
            for (schema, parameter) in zip(schemas, parameters) {
                data.append(try avro.encodeFrom(parameter, schema: schema))
            }
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
                    let schema = unionIndex > 0 ? errorSchema : errorSchema
                    let (param, paramEnd): (T, Int) = try avro.decodeFromContinue(
                        from: data.advanced(by: index), schema: schema
                    )
                    params.append(param)
                    index += paramEnd
                }
            }
        } else {
            if let responseSchema = await cache.responseSchema(hash: serverHash, messageName: messageName),
               let param = try? avro.decodeFrom(
                   from: data.advanced(by: absolutePayload), schema: responseSchema
               ) as T {
                params.append(param)
            }
        }
        return (ResponseHeader(meta: meta, flag: flag), params)
    }
}

// MARK: - MessageRequest (backward-compatible façade)

/// Manages the client side of the Avro IPC handshake and message encoding/decoding.
///
/// This class preserves the original API used by existing tests and callers.
/// Session state is now backed by ``ClientSessionCache`` but exposed through the
/// same `sessionCache` dictionary property for backward compatibility.
final class MessageRequest {
    private let avro:    Avro
    let context:         AvroIPCContext
    private let request: AvroIPCRequest
    private let cache:   ClientSessionCache
    var clientRequest:   HandshakeRequest

    /// Expose the underlying cache as the dictionary expected by tests.
    var sessionCache: [MD5Hash: AvroProtocol] {
        get async { await cache.sessions }
    }

    init(context: AvroIPCContext, clientHash: MD5Hash, clientProtocol: String) throws {
        self.avro    = Avro()
        self.context = context
        self.cache   = ClientSessionCache()
        self.request = AvroIPCRequest(clientHash: clientHash, clientProtocol: clientProtocol)
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
        try encodeHandshakeRequest(
            HandshakeRequest(
                clientHash:     clientRequest.clientHash,
                clientProtocol: nil,
                serverHash:     clientRequest.clientHash
            )
        )
    }

    func decodeResponse(from data: Data) throws -> (HandshakeResponse, Data) {
        let (response, next): (HandshakeResponse, Int) = try avro.decodeFromContinue(
            from: data, schema: context.responseSchema
        )
        return (response, data.subdata(in: next ..< data.count))
    }

    func resolveHandshakeResponse(_ response: HandshakeResponse) throws -> Data? {
        switch response.match {
        case .NONE:
            guard let serverHash = response.serverHash else {
                throw AvroHandshakeError.noServerHash
            }
            return try encodeHandshakeRequest(
                HandshakeRequest(
                    clientHash:     clientRequest.clientHash,
                    clientProtocol: clientRequest.clientProtocol,
                    serverHash:     serverHash
                )
            )
        case .CLIENT:
            guard let serverHash     = response.serverHash,
                  let serverProtocol = response.serverProtocol else {
                throw AvroHandshakeError.noServerHash
            }
            try addSession(hash: serverHash, protocolString: serverProtocol)
            return nil
        case .BOTH:
            return nil
        }
    }

    // MARK: - Session management

    func addSession(hash: MD5Hash, protocolString: String) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        Task { await cache.setDirectly(hash: hash, proto: proto) }
    }

    func removeSession(for hash: MD5Hash) {
        Task { await cache.remove(for: hash) }
    }

    func clearSessions() {
        Task { await cache.clear() }
    }

    // MARK: - Call encode/decode

    func writeRequest<T: Codable>(messageName: String, parameters: [T]) async throws -> Data {
        var data = Data()
        guard let metaData = try? avro.encodeFrom(context.requestMeta, schema: context.metaSchema) else {
            throw AvroCodingError.encodingFailed("request metadata")
        }
        data.append(metaData)
        guard let nameData = try? avro.encodeFrom(messageName, schema: AvroSchema(type: "string")) else {
            throw AvroCodingError.encodingFailed("message name")
        }
        data.append(nameData)

        // Synchronous session lookup — session is always set before write in practice.
        let proto = await cache.syncGet(hash: clientRequest.serverHash)
        if let schemas = proto?.getRequest(messageName: messageName) {
            for (schema, parameter) in zip(schemas, parameters) {
                guard let paramData = try? avro.encodeFrom(parameter, schema: schema) else {
                    throw AvroCodingError.encodingFailed("parameter for \(messageName)")
                }
                data.append(paramData)
            }
        }
        return data
    }

    func readResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        from data:   Data
    ) async throws -> (ResponseHeader, [T]) {
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
            let proto = await cache.syncGet(hash: header.serverHash)
            if let errorSchemas = proto?.getErrors(messageName: messageName) {
                var index = absolutePayload
                for (_, errorSchema) in errorSchemas {
                    let (unionIndex, unionEnd): (Int, Int) = try avro.decodeFromContinue(
                        from: data.advanced(by: index), schema: AvroSchema(type: "long")
                    )
                    index += unionEnd
                    let schema = unionIndex > 0 ? errorSchema : errorSchema
                    let (param, paramEnd): (T, Int) = try avro.decodeFromContinue(
                        from: data.advanced(by: index), schema: schema
                    )
                    params.append(param)
                    index += paramEnd
                }
            }
        } else {
            let proto = await cache.syncGet(hash: header.serverHash)
            if let responseSchema = proto?.getResponse(messageName: messageName),
               let param = try? avro.decodeFrom(
                   from: data.advanced(by: absolutePayload), schema: responseSchema
               ) as T {
                params.append(param)
            }
        }
        return (ResponseHeader(meta: meta, flag: flag), params)
    }
}
