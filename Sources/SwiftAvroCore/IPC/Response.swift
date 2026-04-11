//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//  Refactored: internal `[MD5Hash: AvroProtocol]` dictionary replaced by
//  `ServerSessionCache` actor; `MessageResponse` preserved for backward
//  compatibility and delegates to the new `AvroIPCResponse` value type.
//

import Foundation

// MARK: - AvroIPCResponse

/// Value-type, `Sendable` server-side IPC handler.
///
/// Stateless with respect to sessions — all session state lives in the
/// ``ServerSessionCache`` actor passed to each method.
///
/// Replaces the session-dictionary logic that was inlined into `MessageResponse`.
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
    ) async throws -> (HandshakeRequest, Data) {
        let avro = Avro()
        let request: HandshakeRequest = try avro.decodeFrom(
            from: data, schema: context.requestSchema
        )
        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        let response: HandshakeResponse
        if await cache.contains(hash: request.clientHash) {
            let matchType: HandshakeMatch = request.serverHash == serverHash ? .BOTH : .CLIENT
            response = HandshakeResponse(
                match:          matchType,
                serverProtocol: matchType == .CLIENT ? serverProtocol : nil,
                serverHash:     matchType == .CLIENT ? serverHash : nil
            )
        } else if let clientProtocol = request.clientProtocol,
                  request.serverHash == serverHash {
            try await cache.add(hash: request.clientHash, protocolString: clientProtocol)
            response = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil)
        } else {
            response = HandshakeResponse(
                match:          .NONE,
                serverProtocol: serverProtocol,
                serverHash:     serverHash
            )
        }

        avro.setSchema(schema: context.responseSchema)
        let responseData = try avro.encode(response)
        return (request, responseData)
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
        guard let name = messageName else {
            return (RequestHeader(meta: meta, name: ""), [])
        }

        var params: [T] = []
        if let schemas = await cache.requestSchemas(hash: header.clientHash, messageName: name) {
            var index = nameOffset + nameEnd
            for schema in schemas {
                let (param, paramEnd): (T, Int) = try avro.decodeFromContinue(
                    from: data.advanced(by: index), schema: schema
                )
                params.append(param)
                index += paramEnd
            }
        }
        return (RequestHeader(meta: meta, name: name), params)
    }

    // MARK: - Writing responses

    /// Encodes a successful call response: metadata + `false` flag + serialised value.
    public func encodeResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T,
        cache:       ServerSessionCache,
        context:     AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()
        data.append(try encodeMeta(header.meta, avro: avro, context: context))
        guard let responseSchema = await cache.responseSchema(
            hash: header.clientHash, messageName: messageName
        ) else { return data }
        data.append(try avro.encodeFrom(false,     schema: AvroSchema(type: "boolean")))
        data.append(try avro.encodeFrom(parameter, schema: responseSchema))
        return data
    }

    /// Encodes an error call response: metadata + `true` flag + union-encoded errors.
    public func encodeErrorResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        errors:      [String: T],
        cache:       ServerSessionCache,
        context:     AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()
        data.append(try encodeMeta(header.meta, avro: avro, context: context))
        guard let errorSchemas = await cache.errorSchemas(
            hash: header.clientHash, messageName: messageName
        ) else { return data }

        data.append(try avro.encodeFrom(true, schema: AvroSchema(type: "boolean")))
        for (key, value) in errors {
            if let schema = errorSchemas[key] {
                data.append(contentsOf: [UInt8(2)])   // union index 1 (Avro zig-zag)
                data.append(try avro.encodeFrom(value, schema: schema))
            } else {
                data.append(contentsOf: [UInt8(0)])   // union index 0 (string fallback)
                data.append(try avro.encodeFrom(value, schema: AvroSchema(type: "string")))
            }
        }
        return data
    }

    // MARK: - Private helpers

    private func encodeMeta(
        _ meta: [String: [UInt8]]?,
        avro:   Avro,
        context: AvroIPCContext
    ) throws -> Data {
        if let meta {
            return try avro.encodeFrom(meta, schema: context.metaSchema)
        }
        return Data([0])  // null map — single zero byte per Avro spec
    }
}

// MARK: - MessageResponse (backward-compatible façade)

/// Manages the server side of the Avro IPC handshake and message encoding/decoding.
///
/// This class preserves the original API used by existing tests and callers.
/// Session state is now backed by ``ServerSessionCache`` but exposed through the
/// same `sessionCache` dictionary property for backward compatibility.
final class MessageResponse {
    private let avro:     Avro
    let context:          AvroIPCContext
    private let response: AvroIPCResponse
    private let cache:    ServerSessionCache
    var serverResponse:   HandshakeResponse

    /// Expose the underlying cache as the dictionary expected by tests.
    var sessionCache: [MD5Hash: AvroProtocol] {
        get async { await cache.sessions }
    }

    init(context: AvroIPCContext, serverHash: MD5Hash, serverProtocol: String) throws {
        self.avro     = Avro()
        self.context  = context
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

    func encodeHandshakeResponse(_ resp: HandshakeResponse) throws -> Data {
        try avro.encode(resp)
    }

    func addSupportedProtocol(protocolString: String, hash: MD5Hash) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        Task { await cache.setDirectly(hash: hash, proto: proto) }
    }

    func resolveHandshakeRequest(from requestData: Data) async throws -> (HandshakeRequest, Data) {
        let request: HandshakeRequest = try avro.decodeFrom(
            from: requestData, schema: context.requestSchema
        )
        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        let known = await cache.syncContains(hash: request.clientHash)
        if known {
            let matchType: HandshakeMatch = request.serverHash == serverResponse.serverHash
                ? .BOTH : .CLIENT
            let resp = HandshakeResponse(
                match:          matchType,
                serverProtocol: matchType == .CLIENT ? serverResponse.serverProtocol : nil,
                serverHash:     matchType == .CLIENT ? serverResponse.serverHash : nil
            )
            return (request, try encodeHandshakeResponse(resp))
        }

        if let clientProtocol = request.clientProtocol,
           request.serverHash == serverResponse.serverHash {
            try addSupportedProtocol(protocolString: clientProtocol, hash: request.clientHash)
            let resp = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil)
            return (request, try encodeHandshakeResponse(resp))
        }

        let resp = HandshakeResponse(
            match:          .NONE,
            serverProtocol: serverResponse.serverProtocol,
            serverHash:     serverResponse.serverHash
        )
        return (request, try encodeHandshakeResponse(resp))
    }

    func removeSession(for hash: MD5Hash) {
        Task { await cache.remove(for: hash) }
    }

    func clearSessions() {
        Task { await cache.clear() }
    }

    // MARK: - Call encode/decode

    func writeResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T
    ) async throws -> Data {
        var data = Data()
        data.append(try encodeMeta(header.meta))
        let proto = await cache.syncGet(hash: header.serverHash)
        guard let responseSchema = proto?.getResponse(messageName: messageName) else { return data }
        guard let flag = try? avro.encodeFrom(false,     schema: AvroSchema(type: "boolean")),
              let body = try? avro.encodeFrom(parameter, schema: responseSchema) else {
            throw AvroCodingError.encodingFailed("response for \(messageName)")
        }
        data.append(flag)
        data.append(body)
        return data
    }

    func writeErrorResponse<T: Codable>(
        header:      HandshakeRequest,
        messageName: String,
        errors:      [String: T]
    ) async throws -> Data {
        var data = Data()
        data.append(try encodeMeta(header.meta))
        let proto = await cache.syncGet(hash: header.serverHash)
        guard let errorSchemas = proto?.getErrors(messageName: messageName) else { return data }
        guard let flag = try? avro.encodeFrom(true, schema: AvroSchema(type: "boolean")) else {
            throw AvroCodingError.encodingFailed("error flag")
        }
        data.append(flag)
        for (key, value) in errors {
            if let schema = errorSchemas[key] {
                data.append(contentsOf: [UInt8(2)])
                guard let body = try? avro.encodeFrom(value, schema: schema) else {
                    throw AvroCodingError.encodingFailed("error body for \(key)")
                }
                data.append(body)
            } else {
                data.append(contentsOf: [UInt8(0)])
                guard let body = try? avro.encodeFrom(value, schema: AvroSchema(type: "string")) else {
                    throw AvroCodingError.encodingFailed("error string for \(key)")
                }
                data.append(body)
            }
        }
        return data
    }

    func readRequest<T: Codable>(
        header:    HandshakeRequest,
        from data: Data
    ) async throws -> (RequestHeader, [T]) {
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
        guard let name = messageName else {
            return (RequestHeader(meta: meta, name: ""), [])
        }

        var params: [T] = []
        let proto = await cache.syncGet(hash: header.serverHash)
        if let requestSchemas = proto?.getRequest(messageName: name) {
            var index = nameOffset + nameEnd
            for schema in requestSchemas {
                let (param, paramEnd): (T, Int) = try avro.decodeFromContinue(
                    from: data.advanced(by: index), schema: schema
                )
                params.append(param)
                index += paramEnd
            }
        }
        return (RequestHeader(meta: meta, name: name), params)
    }

    // MARK: Private

    private func encodeMeta(_ meta: [String: [UInt8]]?) throws -> Data {
        if let meta {
            guard let encoded = try? avro.encodeFrom(meta, schema: context.metaSchema) else {
                throw AvroCodingError.encodingFailed("response metadata")
            }
            return encoded
        }
        return Data([0])
    }
}
