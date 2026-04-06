//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//

import Foundation

// MARK: - AvroIPCResponse

/// Stateless server-side RPC handler.
/// All session state lives in ``ServerSessionCache``.
/// All schema/metadata configuration lives in ``AvroIPCContext``.
/// An ``Avro`` instance is created locally per call — never stored.
public struct AvroIPCResponse: Sendable {

    public let serverHash:     MD5Hash
    public let serverProtocol: String

    public init(serverHash: MD5Hash, serverProtocol: String) {
        self.serverHash     = serverHash
        self.serverProtocol = serverProtocol
    }

    // MARK: - Handshake

    /// Resolves a client handshake request and returns the decoded request
    /// alongside the serialised HandshakeResponse to send back.
    public func resolveHandshake(
        from data: Data,
        cache: ServerSessionCache,
        context: AvroIPCContext
    ) async throws -> (HandshakeRequest, Data) {
        let avro = Avro()
        avro.setSchema(schema: context.requestSchema)
        let request: HandshakeRequest = try avro.decodeFrom(
            from: data, schema: context.requestSchema
        )
        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        let response: HandshakeResponse
        if await cache.contains(hash: request.clientHash) {
            let matchType: HandshakeMatch = request.serverHash == serverHash
                ? .BOTH : .CLIENT
            response = HandshakeResponse(
                match:          matchType,
                serverProtocol: matchType == .CLIENT ? serverProtocol : nil,
                serverHash:     matchType == .CLIENT ? serverHash : nil
            )
        } else if let clientProtocol = request.clientProtocol,
                  request.serverHash == serverHash {
            try await cache.add(hash: request.clientHash, protocolString: clientProtocol)
            response = HandshakeResponse(
                match: .BOTH, serverProtocol: nil, serverHash: nil
            )
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
        header: HandshakeRequest,
        from data: Data,
        cache: ServerSessionCache,
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
            meta = m
            nameOffset = metaEnd + mEnd
        }

        let (messageName, nameEnd): (String?, Int) = try avro.decodeFromContinue(
            from: data.advanced(by: nameOffset), schema: AvroSchema(type: "string")
        )
        guard let name = messageName else {
            return (RequestHeader(meta: meta, name: ""), [])
        }

        var params: [T] = []
        if let schemas = await cache.requestSchemas(
            hash: header.clientHash, messageName: name
        ) {
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

    /// Encodes a successful call response: metadata + false flag + serialised value.
    public func encodeResponse<T: Codable>(
        header: HandshakeRequest,
        messageName: String,
        parameter: T,
        cache: ServerSessionCache,
        context: AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()
        data.append(try encodeMeta(header.meta, avro: avro, context: context))

        guard let responseSchema = await cache.responseSchema(
            hash: header.clientHash, messageName: messageName
        ) else { return data }

        let flag = try avro.encodeFrom(false, schema: AvroSchema(type: "boolean"))
        let body = try avro.encodeFrom(parameter, schema: responseSchema)
        data.append(flag)
        data.append(body)
        return data
    }

    /// Encodes an error call response: metadata + true flag + union-encoded errors.
    public func encodeErrorResponse<T: Codable>(
        header: HandshakeRequest,
        messageName: String,
        errors: [String: T],
        cache: ServerSessionCache,
        context: AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()
        data.append(try encodeMeta(header.meta, avro: avro, context: context))

        guard let errorSchemas = await cache.errorSchemas(
            hash: header.clientHash, messageName: messageName
        ) else { return data }

        let flag = try avro.encodeFrom(true, schema: AvroSchema(type: "boolean"))
        data.append(flag)

        for (key, value) in errors {
            if let schema = errorSchemas[key] {
                data.append(contentsOf: [UInt8(2)])  // union index 1 (zig-zag encoded)
                let body = try avro.encodeFrom(value, schema: schema)
                data.append(body)
            } else {
                data.append(contentsOf: [UInt8(0)])  // union index 0 (string fallback)
                let body = try avro.encodeFrom(value, schema: AvroSchema(type: "string"))
                data.append(body)
            }
        }
        return data
    }

    // MARK: - Private helpers

    private func encodeMeta(
        _ meta: [String: [UInt8]]?,
        avro: Avro,
        context: AvroIPCContext
    ) throws -> Data {
        if let meta {
            return try avro.encodeFrom(meta, schema: context.metaSchema)
        }
        return Data([0])  // null map — single zero byte per Avro spec
    }
}
