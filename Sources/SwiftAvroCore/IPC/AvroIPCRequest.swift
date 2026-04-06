//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
//

import Foundation

// MARK: - AvroIPCRequest

/// Stateless client-side RPC handler.
/// All handshake state lives in ``ClientSessionCache``.
/// All schema/metadata configuration lives in ``AvroIPCContext``.
/// An ``Avro`` instance is created locally per call — never stored.
public struct AvroIPCRequest: Sendable {

    public let clientHash:     MD5Hash
    public let clientProtocol: String

    public init(clientHash: MD5Hash, clientProtocol: String) {
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
    }

    // MARK: - Handshake encoding

    /// Encodes the initial handshake request with a null clientProtocol
    /// (first attempt per the Avro IPC spec).
    public func encodeInitialHandshake(context: AvroIPCContext) throws -> Data {
        let avro = Avro()
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: nil,
            serverHash:     clientHash,
            meta:           context.requestMeta
        ))
    }

    /// Encodes a full handshake request including the client protocol string.
    public func encodeHandshake(
        serverHash: MD5Hash,
        context: AvroIPCContext
    ) throws -> Data {
        let avro = Avro()
        avro.setSchema(schema: context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            serverHash:     serverHash,
            meta:           context.requestMeta
        ))
    }

    // MARK: - Handshake decoding

    /// Decodes the server's handshake response and returns the remaining payload bytes.
    public func decodeHandshakeResponse(
        from data: Data,
        context: AvroIPCContext
    ) throws -> (HandshakeResponse, Data) {
        let avro = Avro()
        let (response, next): (HandshakeResponse, Int) = try avro.decodeFromContinue(
            from: data, schema: context.responseSchema
        )
        return (response, data.subdata(in: next ..< data.count))
    }

    /// Resolves a handshake response against the session cache.
    /// Returns follow-up handshake data if the server responded with NONE,
    /// or nil if handshake is complete.
    public func resolveHandshakeResponse(
        _ response: HandshakeResponse,
        cache: ClientSessionCache,
        context: AvroIPCContext
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
        parameters: [T],
        serverHash: MD5Hash,
        cache: ClientSessionCache,
        context: AvroIPCContext
    ) async throws -> Data {
        let avro = Avro()
        var data = Data()

        // Encode metadata — empty map is a single zero byte per Avro spec,
        // matching the server-side encodeMeta behaviour
        if context.requestMeta.isEmpty {
            data.append(Data([0]))
        } else {
            let metaData = try avro.encodeFrom(context.requestMeta, schema: context.metaSchema)
            data.append(metaData)
        }

        let nameData = try avro.encodeFrom(
            messageName, schema: AvroSchema(type: "string")
        )
        data.append(nameData)

        if let schemas = await cache.requestSchemas(
            hash: serverHash, messageName: messageName
        ) {
            for (schema, parameter) in zip(schemas, parameters) {
                let paramData = try avro.encodeFrom(parameter, schema: schema)
                data.append(paramData)
            }
        }
        return data
    }

    // MARK: - Response decoding

    /// Decodes a typed response value from the server payload.
    public func decodeResponse<T: Codable>(
        messageName: String,
        from data: Data,
        serverHash: MD5Hash,
        cache: ClientSessionCache,
        context: AvroIPCContext
    ) async throws -> T {
        let avro = Avro()

        // Decode optional metadata
        let (hasMeta, metaEnd): (Int, Int) = try avro.decodeFromContinue(
            from: data, schema: AvroSchema(type: "int")
        )
        var flagOffset = metaEnd
        if hasMeta != 0 {
            let (_, mEnd): ([String: [UInt8]]?, Int) = try avro.decodeFromContinue(
                from: data.advanced(by: metaEnd), schema: context.metaSchema
            )
            flagOffset = metaEnd + mEnd
        }

        let (flag, payloadOffset): (Bool, Int) = try avro.decodeFromContinue(
            from: data.advanced(by: flagOffset), schema: AvroSchema(type: "boolean")
        )
        let absolutePayload = flagOffset + payloadOffset

        if flag {
            // Error response
            throw AvroHandshakeError.sessionNotFound
        }

        guard let responseSchema = await cache.responseSchema(
            hash: serverHash, messageName: messageName
        ) else {
            throw AvroHandshakeError.sessionNotFound
        }
        return try avro.decodeFrom(
            from: data.advanced(by: absolutePayload),
            schema: responseSchema
        )
    }
}
