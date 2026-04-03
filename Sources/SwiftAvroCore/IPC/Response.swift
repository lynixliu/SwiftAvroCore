//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//

import Foundation

// MARK: - MessageResponse (Server-side RPC handler)

/// Manages the server side of the Avro IPC handshake and message encoding/decoding.
final class MessageResponse {
    private let avro: Avro
    let context: Context
    var sessionCache: [MD5Hash: AvroProtocol]
    var serverResponse: HandshakeResponse

    init(context: Context, serverHash: MD5Hash, serverProtocol: String) throws {
        self.avro = Avro()
        self.context = context
        self.avro.setSchema(schema: context.responseSchema)
        self.sessionCache = [:]
        self.serverResponse = HandshakeResponse(
            match: .NONE,
            serverProtocol: serverProtocol,
            serverHash: serverHash,
            meta: context.responseMeta
        )
    }

    func encodeHandshakeResponse(_ response: HandshakeResponse) throws -> Data {
        try avro.encode(response)
    }

    func addSupportedProtocol(protocolString: String, hash: MD5Hash) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        sessionCache[hash] = try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    /// Resolves a client HandshakeRequest according to the Avro IPC spec state machine.
    /// Returns the decoded request and the serialised HandshakeResponse to send back.
    func resolveHandshakeRequest(from requestData: Data) throws -> (HandshakeRequest, Data) {
        let request: HandshakeRequest = try avro.decodeFrom(
            from: requestData, schema: context.requestSchema
        )
        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        if sessionCache[request.clientHash] != nil {
            // We know this client. Check if they have the right server hash.
            let matchType: HandshakeMatch = request.serverHash == serverResponse.serverHash
                ? .BOTH
                : .CLIENT
            let response = HandshakeResponse(
                match: matchType,
                serverProtocol: matchType == .CLIENT ? serverResponse.serverProtocol : nil,
                serverHash: matchType == .CLIENT ? serverResponse.serverHash : nil
            )
            return (request, try encodeHandshakeResponse(response))
        }

        if let clientProtocol = request.clientProtocol,
           request.serverHash == serverResponse.serverHash {
            // New client with correct server hash — register and confirm.
            try addSupportedProtocol(protocolString: clientProtocol, hash: request.clientHash)
            let response = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil)
            return (request, try encodeHandshakeResponse(response))
        }

        // Unknown client or wrong server hash — send back server protocol so client can retry.
        let response = HandshakeResponse(
            match: .NONE,
            serverProtocol: serverResponse.serverProtocol,
            serverHash: serverResponse.serverHash
        )
        return (request, try encodeHandshakeResponse(response))
    }

    func removeSession(for hash: MD5Hash) {
        sessionCache.removeValue(forKey: hash)
    }

    func clearSessions() {
        sessionCache.removeAll()
    }

    /// Encodes a successful call response: metadata + false flag + serialised response value.
    func writeResponse<T: Codable>(
        header: HandshakeRequest,
        messageName: String,
        parameter: T
    ) throws -> Data {
        var data = Data()
        data.append(try encodeMeta(header.meta))
        guard let proto = sessionCache[header.serverHash],
              let responseSchema = proto.getResponse(messageName: messageName) else {
            return data
        }
        guard let flag = try? avro.encodeFrom(false, schema: AvroSchema(type: "boolean")),
              let body = try? avro.encodeFrom(parameter, schema: responseSchema) else {
            throw AvroCodingError.encodingFailed("response for \(messageName)")
        }
        data.append(flag)
        data.append(body)
        return data
    }

    /// Encodes an error call response: metadata + true flag + union-encoded errors.
    func writeErrorResponse<T: Codable>(
        header: HandshakeRequest,
        messageName: String,
        errors: [String: T]
    ) throws -> Data {
        var data = Data()
        data.append(try encodeMeta(header.meta))
        guard let proto = sessionCache[header.serverHash],
              let errorSchemas = proto.getErrors(messageName: messageName) else {
            return data
        }
        guard let flag = try? avro.encodeFrom(true, schema: AvroSchema(type: "boolean")) else {
            throw AvroCodingError.encodingFailed("error flag")
        }
        data.append(flag)
        for (key, value) in errors {
            if let schema = errorSchemas[key] {
                data.append(contentsOf: [UInt8(2)])  // union index 1 (Avro zig-zag: 2)
                guard let body = try? avro.encodeFrom(value, schema: schema) else {
                    throw AvroCodingError.encodingFailed("error body for \(key)")
                }
                data.append(body)
            } else {
                data.append(contentsOf: [UInt8(0)])  // union index 0 (string fallback)
                guard let body = try? avro.encodeFrom(value, schema: AvroSchema(type: "string")) else {
                    throw AvroCodingError.encodingFailed("error string for \(key)")
                }
                data.append(body)
            }
        }
        return data
    }

    /// Reads an incoming call request: metadata + message name + parameters.
    func readRequest<T: Codable>(
        header: HandshakeRequest,
        from data: Data
    ) throws -> (RequestHeader, [T]) {
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
        if let proto = sessionCache[header.serverHash],
           let requestSchemas = proto.getRequest(messageName: name) {
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

    // MARK: Private helpers

    private func encodeMeta(_ meta: [String: [UInt8]]?) throws -> Data {
        if let meta {
            guard let encoded = try? avro.encodeFrom(meta, schema: context.metaSchema) else {
                throw AvroCodingError.encodingFailed("response metadata")
            }
            return encoded
        } else {
            // Avro encodes null map as a single zero byte (empty map length).
            return Data([0])
        }
    }
}
