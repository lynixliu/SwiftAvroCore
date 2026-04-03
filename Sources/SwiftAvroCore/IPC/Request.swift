//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
//

import Foundation
// MARK: - MessageRequest (Client-side RPC handler)

/// Manages the client side of the Avro IPC handshake and message encoding/decoding.
final class MessageRequest {
    private let avro: Avro
    let context: Context
    var sessionCache: [MD5Hash: AvroProtocol]
    var clientRequest: HandshakeRequest

    init(context: Context, clientHash: MD5Hash, clientProtocol: String) throws {
        self.avro = Avro()
        self.context = context
        self.sessionCache = [:]
        self.clientRequest = HandshakeRequest(
            clientHash: clientHash,
            clientProtocol: clientProtocol,
            serverHash: clientHash,
            meta: context.requestMeta
        )
        avro.setSchema(schema: context.requestSchema)
    }

    func encodeHandshakeRequest(_ request: HandshakeRequest) throws -> Data {
        try avro.encode(request)
    }

    /// Sends the initial handshake — clientProtocol is null on first attempt per the spec.
    func initHandshakeRequest() throws -> Data {
        try encodeHandshakeRequest(
            HandshakeRequest(
                clientHash: clientRequest.clientHash,
                clientProtocol: nil,
                serverHash: clientRequest.clientHash
            )
        )
    }

    func decodeResponse(from responseData: Data) throws -> (HandshakeResponse, Data) {
        let (response, next): (HandshakeResponse, Int) = try avro.decodeFromContinue(
            from: responseData, schema: context.responseSchema
        )
        return (response, responseData.subdata(in: next ..< responseData.count))
    }

    /// Returns a follow-up HandshakeRequest if required by the response match, or nil if done.
    func resolveHandshakeResponse(_ response: HandshakeResponse) throws -> Data? {
        switch response.match {
        case .NONE:
            guard let serverHash = response.serverHash else {
                throw AvroHandshakeError.noServerHash
            }
            return try encodeHandshakeRequest(
                HandshakeRequest(
                    clientHash: clientRequest.clientHash,
                    clientProtocol: clientRequest.clientProtocol,
                    serverHash: serverHash
                )
            )
        case .CLIENT:
            guard let serverHash = response.serverHash,
                  let serverProtocol = response.serverProtocol else {
                throw AvroHandshakeError.noServerHash
            }
            try addSession(hash: serverHash, protocolString: serverProtocol)
            return nil
        case .BOTH:
            return nil
        }
    }

    func decodeResponseData<T: Codable>(
        header: HandshakeResponse,
        messageName: String,
        from responseData: Data
    ) throws -> T {
        guard let serverHash = header.serverHash else {
            throw AvroHandshakeError.noServerHash
        }
        if serverHash == clientRequest.clientHash {
            return try avro.decodeFrom(from: responseData, schema: context.responseSchema)
        }
        guard let proto = sessionCache[serverHash],
              let responseSchema = proto.getResponse(messageName: messageName) else {
            throw AvroHandshakeError.sessionNotFound
        }
        return try avro.decodeFrom(from: responseData, schema: responseSchema)
    }

    func addSession(hash: MD5Hash, protocolString: String) throws {
        guard let data = protocolString.data(using: .utf8) else {
            throw AvroCodingError.decodingFailed("Invalid UTF-8 in protocol string")
        }
        sessionCache[hash] = try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    func removeSession(for hash: MD5Hash) {
        sessionCache.removeValue(forKey: hash)
    }

    func clearSessions() {
        sessionCache.removeAll()
    }

    /// Encodes a call request: metadata map + message name + serialized parameters.
    func writeRequest<T: Codable>(messageName: String, parameters: [T]) throws -> Data {
        var data = Data()

        guard let metaData = try? avro.encodeFrom(context.requestMeta, schema: context.metaSchema) else {
            throw AvroCodingError.encodingFailed("request metadata")
        }
        data.append(metaData)

        guard let nameData = try? avro.encodeFrom(messageName, schema: AvroSchema(type: "string")) else {
            throw AvroCodingError.encodingFailed("message name")
        }
        data.append(nameData)

        if let proto = sessionCache[clientRequest.serverHash],
           let schemas = proto.getRequest(messageName: messageName) {
            for (schema, parameter) in zip(schemas, parameters) {
                guard let paramData = try? avro.encodeFrom(parameter, schema: schema) else {
                    throw AvroCodingError.encodingFailed("parameter for \(messageName)")
                }
                data.append(paramData)
            }
        }
        return data
    }

    /// Reads a two-way response: response metadata + error flag + response/error payload.
    func readResponse<T: Codable>(
        header: HandshakeRequest,
        messageName: String,
        from data: Data
    ) throws -> (ResponseHeader, [T]) {
        // Decode optional metadata map: encoded as an int flag (0 = null) or the map itself.
        let (hasMeta, metaEnd): (Int, Int) = try avro.decodeFromContinue(
            from: data, schema: AvroSchema(type: "int")
        )
        var meta: [String: [UInt8]]?
        var flagOffset = metaEnd
        if hasMeta != 0 {
            let (m, mEnd): ([String: [UInt8]]?, Int) = try avro.decodeFromContinue(
                from: data.advanced(by: metaEnd), schema: context.metaSchema
            )
            meta = m
            flagOffset = metaEnd + mEnd
        }

        let (flag, payloadOffset): (Bool, Int) = try avro.decodeFromContinue(
            from: data.advanced(by: flagOffset), schema: AvroSchema(type: "boolean")
        )
        let absolutePayload = flagOffset + payloadOffset
        var params: [T] = []

        if flag {
            // Error response: decode each error union entry.
            if let proto = sessionCache[header.serverHash],
               let errorSchemas = proto.getErrors(messageName: messageName) {
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
            // Normal response.
            if let proto = sessionCache[header.serverHash],
               let responseSchema = proto.getResponse(messageName: messageName),
               let param = try? avro.decodeFrom(
                   from: data.advanced(by: absolutePayload), schema: responseSchema
               ) as T {
                params.append(param)
            }
        }

        return (ResponseHeader(meta: meta, flag: flag), params)
    }
}
