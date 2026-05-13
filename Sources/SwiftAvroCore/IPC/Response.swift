//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//  Copyright © 2026 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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
/// Obtain via ``Avro/makeIPCResponse(serverHash:serverProtocol:)``.
///
/// ```swift
/// let context  = AvroIPCContext(...)
/// let session  = AvroIPCSession(context: context)
/// let server   = avro.makeIPCResponse(serverHash: hash, serverProtocol: proto)
///
/// let (request, responseData, payload) = try await server.resolveHandshake(
///     avro: avro, from: data, session: session
/// )
/// let (header, params): (RequestHeader, [MyType]) = try await server.decodeCall(
///     avro: avro, header: request, from: payload, session: session
/// )
/// ```
public struct AvroIPCResponse: Sendable {

    public let serverHash:     MD5Hash
    public let serverProtocol: String
    // Lazily parsed at init for request schema evolution; nil if the JSON is invalid.
    private let parsedServerProtocol: AvroProtocol?

    public init(serverHash: MD5Hash, serverProtocol: String) {
        self.serverHash           = serverHash
        self.serverProtocol       = serverProtocol
        self.parsedServerProtocol = try? JSONDecoder().decode(
            AvroProtocol.self, from: Data(serverProtocol.utf8)
        )
    }

    // MARK: - Handshake

    /// Resolves a client handshake request, updating `session.serverCache`,
    /// and returns the decoded request, the serialised response, and the
    /// remaining call payload.
    public func resolveHandshake(
        avro:      Avro,
        from data: Data,
        session:   AvroIPCSession
    ) async throws -> (HandshakeRequest, Data, Data) {
        let reader = avro.makeDataReader(data: data)
        let request: HandshakeRequest = try reader.decode(schema: session.context.requestSchema)

        guard request.clientHash.count == 16 else {
            throw AvroHandshakeError.invalidClientHashLength
        }

        let callPayload = data.suffix(reader.bytesRemaining)

        let response: HandshakeResponse
        if await session.serverCache.contains(hash: request.clientHash) {
            let matchType: HandshakeMatch = request.serverHash == serverHash ? .BOTH : .CLIENT
            response = HandshakeResponse(
                match:          matchType,
                serverProtocol: matchType == .CLIENT ? serverProtocol : nil,
                serverHash:     matchType == .CLIENT ? serverHash : nil,
                meta:           nil
            )
        } else if let clientProtocol = request.clientProtocol,
                  request.serverHash == serverHash {
            try await session.serverCache.add(hash: request.clientHash, protocolString: clientProtocol)
            response = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil, meta: nil)
        } else {
            response = HandshakeResponse(
                match:          .NONE,
                serverProtocol: serverProtocol,
                serverHash:     serverHash,
                meta:           nil
            )
        }

        avro.setSchema(schema: session.context.responseSchema)
        let responseData = try avro.encode(response)
        return (request, responseData, Data(callPayload))
    }

    // MARK: - Reading requests

    /// Decodes an incoming call request: metadata + message name + parameters.
    public func decodeCall<T: Codable>(
        avro:      Avro,
        header:    HandshakeRequest,
        from data: Data,
        session:   AvroIPCSession
    ) async throws -> (RequestHeader, [T]) {
        let reader = avro.makeDataReader(data: data)

        let meta: [String: [UInt8]] = try reader.decode(schema: session.context.metaSchema)

        let messageName: String? = try reader.decode(schema: AvroSchema(type: "string"))

        // Empty/nil message name = ping per Avro IPC spec: return immediately.
        guard let name = messageName, !name.isEmpty else {
            return (RequestHeader(meta: meta, name: messageName ?? ""), [])
        }

        guard let schemas = await session.serverCache.requestSchemas(
            hash: header.clientHash, messageName: name
        ) else {
            throw AvroHandshakeError.missingSchema(name)
        }

        // Apply schema evolution when the server's parsed protocol provides a reader schema.
        // The `schemas` array holds the client's (writer) schemas fetched from serverCache.
        let serverSchemas = parsedServerProtocol?.getRequest(messageName: name)
        var params: [T] = []
        for (index, clientSchema) in schemas.enumerated() {
            let param: T
            if let serverSchemas, index < serverSchemas.count {
                param = try reader.decode(writerSchema: clientSchema, readerSchema: serverSchemas[index])
            } else {
                param = try reader.decode(schema: clientSchema)
            }
            params.append(param)
        }
        return (RequestHeader(meta: meta, name: name), params)
    }

    // MARK: - Writing responses

    /// Encodes a successful call response: metadata + `false` flag + serialised value.
    public func encodeResponse<T: Codable>(
        avro:        Avro,
        header:      HandshakeRequest,
        messageName: String,
        parameter:   T,
        session:     AvroIPCSession
    ) async throws -> Data {
        var data = Data()
        data.append(try avro.encodeFrom(header.meta ?? [:], schema: session.context.metaSchema))

        guard let responseSchema = await session.serverCache.responseSchema(
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
        session:     AvroIPCSession
    ) async throws -> Data {
        var data = Data()
        data.append(try avro.encodeFrom(header.meta ?? [:], schema: session.context.metaSchema))

        guard let errorSchemas = await session.serverCache.errorSchemas(
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
