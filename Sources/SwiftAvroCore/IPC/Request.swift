//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
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
/// Obtain via ``Avro/makeIPCRequest(clientHash:clientProtocol:session:)``.
///
/// ```swift
/// let context   = AvroIPCContext(...)
/// let session   = AvroIPCSession(context: context)
/// let client    = try avro.makeIPCRequest(clientHash: hash, clientProtocol: proto, session: session)
///
/// let handshake = try client.encodeInitialHandshake(avro: avro, session: session)
/// // ... send handshake, receive response ...
/// let call      = try await client.encodeCall(
///     avro: avro, messageName: "add", parameters: [req],
///     serverHash: serverHash, session: session
/// )
/// ```
public struct AvroIPCRequest: Sendable {

    public let clientHash:     MD5Hash
    public let clientProtocol: String
    // Lazily parsed at init for response schema evolution; nil if the JSON is invalid.
    private let parsedClientProtocol: AvroProtocol?

    public init(
        clientHash:     MD5Hash,
        clientProtocol: String,
        session:        AvroIPCSession? = nil
    ) throws {
        if let known = session?.context.knownProtocols, !known.contains(clientProtocol) {
            throw AvroHandshakeError.unknownProtocol(clientProtocol)
        }
        self.clientHash             = clientHash
        self.clientProtocol         = clientProtocol
        self.parsedClientProtocol   = try? JSONDecoder().decode(
            AvroProtocol.self, from: Data(clientProtocol.utf8)
        )
    }

    // MARK: - Handshake encoding

    /// Encodes the initial handshake — `clientProtocol` is nil on the first
    /// attempt per the Avro IPC specification.
    public func encodeInitialHandshake(avro: Avro, session: AvroIPCSession) throws -> Data {
        avro.setSchema(schema: session.context.requestSchema)
        // serverHash is unknown on first contact; send zero-filled bytes as sentinel.
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: nil,
            serverHash:     [UInt8](repeating: 0, count: 16),
            meta:           session.context.requestMeta
        ))
    }

    /// Encodes a retry handshake that includes the full client protocol string.
    public func encodeHandshake(
        avro:       Avro,
        serverHash: MD5Hash,
        session:    AvroIPCSession
    ) throws -> Data {
        avro.setSchema(schema: session.context.requestSchema)
        return try avro.encode(HandshakeRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            serverHash:     serverHash,
            meta:           session.context.requestMeta
        ))
    }

    // MARK: - Handshake decoding

    /// Decodes the server's handshake response and splits off the remaining payload bytes.
    public func decodeHandshakeResponse(
        avro:      Avro,
        from data: Data,
        session:   AvroIPCSession
    ) throws -> (HandshakeResponse, Data) {
        let reader = avro.makeDataReader(data: data)
        let response: HandshakeResponse = try reader.decode(schema: session.context.responseSchema)
        return (response, Data(data.suffix(reader.bytesRemaining)))
    }

    /// Resolves a handshake response, updating `session.clientCache` as needed.
    ///
    /// - Returns: Follow-up handshake data if the server responded with `.NONE`,
    ///   or `nil` if the handshake is complete (`.BOTH` / `.CLIENT`).
    public func resolveHandshakeResponse(
        _ response: HandshakeResponse,
        avro:       Avro,
        session:    AvroIPCSession
    ) async throws -> Data? {
        switch response.match {
        case .NONE:
            guard let serverHash = response.serverHash else {
                throw AvroHandshakeError.noServerHash
            }
            if let serverProtocol = response.serverProtocol {
                try await session.clientCache.add(hash: serverHash, protocolString: serverProtocol)
            }
            return try encodeHandshake(avro: avro, serverHash: serverHash, session: session)

        case .CLIENT:
            guard let serverHash     = response.serverHash,
                  let serverProtocol = response.serverProtocol else {
                throw AvroHandshakeError.noServerHash
            }
            try await session.clientCache.add(hash: serverHash, protocolString: serverProtocol)
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
        session:     AvroIPCSession
    ) async throws -> Data {
        var data = Data()
        data.append(try avro.encodeFrom(session.context.requestMeta, schema: session.context.metaSchema))
        data.append(try avro.encodeFrom(messageName, schema: AvroSchema(type: "string")))

        // Empty message name = ping per Avro IPC spec: no parameters.
        guard !messageName.isEmpty else { return data }

        // Prefer the client's own schemas (Avro IPC spec §7.1: client encodes with its
        // own protocol). Fall back to the server's schemas when the client protocol does
        // not define this message.
        let schemas: [AvroSchema]
        if let clientSchemas = parsedClientProtocol?.getRequest(messageName: messageName) {
            schemas = clientSchemas
        } else if let serverSchemas = await session.clientCache.requestSchemas(
            hash: serverHash, messageName: messageName
        ) {
            schemas = serverSchemas
        } else {
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
        session:     AvroIPCSession
    ) async throws -> (ResponseHeader, [T]) {
        let reader = avro.makeDataReader(data: data)

        let meta: [String: [UInt8]] = try reader.decode(schema: session.context.metaSchema)

        let flag: Bool = try reader.decode(schema: AvroSchema(type: "boolean"))
        var params: [T] = []

        if flag {
            if let errorSchemas = await session.clientCache.errorSchemas(
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
            guard let writerSchema = await session.clientCache.responseSchema(
                hash: serverHash, messageName: messageName
            ) else {
                throw AvroHandshakeError.missingSchema(messageName)
            }
            // Apply schema evolution when the client's parsed protocol provides a
            // reader schema. RecordSchema.== is a compatibility check, not structural
            // equality, so we cannot use != to skip identical schemas — the evolution
            // path is safe for identical schemas (no-op) and required for divergent ones.
            let param: T
            if let readerSchema = parsedClientProtocol?.getResponse(messageName: messageName) {
                param = try reader.decode(writerSchema: writerSchema, readerSchema: readerSchema)
            } else {
                param = try reader.decode(schema: writerSchema)
            }
            params.append(param)
        }
        return (ResponseHeader(meta: meta, flag: flag), params)
    }
}
