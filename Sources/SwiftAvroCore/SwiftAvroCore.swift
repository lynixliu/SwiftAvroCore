//
//  AvroClient/Avro.swift
//
//  Created by Yang Liu on 24/08/18.
//  Copyright © 2018 柳洋 and the project authors.
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

import Foundation

public class Avro {

    private var schema: AvroSchema?
    private var schemaEncodingOption: AvroSchemaEncodingOption = .CanonicalForm
    private var encodingOption: AvroEncodingOption = .AvroBinary
    private var stream = Data()

    private let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!

    public init() {}

    // MARK: - Schema management

    public func setSchema(schema: AvroSchema) {
        self.schema = schema
    }

    public func getSchema() -> AvroSchema? {
        return schema
    }

    public func setSchemaFormat(option: AvroSchemaEncodingOption) {
        schemaEncodingOption = option
    }

    public func setAvroFormat(option: AvroEncodingOption) {
        encodingOption = option
    }

    // MARK: - Schema decoding

    /// Decodes and stores a schema from a JSON string. Returns the decoded schema, or `nil` on failure.
    @discardableResult
    public func decodeSchema(schema: String) -> AvroSchema? {
        guard let data = schema.data(using: .utf8) else { return nil }
        return decodeSchema(schema: data)
    }

    /// Decodes and stores a schema from JSON data. Returns the decoded schema, or `nil` on failure.
    @discardableResult
    public func decodeSchema(schema: Data) -> AvroSchema? {
        do {
            self.schema = try JSONDecoder().decode(AvroSchema.self, from: schema)
            return self.schema
        } catch {
            return nil
        }
    }

    /// Decodes a schema from a JSON string without storing it.
    public func newSchema(schema: String) -> AvroSchema? {
        guard let data = schema.data(using: .utf8) else { return nil }
        return newSchema(schema: data)
    }

    /// Decodes a schema from JSON data without storing it.
    public func newSchema(schema: Data) -> AvroSchema? {
        return try? JSONDecoder().decode(AvroSchema.self, from: schema)
    }

    // MARK: - Schema encoding

    /// Encodes the currently stored schema.
    public func encodeSchema() throws -> Data {
        guard let schema = self.schema else { return Data() }
        return try encodeSchema(schema: schema)
    }

    /// Encodes the given schema according to the current `schemaEncodingOption`.
    public func encodeSchema(schema: AvroSchema) throws -> Data {
        let encoder = JSONEncoder()
        switch schemaEncodingOption {
        case .PrettyPrintedForm:
            encoder.outputFormatting = .prettyPrinted
            encoder.userInfo[infoKey] = schemaEncodingOption
        case .FullForm:
            encoder.userInfo[infoKey] = schemaEncodingOption
        case .CanonicalForm:
            break
        }
        guard let data = try schema.encode(jsonEncoder: encoder) else {
            throw AvroSchemaEncodingError.invalidSchemaType
        }
        return data
    }

    // MARK: - Avro binary encode / decode

    /// Encodes `value` using the stored schema (reflecting one if not set).
    public func encode<T: Encodable>(_ value: T) throws -> Data {
        if schema == nil { schema = AvroSchema.reflecting(value) }
        let encoder = AvroEncoder()
        encoder.setUserInfo(userInfo: [infoKey: encodingOption])
        return try encoder.encode(value, schema: schema!)
    }

    /// Decodes a value of type `T` from binary data using the stored schema.
    public func decode<T: Decodable>(from data: Data) throws -> T {
        guard let schema = self.schema else {
            throw BinaryEncodingError.noSchemaSpecified
        }
        return try AvroDecoder(schema: schema).decode(T.self, from: data)
    }

    /// Decodes an untyped value from binary data using the stored schema.
    public func decode(from data: Data) throws -> Any? {
        guard let schema = self.schema else {
            throw BinaryEncodingError.noSchemaSpecified
        }
        return try AvroDecoder(schema: schema).decode(from: data)
    }

    // MARK: - Stateless encode / decode (explicit schema)

    /// Encodes `value` using the provided schema.
    public func encodeFrom<T: Codable>(_ value: T, schema: AvroSchema) throws -> Data {
        let encoder = AvroEncoder()
        encoder.setUserInfo(userInfo: [infoKey: encodingOption])
        return try encoder.encode(value, schema: schema)
    }

    /// Decodes a value of type `T` from binary data using the provided schema.
    public func decodeFrom<T: Codable>(from data: Data, schema: AvroSchema) throws -> T {
        return try AvroDecoder(schema: schema).decode(T.self, from: data)
    }

    /// Decodes an untyped value from binary data using the provided schema.
    public func decodeFrom(from data: Data, schema: AvroSchema) throws -> Any? {
        return try AvroDecoder(schema: schema).decode(from: data)
    }

    // MARK: - Streaming decode

    /// Creates a reader that decodes consecutive Avro records from `data`.
    public func makeDataReader(data: Data) -> AvroDataReader {
        AvroDataReader(data: data)
    }

    // MARK: - Object container

    public func makeFileObjectContainer(schema: String? = nil, codec: CodecProtocol) throws -> ObjectContainer {
        try ObjectContainer(schema: schema, codec: codec)
    }
}

// MARK: - IPC

extension Avro {

    // MARK: Value-type factories

    /// Creates a stateless client-side IPC value type.
    ///
    /// Session state is managed by `context.clientCache`. Use the returned
    /// ``AvroIPCRequest`` directly when you need fine-grained control; for
    /// the higher-level stateful façade use ``makeIPCClient``.
    ///
    /// ```swift
    /// let request = try avro.makeIPCRequest(
    ///     clientHash:     myHash,
    ///     clientProtocol: "com.example.MyProtocol",
    ///     context:        context
    /// )
    /// let handshake = try request.encodeInitialHandshake(avro: avro, context: context)
    /// ```
    public func makeIPCRequest(
        clientHash:     MD5Hash,
        clientProtocol: String,
        context:        AvroIPCContext? = nil
    ) throws -> AvroIPCRequest {
        try AvroIPCRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            context:        context
        )
    }

    /// Creates a stateless server-side IPC value type.
    ///
    /// Session state is managed by `context.serverCache`. Use the returned
    /// ``AvroIPCResponse`` directly when you need fine-grained control; for
    /// the higher-level stateful façade use ``makeIPCServer``.
    ///
    /// ```swift
    /// let response = avro.makeIPCResponse(
    ///     serverHash:     myHash,
    ///     serverProtocol: "com.example.MyProtocol"
    /// )
    /// let (request, responseData, payload) = try await response.resolveHandshake(
    ///     avro: avro, from: data, context: context
    /// )
    /// ```
    public func makeIPCResponse(
        serverHash:     MD5Hash,
        serverProtocol: String
    ) -> AvroIPCResponse {
        AvroIPCResponse(serverHash: serverHash, serverProtocol: serverProtocol)
    }
/*
    // MARK: Façade factories

    /// Creates a client-side IPC façade.
    ///
    /// The façade owns its own `Avro` instance and delegates to
    /// ``AvroIPCRequest``, using `context.clientCache` for session state.
    ///
    /// ```swift
    /// let client = try avro.makeIPCClient(
    ///     clientHash:     myHash,
    ///     clientProtocol: "com.example.MyProtocol",
    ///     context:        context
    /// )
    /// let handshake = try client.initHandshakeRequest()
    /// ```
    public func makeIPCClient(
        clientHash:     MD5Hash,
        clientProtocol: String,
        context:        AvroIPCContext
    ) throws -> MessageRequest {
        try MessageRequest(context: context, clientHash: clientHash, clientProtocol: clientProtocol)
    }

    /// Creates a server-side IPC façade.
    ///
    /// The façade owns its own `Avro` instance and delegates to
    /// ``AvroIPCResponse``, using `context.serverCache` for session state.
    ///
    /// ```swift
    /// let server = try avro.makeIPCServer(
    ///     serverHash:     myHash,
    ///     serverProtocol: "com.example.MyProtocol",
    ///     context:        context
    /// )
    /// let (request, responseData) = try await server.resolveHandshakeRequest(from: data)
    /// ```
    public func makeIPCServer(
        serverHash:     MD5Hash,
        serverProtocol: String,
        context:        AvroIPCContext
    ) throws -> MessageResponse {
        try MessageResponse(context: context, serverHash: serverHash, serverProtocol: serverProtocol)
    }
 */
}

// MARK: - Options

public enum AvroSchemaEncodingOption: Int {
    case CanonicalForm = 0, FullForm, PrettyPrintedForm
}

public enum AvroEncodingOption: Int {
    case AvroBinary = 0, AvroJson
}

// MARK: - AvroDataReader

/// Reads consecutive Avro-encoded records from a buffer, advancing the
/// position automatically after each successful decode.
///
/// ```swift
/// let reader = avro.makeDataReader(data: buffer)
/// while !reader.isAtEnd {
///     let record: MyRecord = try reader.decode(schema: schema)
///     process(record)
/// }
/// ```
public class AvroDataReader {

    private let data: Data
    private var offset: Int = 0

    public var isAtEnd:        Bool { offset >= data.count }
    public var bytesRemaining: Int  { data.count - offset }

    init(data: Data) {
        self.data = data
    }

    /// Decodes the next typed value from the buffer, advancing the read position.
    public func decode<T: Decodable>(schema: AvroSchema) throws -> T {
        let (value, consumed): (T, Int) = try decodeContinue(schema: schema) { try T(from: $0) }
        offset += consumed
        return value
    }

    /// Decodes the next untyped value from the buffer, advancing the read position.
    public func decode(schema: AvroSchema) throws -> Any? {
        let (value, consumed): (Any?, Int) = try decodeContinue(schema: schema) { try $0.decode(schema: schema) }
        offset += consumed
        return value
    }

    private func decodeContinue<T>(
        schema: AvroSchema,
        initializer: (AvroBinaryDecoder) throws -> T
    ) throws -> (T, Int) {
        let slice = data[offset...]
        return try Data(slice).withUnsafeBytes { (buffer: UnsafeRawBufferPointer) in
            guard let base = buffer.baseAddress else {
                throw AvroCodingError.decodingFailed("Empty data buffer")
            }
            let pointer = base.assumingMemoryBound(to: UInt8.self)
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: slice.count)
            let decoded = try initializer(decoder)
            return (decoded, slice.count - decoder.primitive.available)
        }
    }
}
