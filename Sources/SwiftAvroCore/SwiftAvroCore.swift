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

    // MARK: - Streaming / continuation decode

    /// Decodes a value and returns it alongside the number of bytes consumed.
    public func decodeFromContinue(from data: Data, schema: AvroSchema) throws -> (Any?, Int) {
        return try decodeContinue(from: data, schema: schema) { try $0.decode(schema: schema) }
    }

    /// Decodes a typed value and returns it alongside the number of bytes consumed.
    public func decodeFromContinue<T: Decodable>(from data: Data, schema: AvroSchema) throws -> (T, Int) {
        return try decodeContinue(from: data, schema: schema) { try T(from: $0) }
    }

    private func decodeContinue<T>(
        from data: Data,
        schema: AvroSchema,
        initializer: (AvroBinaryDecoder) throws -> T
    ) throws -> (T, Int) {
        try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) in
            guard let base = buffer.baseAddress else {
                throw AvroCodingError.decodingFailed("Empty data buffer")
            }
            let pointer = base.assumingMemoryBound(to: UInt8.self)
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
            let decoded = try initializer(decoder)
            return (decoded, data.count - decoder.primitive.available)
        }
    }

    // MARK: - Object container

    public func makeFileObjectContainer(schema: String? = nil, codec: CodecProtocol) throws -> ObjectContainer {
        try ObjectContainer(schema: schema, codec: codec)
    }
}

// MARK: - Options

public enum AvroSchemaEncodingOption: Int {
    case CanonicalForm = 0, FullForm, PrettyPrintedForm
}

public enum AvroEncodingOption: Int {
    case AvroBinary = 0, AvroJson
}

// Retained for compatibility with generated test stubs.
struct SwiftAvroCore {
    var text = "SwiftAvroCore"
}
