//
//  AvroClient/AvroEncodable.swift
//
//  Created by Yang Liu on 1/09/18.
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
// MARK: - AvroEncoder


public final class AvroEncoder {
    public var userInfo: [CodingUserInfoKey: Any] = [:]
    private let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!

    init() {
        userInfo[infoKey] = AvroEncodingOption.AvroBinary
    }

    func setUserInfo(userInfo: [CodingUserInfoKey: Any]) {
        self.userInfo = userInfo
    }

    func encode<T: Encodable>(_ value: T, schema: AvroSchema) throws -> Data {
        guard let option = userInfo[infoKey] as? AvroEncodingOption else {
            throw BinaryEncodingError.noEncoderSpecified
        }
        switch option {
        case .AvroBinary:
            let encoder = AvroBinaryEncoder(schema: schema)
            try encoder.encode(value)
            return encoder.getData()
        case .AvroJson:
            let encoder = AvroJSONEncoder(schema: schema)
            try encoder.encode(value)
            return try encoder.getData()
        }
    }

    func sizeOf<T: Encodable>(_ value: T, schema: AvroSchema) throws -> Int {
        let encoder = AvroBinaryEncoder(schema: schema, primitive: AvroPrimitiveSizer())
        try encoder.encode(value)
        return encoder.getSize()
    }
}

// MARK: - AvroBinaryEncoder


private final class AvroBinaryEncoder: Encoder {
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]

    var primitive: any AvroPrimitiveEncodeProtocol
    private(set) var schema: AvroSchema
    private(set) var currentMirror: Mirror?
    var encodeKey: Bool = false
    private var unkeyedContainerCache: AvroUnkeyedEncodingContainer?

    init(schema: AvroSchema) {
        self.schema = schema
        self.primitive = AvroPrimitiveEncoder()
    }

    init(other: inout AvroBinaryEncoder, schema: AvroSchema) {
        self.schema = schema
        self.primitive = other.primitive
    }

    init(schema: AvroSchema, primitive: any AvroPrimitiveEncodeProtocol) {
        self.schema = schema
        self.primitive = primitive
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(AvroKeyedEncodingContainer<Key>(encoder: self, schema: schema))
    }

    func unkeyedContainer() -> UnkeyedEncodingContainer {
        if let cache = unkeyedContainerCache { return cache }
        let container = AvroUnkeyedEncodingContainer(encoder: self, schema: schema)
        unkeyedContainerCache = container
        return container
    }

    func singleValueContainer() -> SingleValueEncodingContainer {
        AvroSingleEncodingContainer(encoder: self, schema: schema)
    }

    func encode<T: Encodable>(_ value: T) throws {
        switch schema {
        case .bytesSchema:
            guard let bytes = value as? [UInt8] else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            primitive.encode(bytes)

        case .fixedSchema(let fixed):
            if fixed.logicalType == .duration {
                guard let values = value as? [UInt32] else {
                    throw BinaryEncodingError.typeMismatchWithSchema
                }
                primitive.encode(fixed: values)
            } else {
                guard let bytes = value as? [UInt8] else {
                    throw BinaryEncodingError.typeMismatchWithSchema
                }
                primitive.encode(fixed: bytes)
            }

        case .arraySchema:
            var container = unkeyedContainer()
            try container.encode(value)

        case .mapSchema:
            let mirror = Mirror(reflecting: value)
            if mirror.displayStyle == .dictionary {
                primitive.encode(mirror.children.count)
                if !mirror.children.isEmpty {
                    encodeKey = true
                    try value.encode(to: self)
                    primitive.encode(UInt8(0))
                }
            } else {
                encodeKey = false
                try value.encode(to: self)
            }

        case .recordSchema, .errorSchema:
            currentMirror = Mirror(reflecting: value)
            try value.encode(to: self)

        default:
            try value.encode(to: self)
        }
    }

    func getData() -> Data { Data(primitive.buffer) }
    func getSize() -> Int  { primitive.size }
}

// MARK: - AvroKeyedEncodingContainer

private struct AvroKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K

    var codingPath: [CodingKey] { encoder.codingPath }

    private var encoder: AvroBinaryEncoder
    private var schemaMap: [String: AvroSchema] = [:]
    private var valueChildren: Mirror.Children?
    private var schema: AvroSchema

    private func schema(for key: K) -> AvroSchema {
        schemaMap[key.stringValue] ?? schema
    }

    // MARK: Nil / union index helpers

    mutating func encodeNilIndicesBefore(forKey key: K) {
        guard var children = valueChildren else { return }
        let count = children.count
        for _ in 0..<count {
            guard let child = children.popFirst() else { break }
            if child.label == key.stringValue {
                valueChildren = children  // FIX: persist consumed children before returning
                return
            }
            if case Optional<Any>.none = child.value {
                encodeNullUnionIndex(for: child.label)
            }
        }
        valueChildren = children
    }

    mutating func encodeNilIndicesAfter(forKey key: K) {
        guard let children = valueChildren, !children.isEmpty else { return }
        guard !children.contains(where: { child in
            if case Optional<Any>.none = child.value { return false }
            return true
        }) else { return }
        children.forEach { child in encodeNullUnionIndex(for: child.label) }
    }

    private func encodeNullUnionIndex(for label: String?) {
        guard let label,
              let fieldSchema = schemaMap[label],
              fieldSchema.isUnion(),
              let nullIndex = fieldSchema.getUnionList().firstIndex(where: { $0.isNull() })
        else { return }
        encoder.primitive.encode(nullIndex)
    }

    @discardableResult
    mutating func encodeUnionIndex(for key: K, typeName: AvroSchema.Types) -> Bool {
        guard case .unionSchema(let union) = schema(for: key),
              let index = union.branches.firstIndex(where: { $0.getTypeName() == typeName.rawValue })
        else { return false }
        encoder.primitive.encode(index)
        return true
    }

    // MARK: Primitive encode overloads

    mutating func encodeNil(forKey key: K) throws {
        guard schema(for: key).isNull() else { throw BinaryEncodingError.typeMismatchWithSchemaNil }
        encoder.primitive.encodeNull()
    }

    mutating func encode(_ value: Bool,   forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isBoolean() || encodeUnionIndex(for: key, typeName: .boolean) else {
            throw BinaryEncodingError.typeMismatchWithSchemaBool
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: String, forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isString() || encodeUnionIndex(for: key, typeName: .string) else {
            throw BinaryEncodingError.typeMismatchWithSchemaString
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Double, forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isDouble() || encodeUnionIndex(for: key, typeName: .double) else {
            throw BinaryEncodingError.typeMismatchWithSchemaDouble
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Float,  forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isFloat() || encodeUnionIndex(for: key, typeName: .float) else {
            throw BinaryEncodingError.typeMismatchWithSchemaFloat
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Int,    forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isInt() || encodeUnionIndex(for: key, typeName: .int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Int8,   forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isInt() || encodeUnionIndex(for: key, typeName: .int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt8
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Int16,  forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isInt() || encodeUnionIndex(for: key, typeName: .int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt16
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Int32,  forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isInt() || encodeUnionIndex(for: key, typeName: .int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt32
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: Int64,  forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isLong() || encodeUnionIndex(for: key, typeName: .long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt64
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: UInt,   forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isLong() || encodeUnionIndex(for: key, typeName: .long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt
        }
        try encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: UInt8,  forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isFixed() || encodeUnionIndex(for: key, typeName: .fixed) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt8
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: UInt16, forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isInt() || encodeUnionIndex(for: key, typeName: .int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt16
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: UInt32, forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isLong() || encodeUnionIndex(for: key, typeName: .long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt32
        }
        encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode(_ value: UInt64, forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        guard schema(for: key).isLong() || encodeUnionIndex(for: key, typeName: .long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt64
        }
        try encoder.primitive.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    mutating func encode<T: Encodable>(_ value: T, forKey key: K) throws {
        encodeNilIndicesBefore(forKey: key)
        switch schema(for: key) {
        case .mapSchema(let map) where encoder.encodeKey:
            encoder.primitive.encode(key.stringValue)
            let inner = AvroBinaryEncoder(other: &encoder, schema: map.values)
            try inner.encode(value)
            return
        case .unionSchema(let union):
            if let index = union.branches.firstIndex(where: { !$0.isNull() }) {
                encoder.primitive.encode(index)
                schemaMap[key.stringValue] = union.branches[index]
            }
        default:
            break
        }
        var container = nestedUnkeyedContainer(forKey: key)
        try container.encode(value)
        encodeNilIndicesAfter(forKey: key)
    }

    // MARK: Nested containers

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> {
        KeyedEncodingContainer(AvroKeyedEncodingContainer<NestedKey>(encoder: encoder, schema: schema(for: key)))
    }

    mutating func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer {
        let s = schema(for: key)
        let inner = AvroBinaryEncoder(other: &encoder, schema: s)
        return AvroUnkeyedEncodingContainer(encoder: inner, schema: s)
    }

    mutating func superEncoder() -> Encoder { encoder }
    mutating func superEncoder(forKey key: K) -> Encoder { encoder }

    // MARK: Schema map setup

    private mutating func buildSchemaMap(from record: AvroSchema.RecordSchema) {
        guard let mirror = encoder.currentMirror else { return }
        for child in mirror.children {
            guard let label = child.label, label != "fields" else {
                record.fields.forEach { schemaMap[$0.name] = $0.type }
                continue
            }
            schemaMap[label] = record.findSchema(name: label)
        }
    }

    fileprivate init(encoder: AvroBinaryEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
        self.valueChildren = encoder.currentMirror?.children
        if case .recordSchema(let record) = schema {
            buildSchemaMap(from: record)
        } else if case .errorSchema(let record) = schema {
            buildSchemaMap(from: record)
        }
    }
}

// MARK: - AvroUnkeyedEncodingContainer

private struct AvroUnkeyedEncodingContainer: UnkeyedEncodingContainer, EncodingHelper {
    var codingPath: [CodingKey] { [] }
    var encoder: AvroBinaryEncoder
    var schema: AvroSchema
    var count: Int = 0

    mutating func encode<T: Encodable>(_ value: T) throws {
        defer { count += 1 }
        switch schema {
        case .bytesSchema:
            guard let bytes = value as? [UInt8] else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            encoder.primitive.encode(bytes)

        case .fixedSchema(let fixed):
            if fixed.logicalType == .duration {
                guard let values = value as? [UInt32] else {
                    throw BinaryEncodingError.typeMismatchWithSchema
                }
                encoder.primitive.encode(fixed: values)
            } else {
                guard let bytes = value as? [UInt8] else {
                    throw BinaryEncodingError.typeMismatchWithSchema
                }
                encoder.primitive.encode(fixed: bytes)
            }

        case .arraySchema(let array):
            let mirror = Mirror(reflecting: value)
            encoder.primitive.encode(mirror.children.count)
            if !mirror.children.isEmpty {
                let inner = AvroBinaryEncoder(other: &encoder, schema: array.items)
                try value.encode(to: inner)
                encoder.primitive.encode(UInt8(0))
            }

        default:
            try encoder.encode(value)
        }
    }

    func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        KeyedEncodingContainer(AvroKeyedEncodingContainer<NestedKey>(encoder: encoder, schema: schema))
    }

    func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        AvroUnkeyedEncodingContainer(encoder: encoder, schema: schema)
    }

    func superEncoder() -> Encoder { encoder }

    init(encoder: AvroBinaryEncoder, schema: AvroSchema) {
        self.encoder = encoder
        if case .unionSchema(let union) = schema,
           let nonNull = union.branches.first(where: { !$0.isNull() }) {
            self.schema = nonNull
        } else {
            self.schema = schema
        }
    }
}

// MARK: - AvroSingleEncodingContainer

private struct AvroSingleEncodingContainer: SingleValueEncodingContainer, EncodingHelper {
    var codingPath: [CodingKey] { [] }
    var encoder: AvroBinaryEncoder
    var schema: AvroSchema

    mutating func encode<T: Encodable>(_ value: T) throws {
        try value.encode(to: encoder)
    }
}

// MARK: - EncodingHelper

private protocol EncodingHelper {
    var codingPath: [CodingKey] { get }
    var encoder: AvroBinaryEncoder { get set }
    var schema: AvroSchema { get }
}

extension EncodingHelper {

    mutating func encodeNil() throws {
        switch schema {
        case .nullSchema:
            encoder.primitive.encodeNull()
        case .unionSchema(let union):
            guard let nullIndex = union.branches.firstIndex(where: { $0.isNull() }) else { return }
            encoder.primitive.encode(nullIndex)
        default:
            throw BinaryEncodingError.typeMismatchWithSchemaNil
        }
    }

    mutating func encode(_ value: Bool) throws {
        guard schema.isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchemaBool }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Int) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchemaInt }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Int8) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchemaInt8 }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Int16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchemaInt16 }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Int32) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchemaInt32 }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Int64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchemaInt64 }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: UInt) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchemaUInt }
        try encoder.primitive.encode(value)
    }

    mutating func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema, .fixedSchema:
            encoder.primitive.encode(value)
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    mutating func encode(_ value: UInt16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchemaInt16 }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: UInt32) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchemaUInt32 }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: UInt64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchemaUInt64 }
        try encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Float) throws {
        guard schema.isFloat() else { throw BinaryEncodingError.typeMismatchWithSchemaFloat }
        encoder.primitive.encode(value)
    }

    mutating func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            encoder.primitive.encode(value)
        case .intSchema(let param) where param.logicalType == .date:
            encoder.primitive.encode(Int(value + Date.timeIntervalBetween1970AndReferenceDate))
        case .intSchema(let param) where param.logicalType == .timeMillis:
            encoder.primitive.encode(Int64(value))
        case .longSchema(let param) where param.logicalType == .timeMicros:
            encoder.primitive.encode(Int64(value))
        case .longSchema(let param) where param.logicalType == .timestampMillis:
            encoder.primitive.encode(Int64(value))
        case .longSchema(let param) where param.logicalType == .timestampMicros:
            encoder.primitive.encode(Int64(value))
        default:
            throw BinaryEncodingError.typeMismatchWithSchemaDouble
        }
    }

    mutating func encode(_ value: String) throws {
        switch schema {
        case .stringSchema(let param):
            if param.logicalType == .uuid {
                guard UUID(uuidString: value) != nil else {
                    throw BinaryEncodingError.typeMismatchWithSchemaString
                }
            }
            encoder.primitive.encode(value)
        case .enumSchema(let attribute):
            guard let id = attribute.symbols.firstIndex(of: value) else {
                throw BinaryEncodingError.typeMismatchWithSchemaString
            }
            encoder.primitive.encode(id)
        case .unionSchema(let union):
            if let id = union.branches.firstIndex(where: {
                if case .stringSchema = $0 { return true }; return false
            }) {
                encoder.primitive.encode(id)
                encoder.primitive.encode(value)
            }
        default:
            throw BinaryEncodingError.typeMismatchWithSchemaString
        }
    }

}

