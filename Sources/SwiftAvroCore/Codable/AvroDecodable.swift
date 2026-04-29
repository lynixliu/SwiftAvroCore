//
//  AvroClient/AvroDecodable.swift
//
//  Created by Yang Liu on 6/09/18.
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

final class AvroDecoder {
    private let schema: AvroSchema
    private let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!

    var userInfo: [CodingUserInfoKey: Any] = [:]

    init(schema: AvroSchema) {
        self.schema = schema
        userInfo[infoKey] = AvroEncodingOption.AvroBinary
    }

    func setUserInfo(userInfo: [CodingUserInfoKey: Any]) {
        self.userInfo = userInfo
    }

    func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let encodingOption = userInfo[infoKey] as! AvroEncodingOption
        switch encodingOption {
        case .AvroBinary:
            return try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) in
                guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                    throw BinaryDecodingError.outOfBufferBoundary
                }
                let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
                return try type.init(from: decoder)
            }
        case .AvroJson:
            return try JSONDecoder().decode(type, from: data)
        }
    }

    func decode<K: Decodable, T: Decodable>(_ type: [K: T].Type, from data: Data) throws -> [K: T] {
        return try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
            return try [K: T](decoder: decoder)
        }
    }

    func decode(from data: Data) throws -> Any? {
        return try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
            return try decoder.decode(schema: schema)
        }
    }
}

// MARK: - AvroBinaryDecoder

final class AvroBinaryDecoder: Decoder {
    var codingPath: [CodingKey] { myCodingPath }
    var userInfo: [CodingUserInfoKey: Any] = [:]
    private(set) var myCodingPath: [CodingKey] = []

    var primitive: any AvroBinaryDecodableProtocol
    private(set) var schema: AvroSchema

    init(schema: AvroSchema, pointer: UnsafePointer<UInt8>, size: Int) throws {
        self.schema = schema
        self.primitive = AvroPrimitiveDecoder(pointer: pointer, size: size)
    }

    fileprivate init(other: AvroBinaryDecoder, schema: AvroSchema) {
        self.schema = schema
        self.primitive = other.primitive
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(AvroKeyedDecodingContainer<Key>(decoder: self, schema: schema))
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        try AvroUnkeyedDecodingContainer(decoder: self, schema: schema)
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        try AvroSingleValueDecodingContainer(decoder: self, schema: schema)
    }

    func decode(schema: AvroSchema) throws -> Any? {
        switch schema {
        case .nullSchema:
            return nil

        case .booleanSchema:
            return try primitive.decode() as Bool

        case .intSchema(let intSchema):
            if intSchema.logicalType == .date {
                return Date(timeIntervalSince1970: Double(try primitive.decode() as Int))
            }
            return try primitive.decode() as Int32

        case .longSchema:
            return try primitive.decode() as Int64

        case .floatSchema:
            return try primitive.decode() as Float

        case .doubleSchema:
            return try primitive.decode() as Double

        case .bytesSchema:
            return try primitive.decode() as [UInt8]

        case .stringSchema(_):
            return try primitive.decode() as String

        case .recordSchema(let record):
            return try record.fields.reduce(into: [String: Any]()) { result, field in
                result[field.name] = try decode(schema: field.type)
            }

        case .enumSchema(let enumSchema):
            let index = try primitive.decode() as Int
            guard (0..<enumSchema.symbols.count).contains(index) else {
                throw BinaryDecodingError.indexOutofBoundary
            }
            return enumSchema.symbols[index]

        case .arraySchema(let arraySchema):
            var values: [Any] = []
            var blockCount = try primitive.decode() as Int64
            while blockCount != 0 {
                let count = abs(blockCount)
                let hasBlockSize = blockCount < 0
                let beforeAvailable = primitive.available
                for _ in 0..<count {
                    if hasBlockSize {
                        let blockSize = try primitive.decode() as Int64
                        guard blockSize > 0 else { return values }
                        if let v = try decode(schema: arraySchema.items) {
                            values.append(v)
                        } else {
                            primitive.advance(Int(blockSize) - (beforeAvailable - primitive.available))
                        }
                    } else {
                        if let v = try decode(schema: arraySchema.items) {
                            values.append(v)
                        }
                    }
                }
                blockCount = try primitive.decode() as Int64
            }
            return values

        case .mapSchema(let mapSchema):
            var pairs: [String: Any] = [:]
            var blockCount = try primitive.decode() as Int64
            while blockCount != 0 {
                let count = abs(blockCount)
                let hasBlockSize = blockCount < 0
                let beforeAvailable = primitive.available
                for _ in 0..<count {
                    if hasBlockSize {
                        let blockSize = try primitive.decode() as Int64
                        guard blockSize > 0 else { return pairs }
                        if let key = try? primitive.decode() as String {
                            pairs[key] = try decode(schema: mapSchema.values)
                        } else {
                            primitive.advance(Int(blockSize) - (beforeAvailable - primitive.available))
                        }
                    } else {
                        let key = try primitive.decode() as String
                        pairs[key] = try decode(schema: mapSchema.values)
                    }
                }
                blockCount = try primitive.decode() as Int64
            }
            return pairs

        case .unionSchema(let unionSchema):
            let index = try primitive.decode() as Int64
            guard (0..<Int64(unionSchema.branches.count)).contains(index) else {
                throw BinaryDecodingError.indexOutofBoundary
            }
            return try decode(schema: unionSchema.branches[Int(index)])

        case .fixedSchema(let fixedSchema):
            if fixedSchema.logicalType == .duration {
                return try primitive.decode(fixedSize: fixedSchema.size) as [UInt32]
            }
            return try primitive.decode(fixedSize: fixedSchema.size) as [UInt8]

        case .errorSchema(let errorSchema):
            return try errorSchema.fields.reduce(into: [String: Any]()) { result, field in
                result[field.name] = try decode(schema: field.type)
            }

        default:
            return nil
        }
    }
}

// MARK: - AvroKeyedDecodingContainer

private struct AvroKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {

    // Cache for resolved union branch indices, keyed by field name.
    private final class UnionIndexCache {
        private var indexMap: [String: Int] = [:]
        func set(_ index: Int, for key: String) { indexMap[key] = index }
        func index(for key: String) -> Int? { indexMap[key] }
    }

    var codingPath: [CodingKey] = []
    private var decoder: AvroBinaryDecoder
    private var schemaMap: [String: AvroSchema] = [:]
    private let unionIndex = UnionIndexCache()

    var allKeys: [K] {
        schemaMap.keys.compactMap { K(stringValue: $0) }
    }

    func contains(_ key: K) -> Bool {
        schemaMap.keys.contains(key.stringValue)
    }

    private func schema(for key: K) throws -> AvroSchema {
        guard let s = schemaMap[key.stringValue] else {
            throw BinaryDecodingError.malformedAvro  // or a new .unknownField error
        }
        if case .unionSchema(let union) = s, let index = unionIndex.index(for: key.stringValue) {
            return union.branches[index]
        }
        return s
    }

    func decodeNil(forKey key: K) throws -> Bool {
        switch try schema(for: key) {
        case .nullSchema:
            return true
        case .unionSchema(let union):
            let index = try decoder.primitive.decode() as Int
            guard index < union.branches.count else {
                throw BinaryDecodingError.indexOutofBoundary
            }
            unionIndex.set(index, for: key.stringValue)
            return union.branches[index].isNull()
        default:
            return false
        }
    }

    @inlinable func decode<T: Decodable>(_ type: T.Type, forKey key: K) throws -> T {
        let currentSchema = try schema(for: key)
        switch currentSchema {
        case .fixedSchema:
            var container = try nestedUnkeyedContainer(forKey: key)
            return try container.decode(type)
        case .unknownSchema:
            throw BinaryEncodingError.invalidSchema
        default:
            return try type.init(from: AvroBinaryDecoder(other: decoder, schema: currentSchema))
        }
    }

    func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> {
        KeyedDecodingContainer(AvroKeyedDecodingContainer<NestedKey>(decoder: decoder, schema: try schema(for: key)))
    }

    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        try AvroUnkeyedDecodingContainer(decoder: decoder, schema: schema(for: key))
    }

    func superDecoder() throws -> Decoder { decoder }
    func superDecoder(forKey key: K) throws -> Decoder { decoder }

    fileprivate init(decoder: AvroBinaryDecoder, schema: AvroSchema) {
        self.decoder = decoder
        self.codingPath = decoder.codingPath
        switch schema {
        case .recordSchema(let record):
            record.fields.forEach { schemaMap[$0.name] = $0.type }
            schemaMap["fields"] = .fieldsSchema(record.fields)
        case .errorSchema(let record):
            record.fields.forEach { schemaMap[$0.name] = $0.type }
            schemaMap["fields"] = .fieldsSchema(record.fields)
        case .fieldsSchema(let fields):
            fields.forEach { schemaMap[$0.name] = $0.type }
        case .mapSchema(let map):
            schemaMap[map.type] = map.values
        case .fieldSchema(let field):
            schemaMap[field.name] = field.type
        default:
            schemaMap[schema.getName()!] = schema
        }
    }
}

// MARK: - AvroUnkeyedDecodingContainer

private struct AvroUnkeyedDecodingContainer: UnkeyedDecodingContainer, DecodingHelper {
    var codingPath: [CodingKey]
    var schema: AvroSchema
    var decoder: AvroBinaryDecoder

    private var keySchema: AvroSchema?
    private var valueSchema: AvroSchema
    private var haveTail: Bool = false
    private var haveBlock: Bool = false
    private var countValue: Int

    var count: Int? { countValue }
    var isAtEnd: Bool { currentIndex >= countValue }
    var currentIndex: Int = 0

    private func currentSchema() throws -> AvroSchema {
        guard countValue > 0 else { return valueSchema }
        if let k = keySchema {
            if currentIndex % 2 == 0 {
                if haveBlock { _ = try decoder.primitive.decode() as Int64 }
                return k
            }
            return valueSchema
        }
        // Only consume the per-element block size for leaf (non-container) schemas.
        // Nested arrays/maps/records read their own counts in their own init.
        if haveBlock, currentIndex < countValue {
            switch valueSchema {
            case .arraySchema, .mapSchema, .recordSchema, .errorSchema:
                break  // nested container reads its own size — don't double-consume
            default:
                _ = try decoder.primitive.decode() as Int64
            }
        }
        return valueSchema
    }

    mutating func advanceIndex() {
        currentIndex += 1
        guard currentIndex == countValue, haveTail else { return }
        guard var blockCount = try? decoder.primitive.decode() as Int64, blockCount != 0 else { return }
        if blockCount < 0 {
            haveBlock = true
            blockCount = -blockCount
        }
        if keySchema != nil { blockCount <<= 1 }
        countValue += Int(blockCount)
    }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        defer { advanceIndex() }
        return KeyedDecodingContainer(AvroKeyedDecodingContainer(decoder: decoder, schema: schema))
    }

    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        try AvroUnkeyedDecodingContainer(decoder: decoder, schema: schema)
    }

    mutating func superDecoder() throws -> Decoder {
        defer { advanceIndex() }
        return decoder
    }

    @inlinable
    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
        defer { advanceIndex() }
        let schema = try currentSchema()
        // Swift's Dictionary<String,V>.init(from:) uses KeyedDecodingContainer,
        // which doesn't work for Avro maps. Route through AvroDecodable instead.
        if case .mapSchema = schema, let avroDecodable = type as? any AvroDecodable.Type {
            return try avroDecodable.init(decoder: AvroBinaryDecoder(other: decoder, schema: schema)) as! T
        }
        return try type.init(from: AvroBinaryDecoder(other: decoder, schema: schema))
    }

    fileprivate init(decoder: AvroBinaryDecoder, schema: AvroSchema) throws {
        self.decoder = decoder
        self.codingPath = decoder.codingPath

        switch schema {
        case .arraySchema(let array):
            let blockCount = try decoder.primitive.decode() as Int64
            countValue = blockCount < 0 ? -Int(blockCount) : Int(blockCount)
            haveBlock = blockCount < 0
            valueSchema = array.items
            haveTail = true
            self.schema = valueSchema

        case .bytesSchema:
            countValue = Int(try decoder.primitive.decode() as Int64)
            valueSchema = schema
            self.schema = valueSchema

        case .mapSchema(let map):
            let blockCount = try decoder.primitive.decode() as Int64
            countValue = blockCount < 0 ? Int(-blockCount) * 2 : Int(blockCount) * 2
            haveBlock = blockCount < 0
            self.schema = .stringSchema(AvroSchema.StringSchema())
            keySchema = .stringSchema(AvroSchema.StringSchema())
            valueSchema = map.values
            haveTail = true

        case .fixedSchema(let fixed):
            countValue = fixed.logicalType == .duration ? 3 : fixed.size
            valueSchema = schema
            self.schema = valueSchema

        default:
            valueSchema = schema
            self.schema = valueSchema
            countValue = 1
        }
    }
}

// MARK: - AvroSingleValueDecodingContainer

private struct AvroSingleValueDecodingContainer: SingleValueDecodingContainer, DecodingHelper {
    var codingPath: [CodingKey]
    var schema: AvroSchema
    var decoder: AvroBinaryDecoder

    fileprivate init(decoder: AvroBinaryDecoder, schema: AvroSchema) throws {
        self.decoder = decoder
        self.codingPath = decoder.codingPath
        switch schema {
        case .recordSchema:
            self.schema = schema
        case .unionSchema(let union):
            let index = Int(try decoder.primitive.decode() as Int64)
            guard index >= 0, index < union.branches.count else {
                throw BinaryDecodingError.indexOutofBoundary
            }
            self.schema = union.branches[index]
        default:
            self.schema = schema.getSerializedSchema().first!
        }
    }

    func decode(_ type: String.Type) throws -> String {
        switch schema {
        case .stringSchema(_):
            return try decoder.primitive.decode() as String
        case .enumSchema(let symbols):
            return symbols.symbols[try decoder.primitive.decode() as Int]
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
    }

    func decodeIfPresent(_ type: String.Type) throws -> String? {
        switch schema {
        case .stringSchema(_):
            return try decoder.primitive.decode() as String
        case .enumSchema(let symbols):
            return symbols.symbols[try decoder.primitive.decode() as Int]
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
    }
}

// MARK: - DecodingHelper

private protocol DecodingHelper {
    var codingPath: [CodingKey] { get }
    var decoder: AvroBinaryDecoder { get set }
    var schema: AvroSchema { get }
}

extension DecodingHelper {
    func decodeNil() -> Bool { schema.isNull() }

    @inlinable func decode(_ type: Bool.Type)   throws -> Bool   {
        guard schema.isBoolean() else { throw BinaryDecodingError.typeMismatchWithSchemaBool }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Int.Type)    throws -> Int    {
        guard schema.isLong() || schema.isInt() else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Int8.Type)   throws -> Int8   {
        guard schema.isInteger() else { throw BinaryDecodingError.typeMismatchWithSchemaInt8 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Int16.Type)  throws -> Int16  {
        guard schema.isInteger() else { throw BinaryDecodingError.typeMismatchWithSchemaInt16 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Int32.Type)  throws -> Int32  {
        guard schema.isInt() || schema.isContainer() else { throw BinaryDecodingError.typeMismatchWithSchemaInt32 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Int64.Type)  throws -> Int64  {
        guard schema.isLong() else { throw BinaryDecodingError.typeMismatchWithSchemaInt64 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: UInt.Type)   throws -> UInt   {
        guard schema.isInteger() else { throw BinaryDecodingError.typeMismatchWithSchemaUInt }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: UInt8.Type)  throws -> UInt8  {
        guard schema.isByte() else { throw BinaryDecodingError.typeMismatchWithSchemaUInt8 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: UInt16.Type) throws -> UInt16 {
        guard schema.isInteger() else { throw BinaryDecodingError.typeMismatchWithSchemaUInt16 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: UInt32.Type) throws -> UInt32 {
        guard schema.isFixed() else { throw BinaryDecodingError.typeMismatchWithSchemaUInt32 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: UInt64.Type) throws -> UInt64 {
        guard schema.isLong() else { throw BinaryDecodingError.typeMismatchWithSchemaUInt64 }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Float.Type)  throws -> Float  {
        guard schema.isFloat() else { throw BinaryDecodingError.typeMismatchWithSchemaFloat }
        return try decoder.primitive.decode()
    }
    @inlinable func decode(_ type: Double.Type) throws -> Double {
        switch schema {
        case .doubleSchema:
            return try decoder.primitive.decode()
        case .intSchema(let intSchema) where intSchema.logicalType == .date:
            // Swift's Date.init(from:) decodes via timeIntervalSinceReferenceDate,
            // so subtract the 1970–2001 offset to align with the Avro epoch.
            let unixDate = Double(try decoder.primitive.decode() as Int)
            return unixDate - Date.timeIntervalBetween1970AndReferenceDate
        case .intSchema(let intSchema) where intSchema.logicalType == .timeMillis:
            // time-millis: milliseconds since midnight, return as Double
            return Double(try decoder.primitive.decode() as Int)
        case .longSchema(let longSchema) where longSchema.logicalType == .timeMicros:
            // time-micros: microseconds since midnight, return as Double
            return Double(try decoder.primitive.decode() as Int64) / 1_000_000.0
        case .longSchema(let longSchema) where longSchema.logicalType == .timestampMillis:
            // timestamp-millis: milliseconds since 1970-01-01, return as Double
            return Double(try decoder.primitive.decode() as Int64) / 1000.0
        case .longSchema(let longSchema) where longSchema.logicalType == .timestampMicros:
            // timestamp-micros: microseconds since 1970-01-01, return as Double
            return Double(try decoder.primitive.decode() as Int64) / 1_000_000.0
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaDouble
        }
    }

    @inlinable func decode(_ type: String.Type) throws -> String {
        switch schema {
        case .stringSchema(let param):
            let value = try decoder.primitive.decode() as String
            if param.logicalType == .uuid {
                guard UUID(uuidString: value) != nil else {
                    throw BinaryDecodingError.typeMismatchWithSchemaString
                }
            }
            return value
        case .enumSchema(let symbols):
            return symbols.symbols[try decoder.primitive.decode() as Int]
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
    }

    @inlinable func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try type.init(from: AvroBinaryDecoder(other: decoder, schema: schema))
    }
}

// MARK: - Dictionary + AvroDecodable

protocol AvroDecodable: Decodable {
    init(decoder: AvroBinaryDecoder) throws
}

extension Dictionary: AvroDecodable where Key: Decodable, Value: Decodable {
    init(decoder: AvroBinaryDecoder) throws {
        self.init()
        var container = try decoder.unkeyedContainer()
        while !container.isAtEnd {
            let key = try container.decode(Key.self)
            guard !container.isAtEnd else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: decoder.codingPath,
                    debugDescription: "Unkeyed container ended before value in key-value pair."))
            }
            self[key] = try container.decode(Value.self)
        }
    }
}

extension KeyedDecodingContainer {
    func decode<MK: Decodable, T: Decodable>(_ type: [MK: T].Type, forKey key: Key) throws -> [MK: T] {
        guard contains(key) else { throw BinaryDecodingError.malformedAvro }
        var c = try nestedUnkeyedContainer(forKey: key)
        guard c.count != 0 else { return [:] }
        var values = [MK: T]()
        while !c.isAtEnd {
            let k = try c.decode(type.Key)
            guard !c.isAtEnd else {
                throw BinaryDecodingError.malformedAvro
            }
            values[k] = try c.decode(type.Value)
        }
        return values
    }

    func decodeIfPresent<MK: Decodable, T: Decodable>(_ type: [MK: T].Type, forKey key: Key) throws -> [MK: T]? {
        guard contains(key) else { throw BinaryDecodingError.malformedAvro }
        return try decodeNil(forKey: key) ? nil : decode(type, forKey: key)
    }
}
