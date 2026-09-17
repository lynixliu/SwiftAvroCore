//
//  AvroClient/AvroDecodable.swift
//
//  Created by Yang Liu on 6/09/18.
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
import Foundation

final class AvroDecoder {
    private let schema: AvroSchema
    private let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!
    private static let jsonDecoder = JSONDecoder()

    var userInfo: [CodingUserInfoKey: Any] = [:]

    init(schema: AvroSchema) {
        self.schema = schema
        userInfo[infoKey] = AvroEncodingOption.AvroBinary
    }

    func setUserInfo(userInfo: [CodingUserInfoKey: Any]) {
        self.userInfo = userInfo
    }

    /// Runs `body` against a binary decoder for `data`.
    ///
    /// An empty payload is valid Avro: null is written as zero bytes, so a null
    /// schema, or a record whose fields are all null, encodes to nothing at all.
    /// Data.withUnsafeBytes hands back a nil base address when the buffer is
    /// empty, so that case needs a valid pointer over a zero-length buffer. Any
    /// schema that does need bytes still fails, because every read in
    /// AvroPrimitiveDecoder checks the remaining count first.
    private func withBinaryDecoder<R>(_ data: Data, _ body: (AvroBinaryDecoder) throws -> R) throws -> R {
        guard !data.isEmpty else {
            let empty: [UInt8] = []
            return try empty.withUnsafeBufferPointer { buffer in
                let decoder = try AvroBinaryDecoder(schema: schema, pointer: buffer.baseAddress ?? UnsafePointer(bitPattern: MemoryLayout<UInt8>.alignment)!, size: 0)
                return try body(decoder)
            }
        }
        return try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) in
            let pointer = buffer.baseAddress!.assumingMemoryBound(to: UInt8.self)
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
            return try body(decoder)
        }
    }

    func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        guard let option = userInfo[infoKey] as? AvroEncodingOption else {
            throw BinaryEncodingError.noEncoderSpecified
        }
        switch option {
        case .AvroBinary:
            return try withBinaryDecoder(data) { decoder in
                if T.self == Date.self, let date = try decoder.decodeLogicalDate(schema: schema) {
                    return date as! T
                }
                return try type.init(from: decoder)
            }
        case .AvroJson:
            return try decodeJSON(type, from: data)
        }
    }

    private func decodeJSON<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        guard !data.isEmpty else { throw BinaryDecodingError.outOfBufferBoundary }
        let jsonValue = try Self.jsonDecoder.decode(JSONValue.self, from: data)
        let decoder = AvroJSONDecoder(schema: schema, value: jsonValue)
        let (branchSchema, branchValue) = try decoder.unwrapped()
        let branchDecoder = AvroJSONDecoder(schema: branchSchema, value: branchValue)
        if let date = try branchDecoder.decodeLogicalDate(schema: branchSchema), let result = date as? T {
            return result
        }
        return try type.init(from: decoder)
    }

    func decode<T: Decodable>(_ type: T.Type, from data: Data, readerSchema: AvroSchema) throws -> T {
        guard let option = userInfo[infoKey] as? AvroEncodingOption else {
            throw BinaryEncodingError.noEncoderSpecified
        }
        guard option == .AvroBinary else {
            return try decode(type, from: data)
        }
        let resolved = try decodeResolvedValue(from: data, readerSchema: readerSchema)
        if T.self == Date.self {
            guard let date = resolved as? Date, let result = date as? T else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return result
        }
        let json = try Self.jsonData(from: resolved)
        return try JSONDecoder().decode(type, from: json)
    }


    // Swift's Dictionary<K,V>.init(from:) uses a KeyedDecodingContainer, which cannot
    // carry the Avro map schema. The binary path therefore needs AvroDecodable instead.
    // The JSON path has a map-aware keyed container, so it uses the generic overload.
    func decode<K: Decodable, T: Decodable>(_ type: [K: T].Type, from data: Data) throws -> [K: T] {
        guard let option = userInfo[infoKey] as? AvroEncodingOption else {
            throw BinaryEncodingError.noEncoderSpecified
        }
        guard option == .AvroBinary else {
            // Call the JSON path directly: `decode(type:from:)` would resolve back
            // to this same overload and recurse.
            return try decodeJSON([K: T].self, from: data)
        }
        return try withBinaryDecoder(data) { decoder in
            try [K: T](decoder: decoder)
        }
    }

    func decode(from data: Data) throws -> Any? {
        try withBinaryDecoder(data) { decoder in
            try decoder.decode(schema: schema)
        }
    }

    func decode(from data: Data, readerSchema: AvroSchema) throws -> Any? {
        try decodeResolvedValue(from: data, readerSchema: readerSchema)
    }

    private func decodeResolvedValue(from data: Data, readerSchema: AvroSchema) throws -> Any? {
        let writerValue = try decode(from: data)
        return try readerSchema.resolveValue(writerValue, writtenBy: schema)
    }

    private static func jsonData(from value: Any?) throws -> Data {
        let object = jsonCompatible(value)
        return try JSONSerialization.data(withJSONObject: object as Any, options: [.fragmentsAllowed])
    }

    private static func jsonCompatible(_ value: Any?) -> Any {
        switch value {
        case nil:
            return NSNull()
        case let value as Date:
            return value.timeIntervalSinceReferenceDate
        case let value as Decimal:
            return NSDecimalNumber(decimal: value)
        case let value as [UInt8]:
            return value.map { Int($0) }
        case let value as [Any]:
            return value.map { jsonCompatible($0) }
        case let value as [String: Any]:
            return value.mapValues { jsonCompatible($0) }
        case let value as Float:
            return Double(value)
        default:
            return value as Any
        }
    }
}

// MARK: - AvroJSONDecoder

final class AvroJSONDecoder: Decoder {
    var codingPath: [CodingKey] { myCodingPath }
    var userInfo: [CodingUserInfoKey: Any] = [:]
    private(set) var myCodingPath: [CodingKey] = []

    private let value: JSONValue
    private let schema: AvroSchema

    init(schema: AvroSchema, value: JSONValue) {
        self.schema = schema
        self.value = value
    }

    func decodeLogicalDate(schema: AvroSchema) throws -> Date? {
        switch schema {
        case .intSchema(let s) where s.logicalType == .date:
            guard case .int(let v) = value else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
            return LogicalTypeConverter.decodeDate(Int(v))
        case .intSchema(let s) where s.logicalType == .timeMillis:
            guard case .int(let v) = value else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
            return LogicalTypeConverter.decodeTimeMillis(Int32(v))
        case .longSchema(let s) where s.logicalType == .timestampMillis:
            guard case .int(let v) = value else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
            return LogicalTypeConverter.decodeTimestampMillis(v)
        case .longSchema(let s) where s.logicalType == .timestampMicros:
            guard case .int(let v) = value else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
            return LogicalTypeConverter.decodeTimestampMicros(v)
        case .longSchema(let s) where s.logicalType == .timeMicros:
            guard case .int(let v) = value else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
            return LogicalTypeConverter.decodeTimeMicros(v)
        default:
            return nil
        }
    }

    fileprivate func unwrapped() throws -> (schema: AvroSchema, value: JSONValue) {
        if case .unionSchema(let union) = schema {
            if case .null = value, union.branches.contains(where: { $0.isNull() }) {
                return (.nullSchema, .null)
            }
            guard case .object(let dict) = value, dict.count == 1 else {
                throw DecodingError.typeMismatch(JSONValue.self, .init(codingPath: self.codingPath, debugDescription: "Expected union object with one key"))
            }

            let key = dict.keys.first!
            guard let branchIndex = union.branches.firstIndex(where: { Self.unionKey(for: $0) == key })
                ?? union.branches.firstIndex(where: { $0.getName() == key }) else {
                throw DecodingError.typeMismatch(JSONValue.self, .init(codingPath: self.codingPath, debugDescription: "Union branch not found for key: \(key)"))
            }
            return (union.branches[branchIndex], dict[key]!)
        }
        return (schema, value)
    }

    /// The JSON object key that names a union branch.
    /// Avro names a named type by its fullname, and any other type by its type name.
    /// A logical type uses the name of its underlying primitive, not the logical name,
    /// so `getName()` alone is not enough — it returns "date" for an int/date branch.
    fileprivate static func unionKey(for branch: AvroSchema) -> String? {
        switch branch {
        case .recordSchema, .errorSchema, .enumSchema, .fixedSchema:
            return branch.getFullname()
        case .nullSchema:    return "null"
        case .booleanSchema: return "boolean"
        case .intSchema:     return "int"
        case .longSchema:    return "long"
        case .floatSchema:   return "float"
        case .doubleSchema:  return "double"
        case .bytesSchema:   return "bytes"
        case .stringSchema:  return "string"
        case .arraySchema:   return "array"
        case .mapSchema:     return "map"
        default:             return branch.getName()
        }
    }

    /// Avro JSON writes bytes and fixed as a string, one character for each byte.
    /// Each character must therefore be in the ISO-8859-1 range.
    static func avroBytes(from string: String) throws -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(string.unicodeScalars.count)
        for scalar in string.unicodeScalars {
            guard scalar.value <= 0xFF else {
                throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Bytes string holds a code point above U+00FF"))
            }
            bytes.append(UInt8(scalar.value))
        }
        return bytes
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        let (s, v) = try unwrapped()
        return KeyedDecodingContainer(AvroJSONKeyedDecodingContainer<Key>(decoder: self, schema: s, value: v))
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        let (s, v) = try unwrapped()
        return try AvroJSONUnkeyedDecodingContainer(decoder: self, schema: s, value: v)
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        let (s, v) = try unwrapped()
        return AvroJSONSingleValueDecodingContainer(decoder: self, schema: s, value: v)
    }

}

private protocol AvroJSONDecodingHelper {
    var decoder: AvroJSONDecoder { get }
    var schema: AvroSchema { get }
}

extension AvroJSONDecodingHelper {
    func decodePrimitive<T>(_ type: T.Type, from value: JSONValue) throws -> T {
        switch (schema, value) {
        case (.booleanSchema, .bool(let v)): return v as! T
        case (.intSchema, .int(let v)): return Int32(v) as! T
        case (.longSchema, .int(let v)): return v as! T
        // JSON writes a whole number without a fractional part, so JSONValue
        // parses 1.0 as .int. A float or double schema must accept both cases.
        case (.floatSchema, .double(let v)): return Float(v) as! T
        case (.floatSchema, .int(let v)): return Float(v) as! T
        case (.doubleSchema, .double(let v)): return v as! T
        case (.doubleSchema, .int(let v)): return Double(v) as! T
        case (.stringSchema, .string(let v)): return v as! T
        case (.enumSchema, .string(let v)):
            guard schema.getEnumSymbols().contains(v) else {
                throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Enum symbol \(v) is not in the schema"))
            }
            return v as! T
        case (.bytesSchema, .string(let v)):
            return try AvroJSONDecoder.avroBytes(from: v) as! T
        case (.fixedSchema(let f), .string(let v)):
            let bytes = try AvroJSONDecoder.avroBytes(from: v)
            guard bytes.count == f.size else {
                throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Fixed value has \(bytes.count) bytes, the schema declares \(f.size)"))
            }
            return bytes as! T
        default:
            throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON"))
        }
    }

}

private struct AvroJSONKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    var codingPath: [CodingKey] = []
    private var decoder: AvroJSONDecoder
    private var schema: AvroSchema
    private var value: JSONValue

    init(decoder: AvroJSONDecoder, schema: AvroSchema, value: JSONValue) {
        self.decoder = decoder
        self.schema = schema
        self.value = value
    }

    var allKeys: [K] {
        guard case .object(let dict) = value else { return [] }
        return dict.keys.compactMap { K(stringValue: $0) }
    }

    func contains(_ key: K) -> Bool {
        guard case .object(let dict) = value else { return false }
        return dict.keys.contains(key.stringValue)
    }

    func decodeNil(forKey key: K) throws -> Bool {
        guard case .object(let dict) = value else { throw DecodingError.typeMismatch(KeyedDecodingContainer<K>.self, .init(codingPath: codingPath, debugDescription: "Expected object for keyed container")) }
        guard let val = dict[key.stringValue] else { return true }
        return val == .null
    }

    func decode<T: Decodable>(_ type: T.Type, forKey key: K) throws -> T {
        guard case .object(let dict) = value else { throw DecodingError.typeMismatch(KeyedDecodingContainer<K>.self, .init(codingPath: codingPath, debugDescription: "Expected object for keyed container")) }
        guard let val = dict[key.stringValue] else {
            guard let codingKey = JSONCodingKey(stringValue: key.stringValue) else {
                throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Invalid coding key"))
            }
            throw DecodingError.keyNotFound(codingKey, .init(codingPath: [], debugDescription: "Missing key"))
        }

        let fieldSchema = try getFieldSchema(for: key)
        let nestedDecoder = AvroJSONDecoder(schema: fieldSchema, value: val)
        // A logical type inside a union is only visible after the branch is
        // resolved, so unwrap first and then look for a date.
        let (branchSchema, branchValue) = try nestedDecoder.unwrapped()
        let branchDecoder = AvroJSONDecoder(schema: branchSchema, value: branchValue)
        if let date = try branchDecoder.decodeLogicalDate(schema: branchSchema), let result = date as? T {
            return result
        }
        return try type.init(from: nestedDecoder)
    }

    private func getFieldSchema(for key: K) throws -> AvroSchema {
        if case .mapSchema(let map) = schema {
            return map.values
        }
        guard case .recordSchema(let record) = schema else { throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON")) }
        guard let field = record.fields.first(where: { $0.name == key.stringValue }) else {
            throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON"))
        }
        return field.type
    }

    func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> {
        guard case .object(let dict) = value else { throw DecodingError.typeMismatch(KeyedDecodingContainer<K>.self, .init(codingPath: codingPath, debugDescription: "Expected object for keyed container")) }
        guard dict[key.stringValue] != nil else { throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON")) }
        let fieldSchema = try getFieldSchema(for: key)
        return KeyedDecodingContainer(AvroJSONKeyedDecodingContainer<NestedKey>(decoder: decoder, schema: fieldSchema, value: dict[key.stringValue]!))
    }

    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        guard case .object(let dict) = value else { throw DecodingError.typeMismatch(KeyedDecodingContainer<K>.self, .init(codingPath: codingPath, debugDescription: "Expected object for keyed container")) }
        guard dict[key.stringValue] != nil else { throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON")) }
        let fieldSchema = try getFieldSchema(for: key)
        return try AvroJSONUnkeyedDecodingContainer(decoder: decoder, schema: fieldSchema, value: dict[key.stringValue]!)
    }

    func superDecoder() throws -> Decoder { decoder }
    func superDecoder(forKey key: K) throws -> Decoder { decoder }
}

private struct AvroJSONUnkeyedDecodingContainer: UnkeyedDecodingContainer {
    var codingPath: [CodingKey] = []
    private var decoder: AvroJSONDecoder
    private var schema: AvroSchema
    private var valueSchema: AvroSchema?
    private var value: JSONValue
    private var sortedKeys: [String] = []
    fileprivate var currentIndex: Int = 0

    /// Bytes and fixed arrive as a JSON string, but Data and [UInt8] both decode
    /// through an unkeyed container. This holds the unpacked bytes for that case.
    private var bytes: [UInt8]?

    init(decoder: AvroJSONDecoder, schema: AvroSchema, value: JSONValue) throws {
        self.decoder = decoder
        self.schema = schema
        self.value = value

        switch (schema, value) {
        case (_, .array):
            break
        case (.mapSchema(let map), .object(let dict)):
            self.valueSchema = map.values
            self.sortedKeys = dict.keys.sorted()
        case (.bytesSchema, .string(let text)):
            self.bytes = try AvroJSONDecoder.avroBytes(from: text)
        case (.fixedSchema(let fixed), .string(let text)):
            let unpacked = try AvroJSONDecoder.avroBytes(from: text)
            guard unpacked.count == fixed.size else {
                throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "Fixed value has \(unpacked.count) bytes, the schema declares \(fixed.size)"))
            }
            self.bytes = unpacked
        default:
            throw DecodingError.typeMismatch(UnkeyedDecodingContainer.self, .init(codingPath: decoder.codingPath, debugDescription: "Expected array, map, bytes or fixed"))
        }
    }

    var count: Int? {
        if let bytes { return bytes.count }
        switch value {
        case .array(let arr): return arr.count
        case .object(let dict): return dict.count * 2
        default: return nil
        }
    }

    var isAtEnd: Bool {
        guard let total = count else { return true }
        return currentIndex >= total
    }

    /// Reports the element without consuming it. A container must not advance
    /// when decodeNil() returns false, or the element is lost.
    mutating func decodeNil() throws -> Bool {
        guard !isAtEnd else { throw BinaryDecodingError.outOfBufferBoundary }
        guard case .array(let arr) = value else { return false }
        return arr[currentIndex] == .null
    }

    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
        let currentVal: JSONValue
        var currentSchema = schema

        if let bytes {
            guard currentIndex < bytes.count else { throw BinaryDecodingError.outOfBufferBoundary }
            guard let byte = bytes[currentIndex] as? T else {
                throw DecodingError.typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "Expected UInt8 for a bytes or fixed schema"))
            }
            currentIndex += 1
            return byte
        }

        switch value {
        case .array(let arr):
            guard currentIndex < arr.count else { throw BinaryDecodingError.outOfBufferBoundary }
            currentVal = arr[currentIndex]
            currentIndex += 1
            if case .arraySchema(let array) = schema {
                currentSchema = array.items
            }
        case .object(let dict):
            guard currentIndex < dict.count * 2 else { throw BinaryDecodingError.outOfBufferBoundary }
            if currentIndex % 2 == 0 {
                let key = sortedKeys[currentIndex / 2]
                currentIndex += 1
                if T.self == String.self {
                    return key as! T
                }
                throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON"))
            } else {
                let key = sortedKeys[currentIndex / 2]
                currentVal = dict[key]!
                currentIndex += 1
                if let vs = valueSchema {
                    currentSchema = vs
                }
            }
        default:
            throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON"))
        }

        let elementDecoder = AvroJSONDecoder(schema: currentSchema, value: currentVal)
        let (finalSchema, finalValue) = try elementDecoder.unwrapped()
        let finalDecoder = AvroJSONDecoder(schema: finalSchema, value: finalValue)
        if let date = try finalDecoder.decodeLogicalDate(schema: finalSchema), let result = date as? T {
            return result
        }
        return try type.init(from: finalDecoder)
    }

    func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON"))
    }

    func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Malformed Avro JSON"))
    }

    func superDecoder() throws -> Decoder { decoder }
}

private struct AvroJSONSingleValueDecodingContainer: SingleValueDecodingContainer, AvroJSONDecodingHelper {
    var codingPath: [CodingKey] = []
    fileprivate var decoder: AvroJSONDecoder
    fileprivate var schema: AvroSchema
    private var value: JSONValue

    init(decoder: AvroJSONDecoder, schema: AvroSchema, value: JSONValue) {
        self.decoder = decoder
        self.schema = schema
        self.value = value
    }

    /// Avro is schema-driven: the JSON value must agree with the schema, not only
    /// with the Swift type the caller asks for. Each decode therefore checks both.
    func decode(_ type: String.Type) throws -> String {
        guard case .string(let v) = value else { throw BinaryDecodingError.typeMismatchWithSchemaString }
        switch schema {
        case .stringSchema:
            return v
        case .enumSchema:
            guard schema.getEnumSymbols().contains(v) else {
                throw DecodingError.dataCorrupted(.init(codingPath: codingPath, debugDescription: "Enum symbol \(v) is not in the schema"))
            }
            return v
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
    }

    func decode(_ type: Bool.Type) throws -> Bool {
        guard case .bool(let v) = value, schema.isBoolean() else { throw BinaryDecodingError.typeMismatchWithSchemaBool }
        return v
    }

    func decode(_ type: Int.Type) throws -> Int {
        guard case .int(let v) = value, schema.isInt() || schema.isLong() else { throw BinaryDecodingError.typeMismatchWithSchemaInt }
        return Int(v)
    }

    func decode(_ type: Int32.Type) throws -> Int32 {
        guard case .int(let v) = value, schema.isInt() else { throw BinaryDecodingError.typeMismatchWithSchemaInt32 }
        guard let narrowed = Int32(exactly: v) else {
            throw DecodingError.dataCorrupted(.init(codingPath: codingPath, debugDescription: "Value \(v) is out of range for an int schema"))
        }
        return narrowed
    }

    func decode(_ type: Int64.Type) throws -> Int64 {
        guard case .int(let v) = value, schema.isLong() || schema.isInt() else { throw BinaryDecodingError.typeMismatchWithSchemaInt64 }
        return v
    }

    func decode(_ type: Float.Type) throws -> Float {
        guard schema.isFloat() else { throw BinaryDecodingError.typeMismatchWithSchemaFloat }
        switch value {
        case .double(let v): return Float(v)
        case .int(let v):    return Float(v)   // JSON writes 1.0 as 1
        default: throw BinaryDecodingError.typeMismatchWithSchemaFloat
        }
    }

    func decode(_ type: Double.Type) throws -> Double {
        guard schema.isDouble() else { throw BinaryDecodingError.typeMismatchWithSchemaDouble }
        switch value {
        case .double(let v): return v
        case .int(let v):    return Double(v)  // JSON writes 1.0 as 1
        default: throw BinaryDecodingError.typeMismatchWithSchemaDouble
        }
    }

    func decodeNil() -> Bool {
        return value == .null
    }

    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        if let primitive = try? decodePrimitive(type, from: value) {
            return primitive
        }

        return try type.init(from: AvroJSONDecoder(schema: schema, value: value))
    }
}

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

    func decodeLogicalDate(schema: AvroSchema) throws -> Date? {
        switch schema {
        case .intSchema(let s) where s.logicalType == .date:
            return LogicalTypeConverter.decodeDate(try primitive.decode() as Int)
        case .intSchema(let s) where s.logicalType == .timeMillis:
            return LogicalTypeConverter.decodeTimeMillis(try primitive.decode() as Int32)
        case .longSchema(let s) where s.logicalType == .timestampMillis:
            return LogicalTypeConverter.decodeTimestampMillis(try primitive.decode() as Int64)
        case .longSchema(let s) where s.logicalType == .timestampMicros:
            return LogicalTypeConverter.decodeTimestampMicros(try primitive.decode() as Int64)
        case .longSchema(let s) where s.logicalType == .timeMicros:
            return LogicalTypeConverter.decodeTimeMicros(try primitive.decode() as Int64)
        default:
            return nil
        }
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
                return LogicalTypeConverter.decodeDate(try primitive.decode() as Int)
            }
            if intSchema.logicalType == .timeMillis {
                return LogicalTypeConverter.decodeTimeMillis(try primitive.decode() as Int32)
            }
            return try primitive.decode() as Int32

        case .longSchema(let longSchema):
            if longSchema.logicalType == .timestampMillis {
                return LogicalTypeConverter.decodeTimestampMillis(try primitive.decode() as Int64)
            }
            if longSchema.logicalType == .timestampMicros {
                return LogicalTypeConverter.decodeTimestampMicros(try primitive.decode() as Int64)
            }
            if longSchema.logicalType == .timeMicros {
                return LogicalTypeConverter.decodeTimeMicros(try primitive.decode() as Int64)
            }
            return try primitive.decode() as Int64

        case .floatSchema:
            return try primitive.decode() as Float

        case .doubleSchema:
            return try primitive.decode() as Double

        case .bytesSchema(let byteSchema):
            if byteSchema.logicalType == .decimal {
                let bytes = try primitive.decode() as [UInt8]
                return LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: byteSchema.scale ?? 0, precision: byteSchema.precision ?? 0)
            }
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
                for _ in 0..<count {
                    if hasBlockSize {
                        let blockSize = try primitive.decode() as Int64
                        guard blockSize > 0 else { return values }
                    }
                    if let v = try decode(schema: arraySchema.items) {
                        values.append(v)
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
                for _ in 0..<count {
                    if hasBlockSize {
                        let blockSize = try primitive.decode() as Int64
                        guard blockSize > 0 else { return pairs }
                    }
                    let key = try primitive.decode() as String
                    pairs[key] = try decode(schema: mapSchema.values)
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
                let bytes = try primitive.decode(fixedSize: fixedSchema.size) as [UInt8]
                return LogicalTypeConverter.decodeDuration(bytes: bytes)
            }
            if fixedSchema.logicalType == .decimal {
                let bytes = try primitive.decode(fixedSize: fixedSchema.size) as [UInt8]
                return LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: fixedSchema.scale ?? 0, precision: fixedSchema.precision ?? 0)
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
        if T.self == Date.self {
            if let date = try decoder.decodeLogicalDate(schema: currentSchema) {
                return date as! T
            }
        }
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
            if let name = schema.getName() {
                schemaMap[name] = schema
            }
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
            // The cast back to T is guaranteed by the upstream `as? AvroDecodable.Type`
            // — `avroDecodable` IS T's metatype, so its initialiser returns a T.
            return try avroDecodable.init(decoder: AvroBinaryDecoder(other: decoder, schema: schema)) as! T
        }
        if T.self == Date.self, let date = try decoder.decodeLogicalDate(schema: schema) {
            return date as! T
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
            return LogicalTypeConverter.decodeDate(try decoder.primitive.decode() as Int).timeIntervalSince1970
        case .intSchema(let intSchema) where intSchema.logicalType == .timeMillis:
            return Double(try decoder.primitive.decode() as Int32)
        case .longSchema(let longSchema) where longSchema.logicalType == .timeMicros:
            return Double(LogicalTypeConverter.decodeTimeMicros(try decoder.primitive.decode() as Int64).timeIntervalSince1970)
        case .longSchema(let longSchema) where longSchema.logicalType == .timestampMillis:
            return LogicalTypeConverter.decodeTimestampMillis(try decoder.primitive.decode() as Int64).timeIntervalSince1970
        case .longSchema(let longSchema) where longSchema.logicalType == .timestampMicros:
            return LogicalTypeConverter.decodeTimestampMicros(try decoder.primitive.decode() as Int64).timeIntervalSince1970
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaDouble
        }
    }

    @inlinable func decode<T: Decodable>(_ type: T.Type) throws -> T {
        if T.self == Date.self, let date = try decoder.decodeLogicalDate(schema: schema) {
            return date as! T
        }
        return try type.init(from: AvroBinaryDecoder(other: decoder, schema: schema))
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
        // Avro maps always emit keys and values in pairs, so the container's
        // count is always even and decode(value) is safe after decode(key).
        while !container.isAtEnd {
            let key = try container.decode(Key.self)
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
            values[k] = try c.decode(type.Value)
        }
        return values
    }

    func decodeIfPresent<MK: Decodable, T: Decodable>(_ type: [MK: T].Type, forKey key: Key) throws -> [MK: T]? {
        guard contains(key) else { throw BinaryDecodingError.malformedAvro }
        return try decodeNil(forKey: key) ? nil : decode(type, forKey: key)
    }
}
