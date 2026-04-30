//
//  swift-avro-core/AvroJsonEncoder.swift
//
//  Created by Yang Liu on 29/09/18.
//  Copyright © 2018 Yang Liu and the project authors.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

import Foundation

// MARK: - AvroJSONEncoder

final class AvroJSONEncoder: Encoder {
    var codingPath: [CodingKey] = []
    var userInfo:   [CodingUserInfoKey: Any] = [:]
    var encodeKey:  Bool = false

    private(set) var schema: AvroSchema
    private(set) var currentMirror: Mirror?

    /// Stack of in-progress JSONValue containers.
    fileprivate var containerStack: ContiguousArray<JSONValue> = []

    init(schema: AvroSchema) {
        self.schema = schema
    }

    init(other: inout AvroJSONEncoder, schema: AvroSchema) {
        self.schema = schema
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(AvroJSONKeyedEncodingContainer<Key>(encoder: self, schema: schema))
    }

    func unkeyedContainer() -> UnkeyedEncodingContainer {
        AvroJSONUnkeyedEncodingContainer(encoder: self, schema: schema)
    }

    func singleValueContainer() -> SingleValueEncodingContainer { self }

    func encode<T: Encodable>(_ value: T) throws {
        switch schema {
        case .bytesSchema:
            // [UInt8] and UInt8 are Encodable, so routing them through
            // unkeyedContainer() recurses back here forever. Short-circuit to
            // the concrete helpers that terminate immediately.
            if let bytes = value as? [UInt8] {
                try encodeBytes(bytes)
            } else if let byte = value as? UInt8 {
                try encode(byte)
            } else {
                var container = unkeyedContainer()
                try container.encode(value)
            }
        case .fixedSchema:
            // Same recursion risk for [UInt8], [UInt32], and UInt8/UInt32.
            if let bytes = value as? [UInt8] {
                try encode(fixedValue: bytes)
            } else if let words = value as? [UInt32] {
                try encode(fixedValue: words)
            } else if let byte = value as? UInt8 {
                try encode(byte)
            } else if let word = value as? UInt32 {
                try encode(word)
            } else {
                var container = unkeyedContainer()
                try container.encode(value)
            }
        case .arraySchema:
            var container = unkeyedContainer()
            try container.encode(value)
        case .recordSchema, .errorSchema:
            currentMirror = Mirror(reflecting: value)
            try value.encode(to: self)
        default:
            try value.encode(to: self)
        }
    }

    /// Converts the accumulated JSONValue to Data using Foundation JSONSerialization at the boundary.
    func getData() throws -> Data {
        guard !containerStack.isEmpty else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        let value = popContainer()
        // JSONSerialization requires top-level object to be NSArray or NSDictionary.
        // Wrap primitive values in an array for serialization.
        let foundationObj = try jsonValueToFoundation(value)
        if foundationObj is String || foundationObj is NSNumber || foundationObj is NSNull {
            // Wrap in array for valid JSON top-level
            return try JSONSerialization.data(withJSONObject: [foundationObj], options: [])
        }
        return try JSONSerialization.data(withJSONObject: foundationObj, options: [])
    }

    /// Converts native JSONValue to Foundation object for JSONSerialization boundary.
    private func jsonValueToFoundation(_ value: JSONValue) throws -> Any {
        switch value {
        case .null:
            return NSNull()
        case .bool(let v):
            return v
        case .int(let v):
            return NSNumber(value: v)
        case .double(let v):
            return NSNumber(value: v)
        case .string(let v):
            return v
        case .array(let arr):
            return try arr.map { try jsonValueToFoundation($0) }
        case .object(let dict):
            var result: [String: Any] = [:]
            for (key, val) in dict {
                result[key] = try jsonValueToFoundation(val)
            }
            return result
        }
    }

    // MARK: Container stack management

    fileprivate func addKeyedContainer() -> [String: JSONValue] {
        let dict: [String: JSONValue] = [:]
        containerStack.append(.object(dict))
        return dict
    }

    fileprivate func addUnkeyedContainer() -> [JSONValue] {
        let array: [JSONValue] = []
        containerStack.append(.array(array))
        return array
    }

    fileprivate func popContainer() -> JSONValue {
        precondition(!containerStack.isEmpty, "popContainer called on an empty stack.")
        return containerStack.removeLast()
    }
}

// MARK: - SingleValueEncodingContainer

extension AvroJSONEncoder: SingleValueEncodingContainer {

    func encodeNil() throws {
        guard schema.isNull() || schema.isUnion() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        containerStack.append(.null)
    }

    func encode(_ value: Bool) throws {
        guard schema.isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.bool(value))
    }

    func encode(_ value: Int) throws {
        guard schema.isLong() || schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: Int8) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: Int16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: Int32) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: Int64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(value))
    }

    func encode(_ value: UInt) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema, .fixedSchema:
            containerStack.append(.string(encodeAvroBytes([value])))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(_ value: UInt16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: UInt32) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: UInt64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt64(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        containerStack.append(.int(Int64(value)))
    }

    func encode(_ value: Float) throws {
        guard schema.isFloat() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.double(Double(value)))
    }

    func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            containerStack.append(.double(value))
        case .intSchema(let param) where param.logicalType == .date:
            // Swift's Date encodes via timeIntervalSinceReferenceDate; add the
            // 1970–2001 offset to align with the Avro epoch (Jan 1 1970).
            let avroDay = Int(value + Date.timeIntervalBetween1970AndReferenceDate)
            containerStack.append(.int(Int64(avroDay)))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(_ value: String) throws {
        switch schema {
        case .stringSchema(_):
            containerStack.append(.string(value))
        case .enumSchema(let attribute):
            guard attribute.symbols.contains(value) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            containerStack.append(.string(value))
        case .unionSchema(let union):
            guard union.branches.contains(where: { if case .stringSchema = $0 { return true }; return false }) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            containerStack.append(.object(["string": .string(value)]))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(fixedValue: [UInt8]) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.string(encodeAvroBytes(fixedValue)))
    }

    func encode(fixedValue: [UInt32]) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        let arr = fixedValue.map { JSONValue.int(Int64($0)) }
        containerStack.append(.array(arr))
    }
}

// MARK: - Bytes encoding (outside SingleValueEncodingContainer)

extension AvroJSONEncoder {
    /// Encodes a raw byte array as an Avro `bytes` field.
    /// Kept outside `SingleValueEncodingContainer` to avoid the "nearly matches
    /// defaulted requirement" warning against the protocol's generic
    /// `encode<T: Encodable>(_ value: T)`.
    func encodeBytes(_ value: [UInt8]) throws {
        guard schema.isBytes() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(.string(encodeAvroBytes(value)))
    }
}

// MARK: - AvroJSONKeyedEncodingContainer

private struct AvroJSONKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K

    var codingPath: [CodingKey] { [] }

    private var encoder:       AvroJSONEncoder
    private var schemaMap:     [String: AvroSchema] = [:]
    private var schema:        AvroSchema
    private var valueChildren: Mirror.Children?
    private var stackIndex:    Int

    init(encoder: AvroJSONEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
        self.stackIndex = encoder.containerStack.count
        _ = encoder.addKeyedContainer()

        if case .recordSchema(let attr) = schema {
            for field in attr.fields {
                schemaMap[field.name] = field.type
            }
        }
        if let mirror = encoder.currentMirror {
            valueChildren = mirror.children
        }
    }

    private var container: [String: JSONValue] {
        get {
            guard stackIndex >= 0, stackIndex < encoder.containerStack.count else { return [:] }
            guard case .object(let dict) = encoder.containerStack[stackIndex] else { return [:] }
            return dict
        }
        set {
            guard stackIndex >= 0, stackIndex < encoder.containerStack.count else { return }
            encoder.containerStack[stackIndex] = .object(newValue)
        }
    }

    private func schema(for key: K) -> AvroSchema {
        schemaMap[key.stringValue] ?? schema
    }

    // MARK: Trailing-nil tracking (mirrors binary encoder logic)

    /// Advances `valueChildren` past any leading nil fields that appear before
    /// `key`, emitting a JSON null entry for each one.
    mutating func encodeNilsBefore(forKey key: K) {
        guard var children = valueChildren else { return }
        let count = children.count
        for _ in 0..<count {
            guard let child = children.popFirst() else { break }
            if child.label == key.stringValue {
                valueChildren = children   // persist consumed state before returning
                return
            }
            if case Optional<Any>.none = child.value {
                encodeNullField(for: child.label)
            }
        }
        valueChildren = children
    }

    /// After encoding `key`, emits JSON null for any remaining fields that are
    /// all nil (i.e. trailing optional fields Swift's Codable never visits).
    mutating func encodeNilsAfter(forKey key: K) {
        guard let children = valueChildren, !children.isEmpty else { return }
        // Only flush if every remaining child is nil.
        guard !children.contains(where: { child in
            if case Optional<Any>.none = child.value { return false }
            return true
        }) else { return }
        children.forEach { encodeNullField(for: $0.label) }
    }

    private mutating func encodeNullField(for label: String?) {
        guard let label,
              let fieldSchema = schemaMap[label],
              fieldSchema.isUnion(),
              fieldSchema.getUnionList().contains(where: { $0.isNull() })
        else { return }
        container[label] = .null
    }

    // MARK: Primitive encode overloads

    mutating func encodeNil(forKey key: K) throws {
        guard schema(for: key).isNull() || schema(for: key).isUnion() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = .null
    }

    mutating func encode(_ value: Bool, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .bool(value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: String, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        switch schema(for: key) {
        case .stringSchema(_):
            container[key.stringValue] = .string(value)
        case .enumSchema(let attribute):
            guard attribute.symbols.contains(value) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container[key.stringValue] = .string(value)
        case .unionSchema(let union):
            guard union.branches.contains(where: { if case .stringSchema = $0 { return true }; return false }) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            // Avro JSON union encoding: {"string": <value>}
            container[key.stringValue] = .object(["string": .string(value)])
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Double, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isDouble() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .double(value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Float, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFloat() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .double(Double(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() || schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int8, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int16, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int32, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int64, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt8, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt16, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt32, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt64, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt64(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        container[key.stringValue] = .int(Int64(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: [UInt8], forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isBytes() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .string(encodeAvroBytes(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(fixedValue: [UInt8], forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = .string(encodeAvroBytes(fixedValue))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(fixedValue: [UInt32], forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        let arr = fixedValue.map { JSONValue.int(Int64($0)) }
        container[key.stringValue] = .array(arr)
        encodeNilsAfter(forKey: key)
    }


    mutating func encode<T: Encodable>(_ value: T, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        let childEncoder = AvroJSONEncoder(other: &encoder, schema: schema(for: key))
        try childEncoder.encode(value)
        guard let encoded = childEncoder.containerStack.last else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = encoded
        encodeNilsAfter(forKey: key)
    }

    // MARK: - Nested containers

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> {
        let childEncoder = AvroJSONEncoder(other: &encoder, schema: schema(for: key))
        let container = childEncoder.container(keyedBy: keyType)
        if let last = childEncoder.containerStack.last {
            self.container[key.stringValue] = last
        }
        return container
    }

    mutating func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer {
        let childEncoder = AvroJSONEncoder(other: &encoder, schema: schema(for: key))
        let container = childEncoder.unkeyedContainer()
        if let last = childEncoder.containerStack.last {
            self.container[key.stringValue] = last
        }
        return container
    }

    mutating func superEncoder() -> Encoder {
        encoder
    }

    mutating func superEncoder(forKey key: K) -> Encoder {
        encoder
    }
}

// MARK: - AvroJSONUnkeyedEncodingContainer

private struct AvroJSONUnkeyedEncodingContainer: UnkeyedEncodingContainer {
    var codingPath: [CodingKey] { [] }
    var count: Int { container.count }

    private var encoder:   AvroJSONEncoder
    private var schema:    AvroSchema
    private var arraySchema: AvroSchema?
    private var stackIndex: Int

    init(encoder: AvroJSONEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
        self.stackIndex = encoder.containerStack.count
        _ = encoder.addUnkeyedContainer()

        if case .arraySchema(let attr) = schema {
            arraySchema = attr.items
        } else {
            arraySchema = nil
        }
    }

    private var container: [JSONValue] {
        get {
            guard stackIndex >= 0, stackIndex < encoder.containerStack.count else { return [] }
            guard case .array(let arr) = encoder.containerStack[stackIndex] else { return [] }
            return arr
        }
        set {
            guard stackIndex >= 0, stackIndex < encoder.containerStack.count else { return }
            encoder.containerStack[stackIndex] = .array(newValue)
        }
    }

    mutating func encodeNil() throws {
        guard schema.isNull() || schema.isUnion() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(.null)
    }

    mutating func encode(_ value: Bool) throws {
        guard schema.isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.bool(value))
    }

    mutating func encode(_ value: String) throws {
        switch schema {
        case .stringSchema(_):
            container.append(.string(value))
        case .enumSchema(let attribute):
            guard attribute.symbols.contains(value) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.append(.string(value))
        case .unionSchema(let union):
            guard union.branches.contains(where: { if case .stringSchema = $0 { return true }; return false }) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.append(.object(["string": .string(value)]))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    mutating func encode(_ value: Double) throws {
        guard schema.isDouble() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.double(value))
    }

    mutating func encode(_ value: Float) throws {
        guard schema.isFloat() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.double(Double(value)))
    }

    mutating func encode(_ value: Int) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: Int8) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: Int16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: Int32) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: Int64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(value))
    }

    mutating func encode(_ value: UInt) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema, .fixedSchema:
            container.append(.string(encodeAvroBytes([value])))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    mutating func encode(_ value: UInt16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: UInt32) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: UInt64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt64(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        container.append(.int(Int64(value)))
    }

    mutating func encode(_ value: [UInt8]) throws {
        guard schema.isBytes() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.append(.string(encodeAvroBytes(value)))
    }

    mutating func encode<T: Encodable>(_ value: T) throws {
        // UInt8 and [UInt8] are Encodable, so letting them fall through to
        // childEncoder.encode() recurses back into unkeyedContainer() forever
        // when the schema is bytes or fixed. Dispatch to the concrete overloads.
        switch schema {
        case .bytesSchema:
            if let bytes = value as? [UInt8] { return try encode(bytes) }
            if let byte  = value as? UInt8   { return try encode(byte)  }
        case .fixedSchema:
            if let bytes = value as? [UInt8]  { container.append(.string(encodeAvroBytes(bytes))); return }
            if let words = value as? [UInt32] { words.forEach { container.append(.int(Int64($0))) }; return }
            if let byte  = value as? UInt8    { return try encode(byte)  }
            if let word  = value as? UInt32   { return try encode(word)  }
        default:
            break
        }
        // For nested encodable (records, arrays of records, etc.)
        let childEncoder = AvroJSONEncoder(other: &encoder, schema: arraySchema ?? schema)
        try childEncoder.encode(value)
        // The encoded value is now on the childEncoder's stack; transfer it
        if let encoded = childEncoder.containerStack.last {
            container.append(encoded)
        }
    }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        let childEncoder = AvroJSONEncoder(other: &encoder, schema: arraySchema ?? schema)
        let container = childEncoder.container(keyedBy: keyType)
        if let last = childEncoder.containerStack.last {
            self.container.append(last)
        }
        return container
    }

    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        let childEncoder = AvroJSONEncoder(other: &encoder, schema: arraySchema ?? schema)
        let container = childEncoder.unkeyedContainer()
        if let last = childEncoder.containerStack.last {
            self.container.append(last)
        }
        return container
    }

    mutating func superEncoder() -> Encoder {
        encoder
    }
}

// MARK: - Helper: Avro byte encoding

func encodeAvroBytes(_ bytes: [UInt8]) -> String {
    Data(bytes).base64EncodedString()
}
