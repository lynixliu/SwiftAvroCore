//
//  swift-avro-core/AvroPrimitiveJsonEncoder.swift
//
//  Created by Yang Liu on 29/09/18.
//  Copyright © 2018 Yang Liu and the project authors.
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

// MARK: - AvroJSONEncoder

final class AvroJSONEncoder: Encoder {
    var codingPath: [CodingKey] = []
    var userInfo:   [CodingUserInfoKey: Any] = [:]
    var encodeKey:  Bool = false

    private(set) var schema: AvroSchema
    private(set) var currentMirror: Mirror?

    /// Stack of in-progress NSMutableDictionary / NSMutableArray containers.
    private var containerStack: [NSObject] = []

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
        case .bytesSchema, .fixedSchema, .arraySchema:
            var container = unkeyedContainer()
            try container.encode(value)
        case .recordSchema, .errorSchema:
            currentMirror = Mirror(reflecting: value)
            try value.encode(to: self)
        default:
            try value.encode(to: self)
        }
    }

    func getData() throws -> Data {
        try JSONSerialization.data(withJSONObject: popContainer(), options: [])
    }

    // MARK: Container stack management

    fileprivate func addKeyedContainer() -> NSMutableDictionary {
        let dict = NSMutableDictionary()
        containerStack.append(dict)
        return dict
    }

    fileprivate func addUnkeyedContainer() -> NSMutableArray {
        let array = NSMutableArray()
        containerStack.append(array)
        return array
    }

    fileprivate func popContainer() -> NSObject {
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
        containerStack.append(NSNull())
    }

    func encode(_ value: Bool) throws {
        guard schema.isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Int) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Int8) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Int16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Int32) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Int64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: UInt) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema, .fixedSchema:
            containerStack.append(NSNumber(value: value))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(_ value: UInt16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: UInt32) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: UInt64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt64(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Float) throws {
        guard schema.isFloat() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSNumber(value: value))
    }

    func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            containerStack.append(NSNumber(value: value))
        case .intSchema(let param) where param.logicalType == .date:
            // Swift's Date encodes via timeIntervalSinceReferenceDate; add the
            // 1970–2001 offset to align with the Avro epoch (Jan 1 1970).
            let avroDay = Int(value + Date.timeIntervalBetween1970AndReferenceDate)
            containerStack.append(NSNumber(value: avroDay))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(_ value: String) throws {
        switch schema {
        case .stringSchema:
            containerStack.append(NSString(string: value))
        case .enumSchema(let attribute):
            guard attribute.symbols.contains(value) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            containerStack.append(NSString(string: value))
        case .unionSchema(let union):
            guard union.branches.contains(.stringSchema) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            // Avro JSON union encoding: {"string": <value>}
            containerStack.append(NSDictionary(object: NSString(string: value),
                                               forKey: NSString(string: "string")))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(fixedValue: [UInt8]) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        containerStack.append(NSString(string: encodeAvroBytes(fixedValue)))
    }

    func encode(fixedValue: [UInt32]) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        fixedValue.forEach { containerStack.append(NSNumber(value: $0)) }
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
        containerStack.append(NSString(string: encodeAvroBytes(value)))
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

    /// The live dictionary this container writes into.
    var container: NSMutableDictionary

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

    private func encodeNullField(for label: String?) {
        guard let label,
              let fieldSchema = schemaMap[label],
              fieldSchema.isUnion(),
              fieldSchema.getUnionList().contains(where: { $0.isNull() })
        else { return }
        container[label] = NSNull()
    }

    // MARK: Primitive encode overloads

    mutating func encodeNil(forKey key: K) throws {
        guard schema(for: key).isNull() || schema(for: key).isUnion() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNull()
    }

    mutating func encode(_ value: Bool, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: String, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        switch schema(for: key) {
        case .stringSchema:
            container[key.stringValue] = NSString(string: value)
        case .enumSchema(let attribute):
            guard attribute.symbols.contains(value) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container[key.stringValue] = NSString(string: value)
        case .unionSchema(let union):
            guard union.branches.contains(.stringSchema) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            // Avro JSON union encoding: {"string": <value>}
            container[key.stringValue] = NSDictionary(object: NSString(string: value),
                                                      forKey: NSString(string: "string"))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Double, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isDouble() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Float, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFloat() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int8, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int16, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int32, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: Int64, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt8, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt16, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt32, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: UInt64, forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        guard value <= UInt64(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        container[key.stringValue] = NSNumber(value: value)
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(_ value: [UInt8], forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isBytes() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSString(string: encodeAvroBytes(value))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(fixedValue: [UInt8], forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container[key.stringValue] = NSString(string: encodeAvroBytes(fixedValue))
        encodeNilsAfter(forKey: key)
    }

    mutating func encode(fixedValue: [UInt32], forKey key: K) throws {
        encodeNilsBefore(forKey: key)
        guard schema(for: key).isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        let arr = NSMutableArray()
        fixedValue.forEach { arr.add(NSNumber(value: $0)) }
        container[key.stringValue] = arr
        encodeNilsAfter(forKey: key)
    }

    mutating func encode<T: Encodable>(_ value: T, forKey key: K) throws {
        encodeNilsBefore(forKey: key)

        if case .mapSchema(let map) = schema {
            schemaMap[key.stringValue] = map.values
        }

        let curSchema = schema(for: key)
        switch curSchema {
        case .nullSchema:
            container[key.stringValue] = NSNull()

        case .booleanSchema: try encode(value as! Bool,    forKey: key)
        case .intSchema:     try encode(value as! Int32,   forKey: key)
        case .longSchema:    try encode(value as! Int64,   forKey: key)
        case .floatSchema:   try encode(value as! Float,   forKey: key)
        case .doubleSchema:  try encode(value as! Double,  forKey: key)
        case .stringSchema:  try encode(value as! String,  forKey: key)
        case .enumSchema:    try encode(value as! String,  forKey: key)
        case .bytesSchema:   try encode(value as! [UInt8], forKey: key)
        case .fixedSchema:   try encode(fixedValue: value as! [UInt8], forKey: key)

        case .unionSchema(let union):
            if let opt = value as? any _OptionalProtocol, opt.isNil {
                container[key.stringValue] = NSNull()
            } else if let nonNull = union.branches.first(where: { !$0.isNull() }) {
                let inner = AvroJSONEncoder(other: &encoder, schema: nonNull)
                try inner.encode(value)
                let wrapper = NSMutableDictionary()
                wrapper[nonNull.getTypeName()] = inner.popContainer()
                container[key.stringValue] = wrapper
            }

        case .mapSchema(let map):
            let inner = AvroJSONEncoder(other: &encoder, schema: map.values)
            inner.encodeKey = true
            try value.encode(to: inner)
            container[key.stringValue] = inner.popContainer()

        case .arraySchema(let array):
            let inner = AvroJSONEncoder(other: &encoder, schema: array.items)
            try value.encode(to: inner)
            container[key.stringValue] = inner.popContainer()

        default:
            let inner = AvroJSONEncoder(other: &encoder, schema: curSchema)
            try value.encode(to: inner)
            container[key.stringValue] = inner.popContainer()
        }

        encodeNilsAfter(forKey: key)
    }

    // MARK: Nested containers

    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: K
    ) -> KeyedEncodingContainer<NestedKey> {
        let nested = AvroJSONKeyedEncodingContainer<NestedKey>(encoder: encoder, schema: schema(for: key))
        container[key.stringValue] = nested.container
        return KeyedEncodingContainer(nested)
    }

    mutating func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer {
        let nested = AvroJSONUnkeyedEncodingContainer(encoder: encoder, schema: schema(for: key))
        container[key.stringValue] = nested.container
        return nested
    }

    mutating func superEncoder() -> Encoder { encoder }
    mutating func superEncoder(forKey key: K) -> Encoder { encoder }

    // MARK: Init

    fileprivate init(encoder: AvroJSONEncoder, schema: AvroSchema) {
        self.encoder       = encoder
        self.schema        = schema
        self.container     = encoder.addKeyedContainer()
        self.valueChildren = encoder.currentMirror?.children
        if case .recordSchema(let record) = schema {
            record.fields.forEach { schemaMap[$0.name] = $0.type }
        }
    }
}

// MARK: - AvroJSONUnkeyedEncodingContainer

private struct AvroJSONUnkeyedEncodingContainer: UnkeyedEncodingContainer {
    var codingPath: [CodingKey] { [] }
    var count: Int = 0

    private var encoder: AvroJSONEncoder
    private var schema:  AvroSchema
    /// The live array this container writes into.
    fileprivate var container: NSMutableArray

    mutating func encode<T: Encodable>(_ value: T) throws {
        defer { count += 1 }
        switch schema {
        case .nullSchema:    try encodeNil()
        case .booleanSchema: try encode(value as! Bool)
        case .intSchema:     try encode(value as! Int32)
        case .longSchema:    try encode(value as! Int64)
        case .floatSchema:   try encode(value as! Float)
        case .doubleSchema:  try encode(value as! Double)
        case .stringSchema:  try encode(value as! String)
        case .bytesSchema:   try encodeBytes(value as! [UInt8])

        case .fixedSchema(let fixed):
            if fixed.logicalType == .duration {
                try encode(fixedValue: value as! [UInt32])
            } else {
                try encode(fixedValue: value as! [UInt8])
            }

        case .arraySchema(let array):
            let inner = AvroJSONEncoder(other: &encoder, schema: array.items)
            try value.encode(to: inner)
            container.add(inner.popContainer())

        case .mapSchema(let map):
            let inner = AvroJSONEncoder(other: &encoder, schema: map.values)
            inner.encodeKey = true
            try value.encode(to: inner)
            container.add(inner.popContainer())

        default:
            let inner = AvroJSONEncoder(other: &encoder, schema: schema)
            try inner.encode(value)
            container.add(inner.popContainer())
        }
    }

    // MARK: Primitive encode overloads

    func encodeNil() throws {
        guard schema.isNull() || schema.isUnion() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNull())
    }

    func encode(_ value: Bool) throws {
        guard schema.isBoolean() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Int) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Int8) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Int16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Int32) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Int64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: UInt) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema, .fixedSchema:
            container.add(NSNumber(value: value))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(_ value: UInt16) throws {
        guard schema.isInt() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: UInt32) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: UInt64) throws {
        guard schema.isLong() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Float) throws {
        guard schema.isFloat() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSNumber(value: value))
    }

    func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            container.add(NSNumber(value: value))
        case .intSchema(let param) where param.logicalType == .date:
            let avroDay = Int(value + Date.timeIntervalBetween1970AndReferenceDate)
            container.add(NSNumber(value: avroDay))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encode(_ value: String) throws {
        switch schema {
        case .stringSchema:
            container.add(NSString(string: value))
        case .enumSchema(let attribute):
            guard attribute.symbols.contains(value) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.add(NSString(string: value))
        case .unionSchema(let union):
            guard union.branches.contains(.stringSchema) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.add(NSDictionary(object: NSString(string: value),
                                       forKey: NSString(string: "string")))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    func encodeBytes(_ value: [UInt8]) throws {
        guard schema.isBytes() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSString(string: encodeAvroBytes(value)))
    }

    func encode(fixedValue: [UInt8]) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        container.add(NSString(string: encodeAvroBytes(fixedValue)))
    }

    func encode(fixedValue: [UInt32]) throws {
        guard schema.isFixed() else { throw BinaryEncodingError.typeMismatchWithSchema }
        fixedValue.forEach { container.add(NSNumber(value: $0)) }
    }

    // MARK: Nested containers

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type
    ) -> KeyedEncodingContainer<NestedKey> {
        KeyedEncodingContainer(AvroJSONKeyedEncodingContainer<NestedKey>(encoder: encoder, schema: schema))
    }

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        schema: AvroSchema
    ) -> KeyedEncodingContainer<NestedKey> {
        let nested = AvroJSONKeyedEncodingContainer<NestedKey>(encoder: encoder, schema: schema)
        container.add(nested.container)
        return KeyedEncodingContainer(nested)
    }

    func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        AvroJSONUnkeyedEncodingContainer(encoder: encoder, schema: schema)
    }

    func nestedUnkeyedContainer(schema: AvroSchema) -> UnkeyedEncodingContainer {
        let nested = AvroJSONUnkeyedEncodingContainer(encoder: encoder, schema: schema)
        container.add(nested.container)
        return nested
    }

    func superEncoder() -> Encoder { encoder }

    init(encoder: AvroJSONEncoder, schema: AvroSchema) {
        self.encoder   = encoder
        self.schema    = schema
        self.container = encoder.addUnkeyedContainer()
    }
}

// MARK: - Avro bytes/fixed JSON encoding helper

/// Avro JSON encoding for `bytes` and `fixed` types.
///
/// The Avro spec requires each byte to be the corresponding Unicode codepoint
/// U+0000–U+00FF (ISO-8859-1 / Latin-1). `String(bytes:encoding:.utf8)` fails
/// for values > 0x7F, so we map through Unicode scalars instead.
private func encodeAvroBytes(_ bytes: [UInt8]) -> String {
    String(bytes.map { Character(Unicode.Scalar($0)) })
}

// MARK: - Optional detection helper

/// Existential wrapper used to test optionality without knowing the wrapped type.
private protocol _OptionalProtocol {
    var isNil: Bool { get }
}
extension Optional: _OptionalProtocol {
    var isNil: Bool { self == nil }
}
