//
//  swift-avro-core/AvroPrimitiveJsonEncoder.swift
//
//  Created by Yang Liu on 29/09/18.
//  Copyright © 2018 ___ORGANIZATIONNAME___ and the project authors.
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
final class AvroJSONEncoder: Encoder {
    var container: [NSObject] = []
    
    public var codingPath: [CodingKey] = []
    
    public var userInfo: [CodingUserInfoKey : Any] = [CodingUserInfoKey : Any]()
    
    public var encodeKey: Bool = false
    
    var schema: AvroSchema
    
    init(schema: AvroSchema) {
        self.schema = schema
    }
    
    init(other: inout AvroJSONEncoder, schema: AvroSchema) {
        self.schema = schema
    }
    
    public func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        return KeyedEncodingContainer(AvroJSONKeyedEncodingContainer<Key>(encoder: self, schema: schema))
    }
    
    public func unkeyedContainer() -> UnkeyedEncodingContainer {
        return AvroJSONUnkeyedEncodingContainer(encoder: self, schema: schema)
    }
    
    public func singleValueContainer() -> SingleValueEncodingContainer {
        return self
    }
    
    public func encode<T : Encodable>(_ value: T) throws {
        switch schema {
        case .bytesSchema, .fixedSchema, .arraySchema:
            var container = unkeyedContainer()
            try container.encode(value)
        default:
            try value.encode(to: self)
        }
    }
    public func getData() throws -> Data {
        let topLevel = popContainer()
        return try JSONSerialization.data(withJSONObject: topLevel, options: [])
    }
    
    fileprivate func popContainer() -> NSObject {
        precondition(!self.container.isEmpty, "Empty container stack.")
        return self.container.removeLast()
    }
}

internal struct AvroJSONKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K
    
    var codingPath: [CodingKey] {
        return []
    }
    
    var allKeys: [K] = []
    var schemaMap: [String: AvroSchema] = [:]
    
    func schema(_ key: K) ->AvroSchema {
        if let sch = schemaMap[key.stringValue] {
            return sch
        }
        return schema
    }
    
    var encoder: AvroJSONEncoder
    /// A reference to the container we're writing to.
    var container: NSMutableDictionary
    
    fileprivate var schema: AvroSchema
    init(encoder: AvroJSONEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.container = self.encoder.addKeyedContainer()
        self.schema = schema
        
        switch schema {
        case .recordSchema(let record):
            for field in record.fields {
                self.schemaMap[field.name] = field.type
            }
            
        default:
            return
        }
    }
    
    mutating func encodeNil(forKey key: K) throws {
        guard self.schema(key).isNull() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSString(string: "null")
    }
    
    mutating func encode(_ value: Bool, forKey key: K) throws {
        guard self.schema(key).isBoolean() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: String, forKey key: K) throws {
        guard self.schema(key).isString() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSString(string: value)
    }
    
    mutating func encode(_ value: Double, forKey key: K) throws {
        guard self.schema(key).isDouble() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: Float, forKey key: K) throws {
        guard self.schema(key).isFloat() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: Int, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: Int8, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: Int16, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: Int32, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: Int64, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: UInt, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: UInt8, forKey key: K) throws {
        guard self.schema(key).isFixed() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: UInt16, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: UInt32, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: UInt64, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSNumber(value: value)
    }
    
    mutating func encode(_ value: [UInt8], forKey key: K) throws {
        guard self.schema(key).isBytes() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = String(bytes: value, encoding: .utf8)
    }
    
    mutating func encode(fixedValue: [UInt8], forKey key: K) throws {
        guard self.schema(key).isFixed() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = String(bytes: fixedValue, encoding: .utf8)
    }
    
    mutating func encode(fixedValue: [UInt32], forKey key: K) throws {
        guard self.schema(key).isFixed() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container[key.stringValue] = NSString(bytes: fixedValue, length:
            fixedValue.count, encoding: String.Encoding.utf8.rawValue)!
    }
    
    mutating func encode<T: Encodable>(_ value: T, forKey key: K) throws {
        let curSchema = schema(key)
        if case .mapSchema(let map) = schema {
            self.schemaMap[key.stringValue] = map.values
        }
        switch curSchema {
        case .nullSchema: try encodeNil(forKey: key)
        case .booleanSchema: try encode(value as! Bool, forKey: key)
        case .intSchema: try encode(value as! Int32, forKey: key)
        case .longSchema: try encode(value as! Int64, forKey: key)
        case .floatSchema: try encode(value as! Float, forKey: key)
        case .doubleSchema: try encode(value as! Double, forKey: key)
        case .stringSchema: try encode(value as! String, forKey: key)
        case .enumSchema: try encode(value as! String, forKey: key)
        case .bytesSchema: try encode(value as! [UInt8], forKey: key)
        case .fixedSchema: try encode(fixedValue: value as! [UInt8], forKey: key)
        case .mapSchema(let map):
            let e = AvroJSONEncoder(other: &encoder ,schema: map.values)
            e.encodeKey = true
            try value.encode(to: e)
            container[key.stringValue] = e.popContainer()
        case .arraySchema(let array):
            let d = AvroJSONEncoder(other: &encoder ,schema: array.items)
            try value.encode(to: d)
            container[key.stringValue] = d.popContainer()
        default:
            let d = AvroJSONEncoder(other: &encoder ,schema: curSchema)
            try value.encode(to: d)
            container[key.stringValue] = d.popContainer()
        }
    }
    
    mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
        let keyedContainer =
            AvroJSONKeyedEncodingContainer<NestedKey>(
                encoder: encoder, schema: schema(key))
        container[key.stringValue] = keyedContainer.container
        return KeyedEncodingContainer(keyedContainer)
    }
    
    mutating func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer {
        let unkeyedContainer = AvroJSONUnkeyedEncodingContainer(encoder: encoder, schema: schema(key))
        container[key.stringValue] = unkeyedContainer.container
        return unkeyedContainer
    }
    
    mutating func superEncoder() -> Encoder {
        return encoder
    }
    
    mutating func superEncoder(forKey key: K) -> Encoder {
        return encoder
    }
}

internal struct AvroJSONUnkeyedEncodingContainer: UnkeyedEncodingContainer {
    var codingPath: [CodingKey] {
        return []
    }
    var encoder: AvroJSONEncoder
    var schema: AvroSchema
    var container: NSMutableArray
    
    init(encoder: AvroJSONEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
        self.count = 0
        self.container = self.encoder.addUnkeyedContainer()
    }
    mutating func encode<T>(_ value: T) throws where T : Encodable {
        /// encode nested types
        switch schema {
        case .nullSchema: try encodeNil()
        case .booleanSchema: try encode(value as! Bool)
        case .intSchema: try encode(value as! Int32)
        case .longSchema: try encode(value as! Int64)
        case .floatSchema: try encode(value as! Float)
        case .doubleSchema: try encode(value as! Double)
        case .stringSchema: try encode(value as! String)
        case .bytesSchema: try encode(value as! [UInt8])
        case .fixedSchema(let fixedSch):
            if let logicalType = fixedSch.logicalType, logicalType == .duration {
                try encode(fixedValue: value as! [UInt32])
            } else {
                try encode(fixedValue: value as! [UInt8])
            }
        case .arraySchema(let array):
            let d = AvroJSONEncoder(other: &encoder, schema: array.items)
            try value.encode(to: d)
            container.add(d.popContainer())
        case .mapSchema(let map):
            let e = AvroJSONEncoder(other: &encoder ,schema: map.values)
            e.encodeKey = true
            try value.encode(to: e)
            container.add(e.popContainer())
        default:
            let d = AvroJSONEncoder(other: &encoder ,schema: schema)
            try d.encode(value)
            
        }
        count += 1
    }
    var count: Int
    func encodeNil() throws {
        if !schema.isNull() || !schema.isUnion() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSString(string: "null"))
    }
    func encode(_ value: Bool) throws {
        if !schema.isBoolean() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: Int) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: Int8) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: Int16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    
    func encode(_ value: Int32) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    
    func encode(_ value: Int64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: UInt) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema:
            container.add(NSNumber(value: value))
        case .fixedSchema:
            container.add(NSNumber(value: value))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    func encode(_ value: UInt16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: UInt32) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: UInt64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    func encode(_ value: Float) throws {
        if !schema.isFloat() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSNumber(value: value))
    }
    
    func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            container.add(NSNumber(value: value))
        case .intSchema(let param):
            if let logicalType = param.logicalType {
                switch logicalType {
                case .date:
                    /// Date is Codable in Swift and encode(to:) -> Double in singleValueContainer
                    /// the date is from timeIntervalSinceReferenceDate,
                    /// so it need to be added an offset to Jan 1 1970 according to Avro spec
                    let date = Int(value + Date.timeIntervalBetween1970AndReferenceDate)
                    container.add(NSNumber(value: date))
                default:
                    throw BinaryEncodingError.typeMismatchWithSchema
                }
                return
            }
            throw BinaryEncodingError.typeMismatchWithSchema
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    
    func encode(_ value: String) throws {
        switch schema {
        case .stringSchema:
            container.add(NSString(string: value))
        case .enumSchema(let attribute):
            guard attribute.symbols.firstIndex(of: value) != nil else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.add(NSString(string: value))
        case .unionSchema(let union):
            guard union.branches.firstIndex(of: .stringSchema) != nil else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.add(NSString(string: "\"string\":\(value)"))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    
    func encode(_ value: [UInt8]) throws {
        if !schema.isBytes() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSString(bytes: value, length:
            value.count, encoding: String.Encoding.utf8.rawValue)!)
    }
    
    func encode(fixedValue: [UInt8]) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.add(NSString(bytes: fixedValue, length:
            fixedValue.count, encoding: String.Encoding.utf8.rawValue)!)
    }
    
    func encode(fixedValue: [UInt32]) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        for value in fixedValue {
            container.add(NSNumber(value: value))
        }
    }
    func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
        return KeyedEncodingContainer(AvroJSONKeyedEncodingContainer<NestedKey>(
            encoder: encoder, schema: schema))
    }
    
    func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        return AvroJSONUnkeyedEncodingContainer(encoder: encoder, schema: schema)
    }
    func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, schema: AvroSchema) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
        let keyedContainer = AvroJSONKeyedEncodingContainer<NestedKey>(
            encoder: encoder, schema: schema)
        container.add(keyedContainer.container)
        return KeyedEncodingContainer(keyedContainer)
    }
    
    func nestedUnkeyedContainer(schema: AvroSchema) -> UnkeyedEncodingContainer {
        let unkeyedContainer = AvroJSONUnkeyedEncodingContainer(encoder: encoder, schema: schema)
        container.add(unkeyedContainer.container)
        return unkeyedContainer
    }
    
    func superEncoder() -> Encoder {
        return encoder
    }
}

// MARK: - AvroPrimitiveJSONEncoder
/*
 * Encoder for convert Avro  to binary format
 */
extension AvroJSONEncoder: SingleValueEncodingContainer {
    
    //private var container: [NSObject] = []
        
    func addKeyedContainer() -> NSMutableDictionary {
            let dictionary = NSMutableDictionary()
            self.container.append(dictionary)
            return dictionary
        }
    func addUnkeyedContainer() -> NSMutableArray {
            let array = NSMutableArray()
            self.container.append(array)
            return array
        }
    
    func encodeNil() throws {
        if !schema.isNull() || !schema.isUnion() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSString(string: "null"))
    }
    func encode(_ value: Bool) throws {
        if !schema.isBoolean() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: Int) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: Int8) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: Int16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    
    func encode(_ value: Int32) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    
    func encode(_ value: Int64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: UInt) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema:
            container.append(NSNumber(value: value))
        case .fixedSchema:
            container.append(NSNumber(value: value))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    func encode(_ value: UInt16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: UInt32) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: UInt64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    func encode(_ value: Float) throws {
        if !schema.isFloat() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSNumber(value: value))
    }
    
    func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            container.append(NSNumber(value: value))
        case .intSchema(let param):
            if let logicalType = param.logicalType {
                switch logicalType {
                case .date:
                    /// Date is Codable in Swift and encode(to:) -> Double in singleValueContainer
                    /// the date is from timeIntervalSinceReferenceDate,
                    /// so it need to be added an offset to Jan 1 1970 according to Avro spec
                    let date = Int(value + Date.timeIntervalBetween1970AndReferenceDate)
                    container.append(NSNumber(value: date))
                default:
                    throw BinaryEncodingError.typeMismatchWithSchema
                }
                return
            }
            throw BinaryEncodingError.typeMismatchWithSchema
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    
    func encode(_ value: String) throws {
        switch schema {
        case .stringSchema:
            container.append(NSString(string: value))
        case .enumSchema(let attribute):
            guard attribute.symbols.firstIndex(of: value) != nil else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.append(NSString(string: value))
        case .unionSchema(let union):
            guard union.branches.firstIndex(of: .stringSchema) != nil else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            container.append(NSString(string: "\"string\":\(value)"))
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    
    func encode(_ value: [UInt8]) throws {
        if !schema.isBytes() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSString(bytes: value, length:
            value.count, encoding: String.Encoding.utf8.rawValue)!)
    }
    
    func encode(fixedValue: [UInt8]) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        container.append(NSString(bytes: fixedValue, length:
            fixedValue.count, encoding: String.Encoding.utf8.rawValue)!)
    }
    
    func encode(fixedValue: [UInt32]) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        for value in fixedValue {
            container.append(NSNumber(value: value))
        }
    }
}
