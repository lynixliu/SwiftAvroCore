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
public final class AvroEncoder {
    //private var encoder: Encoder? = nil
    public var userInfo: [CodingUserInfoKey : Any] = [CodingUserInfoKey : Any]()
    fileprivate let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!
    
    init() {
        userInfo[infoKey] = AvroEncodingOption.AvroBinary
    }
    func setUserInfo(userInfo: [CodingUserInfoKey : Any]) {
        self.userInfo = userInfo
    }
    func encode<T: Encodable>(_ type: T, schema: AvroSchema) throws -> Data {
        let encodingOption = userInfo[infoKey] as! AvroEncodingOption
        switch encodingOption {
        case .AvroBinary:
            let encoder = AvroBinaryEncoder(schema: schema)
            try encoder.encode(type)
            return encoder.getData()
        case .AvroJson:
            let encoder = AvroJSONEncoder(schema: schema)
            try encoder.encode(type)
            return try encoder.getData()
        }
    }
    
    func sizeOf<T: Encodable>(_ type: T, schema: AvroSchema) throws -> Int {
        let encoder = AvroBinaryEncoder(schema: schema, primitiveEncoder: AvroPrimitiveSizer())
        try encoder.encode(type)
        return encoder.getSize()
    }
}

/*
 * Encoder for convert Avro types to binary format by checking the schema
 */
fileprivate final class AvroBinaryEncoder: Encoder {
    public var codingPath: [CodingKey] = []
    
    public var userInfo: [CodingUserInfoKey : Any] = [CodingUserInfoKey : Any]()
    var unkeyedContainerCache: AvroUnkeyedEncodingContainer? = nil
    public var encodeKey: Bool = false
    public func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        return KeyedEncodingContainer(AvroKeyedEncodingContainer<Key>(encoder: self, schema: schema))
    }
    
    public func unkeyedContainer() -> UnkeyedEncodingContainer {
        if let container = unkeyedContainerCache {
            return container
        }
        unkeyedContainerCache = AvroUnkeyedEncodingContainer(encoder: self, schema: schema)
        return unkeyedContainerCache!
    }
    
    public func singleValueContainer() -> SingleValueEncodingContainer {
        return AvroSingleEncodingContainer(encoder: self, schema: schema)
    }
    public func encode<T : Encodable>(_ value: T) throws {
        switch schema {
        case .bytesSchema:
            primitive.encode(value as! [UInt8])
        case .fixedSchema(let param):
            if let logic = param.logicalType, logic == .duration {
                primitive.encode(fixed: value as! [UInt32])
                return
            }
            primitive.encode(fixed: value as! [UInt8])
        case .arraySchema:
            var container = unkeyedContainer()
            try container.encode(value)
        case .mapSchema:
            let mirror = Mirror(reflecting: value)
            primitive.encode(mirror.children.count)
            encodeKey = true
            try value.encode(to: self)
            primitive.encode(UInt8(0))
            encodeKey = false
        default:
            try value.encode(to: self)
        }
    }
   
    internal var primitive: AvroPrimitiveEncodeProtocol
    
    var schema: AvroSchema
    
    init(schema: AvroSchema) {
        self.schema = schema
        self.primitive = AvroPrimitiveEncoder()
    }
    
    init(other: inout AvroBinaryEncoder, schema: AvroSchema) {
        self.schema = schema
        self.primitive = other.primitive
    }
    
    init(schema: AvroSchema, primitiveEncoder: AvroPrimitiveEncodeProtocol) {
        self.schema = schema
        self.primitive = primitiveEncoder
    }
    func getData() -> Data {
        return Data(primitive.buffer)
    }
    func getSize() -> Int {
        return primitive.size
    }
}
fileprivate struct  AvroKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
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
    
    var encoder: AvroBinaryEncoder
    
    mutating func encodeNil(forKey key: K) throws {
        guard self.schema(key).isNull() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encodeNull()
    }
    
    mutating func encode(_ value: Bool, forKey key: K) throws {
        guard self.schema(key).isBoolean() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: String, forKey key: K) throws {
        guard self.schema(key).isString() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Double, forKey key: K) throws {
        guard self.schema(key).isDouble() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Float, forKey key: K) throws {
        guard self.schema(key).isFloat() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int8, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int16, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int32, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int64, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: UInt, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: UInt8, forKey key: K) throws {
        guard self.schema(key).isFixed() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: UInt16, forKey key: K) throws {
        guard self.schema(key).isInt() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: UInt32, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: UInt64, forKey key: K) throws {
        guard self.schema(key).isLong() else {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode<T>(_ value: T, forKey key: K) throws where T : Encodable {
        if case .mapSchema(let map) = schema {
            self.schemaMap[key.stringValue] = map.values
        }
        if encoder.encodeKey {
            encoder.primitive.encode(key.stringValue)
        }
        var container = nestedUnkeyedContainer(forKey: key)
        try container.encode(value)
    }
   
    mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
        return KeyedEncodingContainer(AvroKeyedEncodingContainer<NestedKey>(
            encoder: encoder, schema: schema))
    }
    
    mutating func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer {
        let keySchema = schema(key)
        let keyEncoder = AvroBinaryEncoder.init(other: &encoder, schema: keySchema)
        return AvroUnkeyedEncodingContainer(encoder: keyEncoder, schema: keySchema)
    }
    
    mutating func superEncoder() -> Encoder {
        return encoder
    }
    
    mutating func superEncoder(forKey key: K) -> Encoder {
        return encoder
    }

    fileprivate var schema: AvroSchema
    init(encoder: AvroBinaryEncoder, schema: AvroSchema) {
        self.encoder = encoder
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
}

fileprivate struct AvroUnkeyedEncodingContainer: UnkeyedEncodingContainer, EncodingHelper {
    mutating func encode<T>(_ value: T) throws where T : Encodable {
        /// encode nested types
        switch schema {
        case .bytesSchema:
            encoder.primitive.encode(value as! [UInt8])
        case .fixedSchema(let fixedSch):
            if let logicalType = fixedSch.logicalType, logicalType == .duration {
                encoder.primitive.encode(fixed: value as! [UInt32])
            } else {
                encoder.primitive.encode(fixed: value as! [UInt8])
            }
        case .arraySchema(let array):
            let mirror = Mirror(reflecting: value)
            encoder.primitive.encode(mirror.children.count)
            let d = AvroBinaryEncoder(other: &encoder ,schema: array.items)
            try value.encode(to: d)
            encoder.primitive.encode(UInt8(0))
        case .mapSchema(let map):
            let mirror = Mirror(reflecting: value)
            encoder.primitive.encode(mirror.children.count)
            let e = AvroBinaryEncoder(other: &encoder ,schema: map.values)
            e.encodeKey = true
            try value.encode(to: e)
            encoder.primitive.encode(UInt8(0))
        default:
            try encoder.encode(value)
            
        }
        count += 1
    }
    var count: Int
    
    func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
        return KeyedEncodingContainer(AvroKeyedEncodingContainer<NestedKey>(
            encoder: encoder, schema: schema))
    }
    
    func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        return AvroUnkeyedEncodingContainer(encoder: encoder, schema: schema)
    }
    
    func superEncoder() -> Encoder {
        return encoder
    }
    
    var codingPath: [CodingKey] {
        return []
    }
    var encoder: AvroBinaryEncoder
    var schema: AvroSchema
    var identifier: Int
    private static var identifierFactory = 0
    
    private static func getUniqueIdentifier() -> Int {
        identifierFactory += 1
        return identifierFactory
    }
    init(encoder: AvroBinaryEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
        self.count = 0
        self.identifier = AvroUnkeyedEncodingContainer.getUniqueIdentifier()
    }
}

fileprivate struct AvroSingleEncodingContainer: SingleValueEncodingContainer, EncodingHelper {
    var codingPath: [CodingKey] {
        return []
    }
    var encoder: AvroBinaryEncoder
    var schema: AvroSchema
    
    init(encoder: AvroBinaryEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
    }
    mutating func encode<T>(_ value: T) throws where T : Encodable {
        try value.encode(to: encoder)
    }
}
fileprivate protocol EncodingHelper {
    var codingPath: [CodingKey] { get }
    var encoder: AvroBinaryEncoder { get set}
    var schema: AvroSchema {get}
}

extension EncodingHelper {
    
    mutating func encodeNil() throws {
        switch schema {
        case .nullSchema:
            encoder.primitive.encodeNull()
        case .unionSchema(let union):
            guard let id = union.branches.firstIndex(of: .nullSchema) else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
            encoder.primitive.encode(id)
            encoder.primitive.encodeNull()
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    mutating func encode(_ value: Bool) throws {
        if !schema.isBoolean() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Int) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Int8) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Int16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int32) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt8) throws {
        switch schema {
        case .bytesSchema:
            encoder.primitive.encode(value)
        case .fixedSchema:
            encoder.primitive.encode(value)
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }
    mutating func encode(_ value: UInt16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt32) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Float) throws {
        if !schema.isFloat() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Double) throws {
        switch schema {
        case .doubleSchema:
            encoder.primitive.encode(value)
        case .intSchema(let param):
            if let logicalType = param.logicalType {
                switch logicalType {
                case .date:
                    /// Date is Codable in Swift and encode(to:) -> Double in singleValueContainer
                    /// the date is from timeIntervalSinceReferenceDate,
                    /// so it need to be added an offset to Jan 1 1970 according to Avro spec
                    let date = Int(value + Date.timeIntervalBetween1970AndReferenceDate)
                    encoder.primitive.encode(date)
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
    
    mutating func encode(_ value: String) throws {
        switch schema {
        case .stringSchema:
            encoder.primitive.encode(value)
        case .enumSchema(let attribute):
            if let id = attribute.symbols.firstIndex(of: value) {
                encoder.primitive.encode(id)
            } else {
                throw BinaryEncodingError.typeMismatchWithSchema
            }
        case .unionSchema(let union):
            if let id = union.branches.firstIndex(of: .stringSchema) {
                encoder.primitive.encode(id)
                encoder.primitive.encode(value)
            }
        default:
            throw BinaryEncodingError.typeMismatchWithSchema
        }
    }

    mutating func encode(_ value: [UInt8]) throws {
        if !schema.isBytes() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(fixedValue: [UInt8]) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchema
        }
        encoder.primitive.encode(fixed: fixedValue)
    }
    
}
