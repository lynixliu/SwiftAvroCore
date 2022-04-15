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
    fileprivate let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!
    public var userInfo: [CodingUserInfoKey : Any] = [CodingUserInfoKey : Any]()
    init(schema: AvroSchema) {
        self.schema = schema
        userInfo[infoKey] = AvroEncodingOption.AvroBinary
    }
    
    func setUserInfo(userInfo: [CodingUserInfoKey : Any]) {
        self.userInfo = userInfo
    }
    
    func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let encodingOption = userInfo[infoKey] as! AvroEncodingOption
        switch encodingOption {
        case .AvroBinary:
            return try data.withUnsafeBytes{ (pointer: UnsafePointer<UInt8>) in
                let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
                return try type.init(from: decoder)
            }
        case .AvroJson:
            return try JSONDecoder().decode(type, from: data)
        }
    }
    
    func decode<K: Decodable, T: Decodable>(_ type: [K:T].Type, from data: Data) throws -> [K:T] {
        return try data.withUnsafeBytes{ (pointer: UnsafePointer<UInt8>) in
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
            return try [K:T](decoder: decoder)
        }
    }
    
    func decode(from data: Data) throws -> Any? {
        return try data.withUnsafeBytes{ (pointer: UnsafePointer<UInt8>) in
            let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: data.count)
            return try decoder.decode(schema: schema)
        }
    }
}

final class AvroBinaryDecoder: Decoder {
    
    /// required by Decoder
    var codingPath = [CodingKey]()
    var userInfo = [CodingUserInfoKey : Any]()
    
    /// AvroBinaryDecoder
    var primitive: AvroBinaryDecodableProtocol
    
    // schema relate
    var schema: AvroSchema
    
    init(schema: AvroSchema, pointer: UnsafePointer<UInt8>, size: Int) throws {
        self.schema = schema
        self.primitive = AvroPrimitiveDecoder(pointer: pointer, size: size)
    }
    
    fileprivate init(other: AvroBinaryDecoder, schema: AvroSchema) throws {
        self.schema = schema
        self.primitive = other.primitive
    }
    
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        return KeyedDecodingContainer(AvroKeyedDecodingContainer<Key>(decoder: self, schema: schema))
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        return try AvroUnkeyedDecodingContainer(decoder: self, schema: schema)
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        return try AvroSingleValueDecodingContainer(decoder: self, schema: schema)
    }
    
    func decode<MK: Decodable, T: Decodable>(type: [MK: T].Type) throws -> [MK:T] {
        let infoKey = CodingUserInfoKey(rawValue: "decodeOption")!
        self.userInfo[infoKey] = self
        return try [MK:T](decoder: self)
    }
    
    func decode(schema: AvroSchema) throws -> Any? {
        switch schema {
        case .nullSchema:
            return nil
        case .booleanSchema:
            return try primitive.decode() as Bool
        case .intSchema(let intSchema):
            if let logicType = intSchema.logicalType, logicType == .date {
                let unixdate = Double(try primitive.decode() as Int)
                return Date(timeIntervalSince1970: unixdate)
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
        case .stringSchema:
            return try primitive.decode() as String
        case .recordSchema(let recordSchema):
            var value: [String: Any] = [String: Any]()
            for f in recordSchema.fields {
                if let v = try decode(schema: f.type) {
                    value[f.name] = v
                }
            }
            return value
        case .enumSchema(let enumSchema):
            let index = try primitive.decode() as Int
            if (index < 0 || index > enumSchema.symbols.count) {
                print("index of symbols:",index)
                throw BinaryDecodingError.indexOutofBoundary
            }
            return enumSchema.symbols[index]
        case .arraySchema(let arraySchema):
            var value: [Any] = [Any]()
            var blockCount = try primitive.decode() as Int64
            while blockCount != 0 {
                if blockCount < 0 {
                    blockCount = -blockCount
                    let beforeDecodedLength = primitive.available
                    for _ in 0..<blockCount {
                        let blockSize = try primitive.decode() as Int64
                        if blockSize <= 0 {
                            return value
                        }
                        if let v = try decode(schema: arraySchema.items) {
                            value.append(v)
                        } else {
                            //skip unkown block
                            primitive.advance(Int(blockSize) - (beforeDecodedLength - primitive.available))
                        }
                    }
                } else {
                    for _ in 0..<blockCount {
                        let v = try decode(schema: arraySchema.items)
                        value.append(v!)
                    }
                }
                blockCount = try primitive.decode() as Int64
            }
            return value
        case .mapSchema(let mapSchema):
            var value: [String: Any] = [String: Any]()
            var blockCount = try primitive.decode() as Int64
            while blockCount != 0 {
                if blockCount < 0 {
                    blockCount = -blockCount
                    let beforeDecodedLength = primitive.available
                    for _ in 0..<blockCount {
                        let blockSize = try primitive.decode() as Int64
                        if blockSize <= 0 {
                            return value
                        }
                        if let k = try? primitive.decode() as String {
                            value[k] = try decode(schema: mapSchema.values)
                        } else {
                            //skip unkown block
                            primitive.advance(Int(blockSize) - (beforeDecodedLength - primitive.available))
                        }
                    }
                }else {
                    for _ in 0..<blockCount{
                        let k = try primitive.decode() as String
                        value[k] = try decode(schema: mapSchema.values)
                    }
                }
                blockCount = try primitive.decode() as Int64
            }
            return value
        case .unionSchema(let unionSchema):
            let index = try primitive.decode() as Int64
            if (index < 0 || index > unionSchema.branches.count) {
                print("index of union:",index)
                throw BinaryDecodingError.indexOutofBoundary
            }
            return try decode(schema:unionSchema.branches[Int(index)])
        case .fixedSchema(let fixedSchema):
            if let logicalType = fixedSchema.logicalType, logicalType == .duration {
                return try primitive.decode(fixedSize: fixedSchema.size) as [UInt32]
            }
            return try primitive.decode(fixedSize: fixedSchema.size) as [UInt8]
        case .errorSchema(let errorSchema):
            var value: [String: Any] = [String: Any]()
            for f in errorSchema.fields{
                value[f.name] = try decode(schema: f.type)
            }
            return value
        default:
            return nil
        }
    }
}

fileprivate struct AvroKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    class unionIndexCache {
        var indexMap: [String:Int] = [:]
        func add(key: String, index: Int) {
            indexMap[key] = index
        }
        func getIndex(key: String) -> Int? {
            return indexMap[key]
        }
    }
    var allKeys: [K] {
        return schemaMap.keys.reduce(into: [K]()) { keys, key in
            guard let key = K.init(stringValue: key) else {
                return
            }
            keys.append(key)
        }
    }
    var codingPath: [CodingKey] = []
    fileprivate var decoder: AvroBinaryDecoder
    var schemaMap: [String: AvroSchema] = [:]
    var unionIndex: unionIndexCache
    func schema(_ key: K) ->AvroSchema {
        let s = schemaMap[key.stringValue]!
        if case .unionSchema(let union) = s {
            if let index = unionIndex.getIndex(key: key.stringValue) {
                return union.branches[index]
            }
        }
        return s
    }
    
    func contains(_ key: K) -> Bool {
        return schemaMap.keys.contains(key.stringValue)
    }
    func decodeNil(forKey key: K) throws -> Bool {
        let currentSchema = schema(key)
        switch currentSchema {
        case .nullSchema:
            return true
        case .unionSchema(let union):
            let index = try! decoder.primitive.decode() as Int
            guard index < union.branches.count else {
                throw BinaryDecodingError.indexOutofBoundary
            }
            unionIndex.add(key: key.stringValue, index: index)
            return union.branches[index].isNull()
        default:
            return false
        }
    }
    @inlinable
    mutating func decode(_ type: Bool.Type, forKey key: K) throws -> Bool {
        guard self.schema(key).isBoolean() else {
            throw BinaryDecodingError.typeMismatchWithSchemaBool
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: Int.Type, forKey key: K) throws -> Int {
        guard self.schema(key).isInt() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt
        }
        return try Int(decoder.primitive.decode() as Int64)
    }
    @inlinable
    mutating func decode(_ type: Int8.Type, forKey key: K) throws -> Int8 {
        guard self.schema(key).isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt8
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: Int16.Type, forKey key: K) throws -> Int16 {
        guard self.schema(key).isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt16
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: Int32.Type, forKey key: K) throws -> Int32 {
        guard self.schema(key).isInt() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt32
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: Int64.Type, forKey key: K) throws -> Int64 {
        guard self.schema(key).isLong() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt64
        }
        let r: Int64 = try decoder.primitive.decode()
        return r
    }
    @inlinable
    mutating func decode(_ type: UInt.Type, forKey key: K) throws -> UInt {
        guard self.schema(key).isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8 {
        guard self.schema(key).isFixed() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt8
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16 {
        guard self.schema(key).isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt16
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32 {
        guard self.schema(key).isFixed() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt32
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64 {
        guard self.schema(key).isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt64
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: Float.Type, forKey key: K) throws -> Float {
        guard self.schema(key).isFloat() else {
            throw BinaryDecodingError.typeMismatchWithSchemaFloat
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: Double.Type, forKey key: K) throws -> Double {
        guard self.schema(key).isDouble() else {
            throw BinaryDecodingError.typeMismatchWithSchemaDouble
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: [UInt8].Type, forKey key: K) throws -> [UInt8] {
        guard self.schema(key).isBytes() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt8
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    mutating func decode(_ type: [UInt32].Type, forKey key: K) throws -> [UInt32] {
        let sch = self.schema(key)
        switch sch {
        case .fixedSchema(let fixed):
            return try decoder.primitive.decode(fixedSize: fixed.size)
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaUInt32
        }
    }
    @inlinable
    mutating func decode(_ type: String.Type, forKey key: K) throws -> String {
        guard self.schema(key).isString() else {
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T : Decodable {
        let currentSchema = schema(key)
        switch currentSchema {
        case .mapSchema,.fixedSchema:
            var container = try nestedUnkeyedContainer(forKey: key)
            return try container.decode(type)
        case .unknownSchema:
            throw BinaryEncodingError.invalidSchema
        default:
            let innerDecoder = try! AvroBinaryDecoder(other: decoder, schema: currentSchema)
            return try type.init(from: innerDecoder)
        }
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        return KeyedDecodingContainer(AvroKeyedDecodingContainer<NestedKey>(decoder: decoder, schema: schema(key)))
    }
    
    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        return try AvroUnkeyedDecodingContainer(decoder: decoder, schema: schema(key))
    }
    
    func superDecoder() throws -> Decoder {
        return decoder
    }
    
    func superDecoder(forKey key: K) throws -> Decoder {
        return decoder
    }
    
    fileprivate init(decoder: AvroBinaryDecoder, schema: AvroSchema) {
        self.decoder = decoder
        switch(schema) {
        case .recordSchema(let record):
            for field in record.fields {
                self.schemaMap[field.name] = field.type
            }
            self.schemaMap["fields"] = .fieldsSchema(record.fields)
        case .fieldsSchema(let fields):
            for field in fields {
                self.schemaMap[field.name] = field.type
            }
        case .mapSchema(let map):
            self.schemaMap[map.type] = map.values
        case .fieldSchema(let field):
            self.schemaMap[field.name] = field.type
        default: self.schemaMap[schema.getName()!] = schema
        }
        self.codingPath = decoder.codingPath
        self.unionIndex = unionIndexCache()
    }
}

fileprivate struct AvroUnkeyedDecodingContainer: UnkeyedDecodingContainer, DecodingHelper {
    var codingPath: [CodingKey]
    var schema: AvroSchema
    fileprivate var keySchema: AvroSchema? = nil
    fileprivate var valueSchema: AvroSchema = AvroSchema()
    var decoder: AvroBinaryDecoder
    fileprivate var haveTail: Bool = false
    fileprivate var haveBlock: Bool = false
    fileprivate var countValue: Int
    fileprivate var count: Int? {
        return countValue
    }
    var isAtEnd: Bool {
        if let c = count {
            let ret = (currentIndex >= c)
            return ret
        } else {
            return true
        }
    }
    
    fileprivate var currentIndex: Int = 0
    func getCurrentSchema() throws -> AvroSchema {
        if count == 0 {
            return valueSchema
        }
        if let k = keySchema {
            if (currentIndex % 2) == 0 {
                if self.haveBlock {
                    _ = try decoder.primitive.decode() as Int64
                }
                return  k
            }
            return valueSchema
        }
        if self.haveBlock {
            if currentIndex < count! {
                _ = try decoder.primitive.decode() as Int64
            }
        }
        return valueSchema
    }
    mutating func advanceIndex() {
        currentIndex += 1
        if currentIndex == countValue,haveTail {
            if var blockCount = try? decoder.primitive.decode() as Int64 {
                if blockCount == 0 {
                    return
                }
                if blockCount < 0 {
                    haveBlock = true
                    blockCount = -blockCount
                }
                if keySchema != nil {
                    blockCount <<= 1
                }
                countValue += Int(blockCount)
            }
        }
    }
    mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        advanceIndex()
        return KeyedDecodingContainer(AvroKeyedDecodingContainer(decoder: decoder, schema: schema))
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        return try AvroUnkeyedDecodingContainer(decoder: decoder, schema: schema)
    }
    
    mutating func superDecoder() throws -> Decoder {
        advanceIndex()
        return decoder
    }
    @inlinable
    mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        defer {advanceIndex()}
        let innerDecoder = try AvroBinaryDecoder(other: decoder, schema: getCurrentSchema())
        let dd =  try T.init(from: innerDecoder)
        return dd
    }

    fileprivate init(decoder: AvroBinaryDecoder, schema: AvroSchema) throws {
        self.decoder = decoder
        self.codingPath = decoder.codingPath
        
        switch schema {
        /// get the size of avro array,bytes and map from the decoding data
        case .arraySchema(let array):
            let blockCount = try decoder.primitive.decode() as Int64
            if blockCount < 0 {
                self.countValue = -Int(blockCount)
                self.haveBlock = true
            } else {
                self.countValue = Int(blockCount)
            }
            self.valueSchema = array.items
            self.haveTail = true
            self.schema = self.valueSchema
        case .bytesSchema:
            self.countValue = try Int(decoder.primitive.decode() as Int64)
            self.valueSchema = schema
            self.schema = self.valueSchema
        case .mapSchema(let map):
            /// map are key value pairs, so the count is doubled of block count
            let blockCount = try decoder.primitive.decode() as Int64
            if blockCount < 0 {
                self.countValue = -(Int(blockCount)<<1)
                self.haveBlock = true
            } else {
                self.countValue = Int(blockCount)<<1
            }
            self.schema = .stringSchema
            self.keySchema = self.schema
            self.valueSchema = map.values
            self.haveTail = true
        /// get the size of avro fixed from schema
        case .fixedSchema(let fixed):
            if let logicalType = fixed.logicalType, logicalType == .duration {
                self.countValue = 3 /// 3 UInt32 : month/day/year
            } else {
                self.countValue = fixed.size
            }
            self.valueSchema = schema
            self.schema = self.valueSchema
        default:
            self.valueSchema = schema
            self.schema = self.valueSchema
            self.countValue = 1
        }
        self.currentIndex = 0
    }
}

fileprivate struct AvroSingleValueDecodingContainer: SingleValueDecodingContainer, DecodingHelper {
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
            let index = try decoder.primitive.decode() as Int
            guard index < union.branches.count else {
                throw BinaryDecodingError.indexOutofBoundary
            }
            self.schema = union.branches[index]
        default:
            self.schema = schema
            self.schema = schema.getSerializedSchema().first!
        }
    }
    @inlinable
    func decodeIfPresent(_ type: String.Type) throws -> String? {
        switch self.schema {
        case .stringSchema:
            return try decoder.primitive.decode() as String
        case .enumSchema(let symbols):
            let id = try decoder.primitive.decode() as Int
            return symbols.symbols[id]
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
    }
}

fileprivate protocol DecodingHelper {
    var codingPath: [CodingKey] { get }
    var decoder: AvroBinaryDecoder { get set}
    var schema: AvroSchema {get}
}

extension DecodingHelper {
    func decodeNil() -> Bool {
        if self.schema.isNull() {
            return true
        }
        return false
    }
    @inlinable
    func decode(_ type: Bool.Type) throws -> Bool {
        guard self.schema.isBoolean() else {
            throw BinaryDecodingError.typeMismatchWithSchemaBool
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Int.Type) throws -> Int {
        guard self.schema.isLong() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Int8.Type) throws -> Int8 {
        guard self.schema.isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt8
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Int16.Type) throws -> Int16 {
        guard self.schema.isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt16
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Int32.Type) throws -> Int32 {
        guard self.schema.isInt()||self.schema.isContainer() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt32
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Int64.Type) throws -> Int64 {
        guard self.schema.isLong() else {
            throw BinaryDecodingError.typeMismatchWithSchemaInt64
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: UInt.Type) throws -> UInt {
        guard self.schema.isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: UInt8.Type) throws -> UInt8 {
        guard self.schema.isByte() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt8
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: UInt16.Type) throws -> UInt16 {
        guard self.schema.isInteger() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt16
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: UInt32.Type) throws -> UInt32 {
        guard self.schema.isFixed() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt32
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: UInt64.Type) throws -> UInt64 {
        guard self.schema.isLong() else {
            throw BinaryDecodingError.typeMismatchWithSchemaUInt64
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Float.Type) throws -> Float {
        guard self.schema.isFloat() else {
            throw BinaryDecodingError.typeMismatchWithSchemaFloat
        }
        return try decoder.primitive.decode()
    }
    @inlinable
    func decode(_ type: Double.Type) throws -> Double {
        switch self.schema {
        case .doubleSchema:
            return try decoder.primitive.decode()
        case .intSchema(let intSchema):
            /// Date is Codable in Swift and init(from: decoder), in singleValueContainer
            /// the value is decoded as Double and date is from timeIntervalSinceReferenceDate,
            /// so it need to be minus an offset to Jan 1 1970 according to Avro spec
            if let logicalType = intSchema.logicalType, logicalType == .date {
                let unixdate = Double(try decoder.primitive.decode() as Int)
                return unixdate - Date.timeIntervalBetween1970AndReferenceDate
            }
            throw BinaryDecodingError.typeMismatchWithSchemaDouble
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaDouble
        }
    }
    @inlinable
    func decode(_ type: String.Type) throws -> String {
        switch self.schema {
        case .stringSchema:
            return try decoder.primitive.decode() as String
        case .enumSchema(let symbols):
            let id = try decoder.primitive.decode() as Int
            return symbols.symbols[id]
        default:
            throw BinaryDecodingError.typeMismatchWithSchemaString
        }
    }
    @inlinable
    func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        let d = try AvroBinaryDecoder(other: decoder, schema: schema)
        return try type.init(from: d)
    }
}

fileprivate struct UnsupportedAvroType: Error {}
fileprivate struct InvalidResponseFormat: Error {}
protocol AvroDecodable: Decodable {
    init(decoder: AvroBinaryDecoder) throws
}

public extension KeyedDecodingContainer {
    func decode<MK: Decodable, T: Decodable>(
        _ type: [MK : T].Type, forKey key: Key) throws -> [MK : T]
    {
        guard self.contains(key) else {
            throw BinaryDecodingError.malformedAvro
        }
        var c = try nestedUnkeyedContainer(forKey: key)
        var values = [MK : T]()
        if c.count == 0 {
            return values
        }
        while !c.isAtEnd {
            if let k = try? c.decode(type.Key) {
                values[k] = try c.decode(type.Value)
            }
        }
        return values
    }
    func decodeIfPresent<MK: Decodable, T: Decodable>(
        _ type: [MK : T].Type, forKey key: Key) throws -> [MK : T]?
    {
        guard self.contains(key) else {
            throw BinaryDecodingError.malformedAvro
        }
        if try self.decodeNil(forKey: key) {
            return nil
        }
        var c = try nestedUnkeyedContainer(forKey: key)
        var values = [MK : T]()
        if c.count == 0 {
            return values
        }
        while !c.isAtEnd {
            if let k = try? c.decode(type.Key) {
                values[k] = try c.decode(type.Value)
            }
        }
        return values
    }
}

extension Dictionary: AvroDecodable where Key : Decodable, Value : Decodable {
    init(decoder: AvroBinaryDecoder) throws {
        self.init()
        
        // Decode as an array of key-value pairs.
        var container = try! decoder.unkeyedContainer()
    
        // Count of key-value pairs should be even number
        if let count = container.count {
            guard count % 2 == 0 else {
                throw DecodingError.dataCorrupted(
                    DecodingError.Context(
                        codingPath: decoder.codingPath,
                        debugDescription: "Expected collection of key-value pairs; encountered odd-length array instead."))
            }
        }
        
        while !container.isAtEnd {
            
            let key = try container.decode(Key.self)
            
            guard !container.isAtEnd else {
                throw DecodingError.dataCorrupted(
                    DecodingError.Context(
                        codingPath: decoder.codingPath,
                        debugDescription: "Unkeyed container reached end before value in key-value pair."))
            }
            
            let value = try container.decode(Value.self)
            self[key] = value
        }
    }
}
