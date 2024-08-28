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
    public var encodeKey: Bool = false
    public func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        return KeyedEncodingContainer(AvroKeyedEncodingContainer<Key>(encoder: self, schema: schema))
    }
    public var currrentType: Mirror? = nil
    public func unkeyedContainer() -> UnkeyedEncodingContainer {
        return AvroUnkeyedEncodingContainer(encoder: self, schema: schema)
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
            switch mirror.displayStyle! {
            case Mirror.DisplayStyle.dictionary:
                primitive.encode(mirror.children.count)
                if mirror.children.count > 0 {
                    encodeKey = true
                    try value.encode(to: self)
                    primitive.encode(UInt8(0))
                }
            default:
                encodeKey = false
                try value.encode(to: self)
            }
        case .recordSchema:
            currrentType = Mirror(reflecting: value)
            try value.encode(to: self)
        case .errorSchema:
            currrentType = Mirror(reflecting: value)
            try value.encode(to: self)
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
fileprivate struct skipNil {
    var before: [Int]
    var after: [Int]
}
fileprivate struct  AvroKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K
    
    var codingPath: [CodingKey] {
        return encoder.codingPath
    }
    var valueChildren: Mirror.Children?
    var schemaMap: [String: AvroSchema] = [:]
    
    func schema(_ key: K) ->AvroSchema {
        if let sch = schemaMap[key.stringValue] {
            return sch
        }
        return schema
    }
    
    var encoder: AvroBinaryEncoder
    
    mutating func encodeNilIndexBefore(forKey key: K) {
        if case Optional<Any>.none = valueChildren{
            return
        }
        let count = valueChildren!.count
        for _ in 0...count {
            if let child = valueChildren?.popFirst(){
                if child.label == key.stringValue {
                    return
                }
                if case Optional<Any>.none = child.value {
                    let skippedSchema = schemaMap[child.label!]
                    if ((skippedSchema?.isUnion()) != nil), let index = skippedSchema!.getUnionList().firstIndex(where: { s in
                        s.isNull()
                    }){
                        encoder.primitive.encode(index)
                    }
                }
            }
        }
    }
    
    mutating func encodeNilIndexAfter(forKey key: K) {
        if case Optional<Any>.none = valueChildren{
            return
        }
        if valueChildren!.isEmpty {
            return
        }
        if valueChildren!.contains(where: { (label: String?, value: Any) in
            if case Optional<Any>.none = value {
                return false
            } else {
                return true
            }
        }) {
            return
        }
        valueChildren!.forEach { child in
            let skippedSchema = schemaMap[child.label!]
            if ((skippedSchema?.isUnion()) != nil), let index = skippedSchema!.getUnionList().firstIndex(where: { s in
                s.isNull()
            }){
                encoder.primitive.encode(index)
            }
        }
    }
    mutating func encodeUnionIndex(forKey key: K, typeName: AvroSchema.Types) -> Bool {
        if case .unionSchema(let param) = schema(key) {
            if let index = param.branches.firstIndex(where: { a in
                a.getTypeName() == typeName.rawValue
            }) {
                encoder.primitive.encode(index)
                return true
            }
        }
        return false
    }
    mutating func encodeNil(forKey key: K) throws {
        guard self.schema(key).isNull() else {
            throw BinaryEncodingError.typeMismatchWithSchemaNil
        }
        encoder.primitive.encodeNull()
    }
    
    mutating func encode(_ value: Bool, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isBoolean() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.boolean) else {
            throw BinaryEncodingError.typeMismatchWithSchemaBool
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: String, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isString() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.string) else {
            throw BinaryEncodingError.typeMismatchWithSchemaBool
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Double, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isDouble() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.double) else {
            throw BinaryEncodingError.typeMismatchWithSchemaDouble
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Float, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isFloat() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.float) else {
            throw BinaryEncodingError.typeMismatchWithSchemaFloat
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Int, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isInt() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Int8, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isInt() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt8
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Int16, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isInt() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt16
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Int32, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isInt() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt32
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: Int64, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isLong() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaInt64
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: UInt, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isLong() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: UInt8, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isFixed() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.fixed) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt8
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: UInt16, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isInt() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.int) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt16
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: UInt32, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isLong() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt32
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode(_ value: UInt64, forKey key: K) throws {
        encodeNilIndexBefore(forKey: key)
        guard self.schema(key).isLong() || encodeUnionIndex(forKey:key, typeName:AvroSchema.Types.long) else {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt64
        }
        encoder.primitive.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
    
    mutating func encode<T>(_ value: T, forKey key: K) throws where T : Encodable {
        encodeNilIndexBefore(forKey: key)
        switch schema(key) {
        case .mapSchema(let param):
            if encoder.encodeKey {
                encoder.primitive.encode(key.stringValue)
                let innerEncoder = AvroBinaryEncoder(other: &encoder, schema: param.values)
                try innerEncoder.encode(value)
                return
            }
            
        case .unionSchema(let union):
            if let index = union.branches.firstIndex(where: { s in
                !s.isNull()
            }) {
                encoder.primitive.encode(index)
                self.schemaMap[key.stringValue] = union.branches[index]
            }
        default:
            break
        }
        var container = nestedUnkeyedContainer(forKey: key)
        try container.encode(value)
        encodeNilIndexAfter(forKey: key)
    }
   
    mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
        return KeyedEncodingContainer(AvroKeyedEncodingContainer<NestedKey>(
            encoder: encoder, schema: schema(key)))
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
    mutating func setRecordSchemaMap(encoder: AvroBinaryEncoder, record: AvroSchema.RecordSchema) {
        if let mi = encoder.currrentType {
            for child in mi.children {
                if let label = child.label {
                    if label != "fields" {
                        self.schemaMap[label] = record.findSchema(name: label)
                    } else {
                        record.fields.forEach { field in
                            self.schemaMap[field.name] = field.type
                        }
                    }
                }
            }
        }
    }
    fileprivate var schema: AvroSchema
    init(encoder: AvroBinaryEncoder, schema: AvroSchema) {
        self.encoder = encoder
        self.schema = schema
        self.valueChildren = encoder.currrentType?.children
        switch schema {
        case .recordSchema(let record):
            setRecordSchemaMap(encoder: encoder, record: record)
        case .errorSchema(let record):
            setRecordSchemaMap(encoder: encoder, record: record)
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
            if mirror.children.count > 0 {
                let d = AvroBinaryEncoder(other: &encoder ,schema: array.items)
                try value.encode(to: d)
                encoder.primitive.encode(UInt8(0))
            }
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
        if case .unionSchema(let union) = schema {
            if let s = union.branches.first(where: { b in
                !b.isNull()
            }) {
                self.schema = s
            } else {
                //should throw here
                self.schema = schema
            }
        } else {
            self.schema = schema
        }
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
        case .unionSchema(let param):
            if let nullIndex = param.branches.firstIndex(where: { s in
                s.isNull()
            }) {
                encoder.primitive.encode(nullIndex)
            }
        default:
            throw BinaryEncodingError.typeMismatchWithSchemaNil
        }
    }
    mutating func encode(_ value: Bool) throws {
        if !schema.isBoolean() {
            throw BinaryEncodingError.typeMismatchWithSchemaBool
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Int) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchemaInt
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Int8) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchemaInt8
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Int16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchemaInt16
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int32) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchemaInt32
        }
        encoder.primitive.encode(value)
    }
    
    mutating func encode(_ value: Int64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchemaInt64
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt
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
            throw BinaryEncodingError.typeMismatchWithSchemaUInt8
        }
    }
    mutating func encode(_ value: UInt16) throws {
        if !schema.isInt() {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt16
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt32) throws {
        if !schema.isFixed() {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt32
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: UInt64) throws {
        if !schema.isLong() {
            throw BinaryEncodingError.typeMismatchWithSchemaUInt64
        }
        encoder.primitive.encode(value)
    }
    mutating func encode(_ value: Float) throws {
        if !schema.isFloat() {
            throw BinaryEncodingError.typeMismatchWithSchemaFloat
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
            throw BinaryEncodingError.typeMismatchWithSchemaDouble
        default:
            throw BinaryEncodingError.typeMismatchWithSchemaDouble
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
            throw BinaryEncodingError.typeMismatchWithSchemaString
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
