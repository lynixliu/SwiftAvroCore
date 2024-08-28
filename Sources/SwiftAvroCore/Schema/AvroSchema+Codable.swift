
//
//  AvroClient/SchemaCodable.swift
//
//  Created by Yang Liu on 24/08/12.
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


fileprivate func encodePrimitives(_ value: AvroSchema.Types) -> Data? {
    return "\"\(value.rawValue)\"".data(using: .utf8)
}
fileprivate func encodeLogicalType(type: AvroSchema.Types, logicalType: AvroSchema.LogicalType) -> Data? {
    return "{\"type\":\"\(type.rawValue)\",\"logicalType\":\"\(logicalType.rawValue)\"}".data(using: .utf8)
}



// implement codable protocol for Avro Schema encoder and decoder
extension AvroSchema  {

    
    init(type: String) {
        switch type {
        case Types.null.rawValue:
            self = .nullSchema
        case Types.boolean.rawValue:
            self = .booleanSchema
        case Types.int.rawValue:
            self = .intSchema(IntSchema())
        case Types.long.rawValue:
            self = .longSchema(IntSchema(isLong: true))
        case Types.float.rawValue:
            self = .floatSchema
        case Types.double.rawValue:
            self = .doubleSchema
        case Types.bytes.rawValue:
            self = .bytesSchema(BytesSchema())
        case Types.string.rawValue:
            self = .stringSchema
        default:
            self = .unknownSchema(UnknownSchema(type))
        }
    }
    
    // init from String and JSONDecoder
    public init (schemaJson: String, decoder: JSONDecoder) throws {
        // Avro specifacation http://avro.apache.org/docs/current/spec.html#Transforming+into+Parsing+Canonical+Form
        // Transforming into Parsing Canonical Form
        // [PRIMITIVES] Convert primitive schemas to their simple form (e.g., int instead of {"type":"int"}).
        // But JSONSerialization(using by JSONDecoder) requires the top level object is
        // an NSArray or NSDictionary https://developer.apple.com/documentation/foundation/jsonserialization
        // So add a branch for primitives simple form
        let MaxPrimitivesNameCount = 10 // = "\"boolean\"".count + 1
        do {
            if (schemaJson.count < MaxPrimitivesNameCount) {
                self.init(type: schemaJson.replacingOccurrences(of: "\"", with: ""))
            } else {
                let json = schemaJson.data(using: .utf8)!
                try self.init(schema: json, decoder: decoder)
            }
        } catch {
            throw error
        }
    }
    // init from Data and JSONDecoder
    init (schema: Data, decoder: JSONDecoder) throws {
        do {
            self = try decoder.decode(AvroSchema.self, from: schema)
        } catch {
            throw error
        }
    }
    enum FieldCodingKeys: String, CodingKey {
        case fields
        case symbols
        case items
        case values
        case size
        case type
        case namespace
    }
    private enum CodingKeys: String, CodingKey {
        case type, name, namespace, aliases, doc, protocolName = "protocol",
        items, values, fields, symbols, size, order, defaultValue = "default",
        logicalType, precision, scale,
        error,messages,
        /// Avro 2.0 Specification Proposals
        optional,/// optional field
        union, branches/// named union
    }
    
    /// init Schema from Keyed container
    
    private init(container: KeyedDecodingContainer<CodingKeys>, decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        do  {
            /// when parsing json schema, some inner schemas of complex schema may use name or aliases
            /// defined in parent schema or previous brother schema as type, but JSONDecoder decode string
            /// in a deep first order, at this time, the parent's type of current schema cannot be retrieved
            /// whitout extra cache parameter or regression parsing, because the parent is not created yet.
            /// for simplicity, the type of current schema can be guessed from the required field such as:
            /// fields for record, sybmols for enum, items for array, values for map and size for fixed.
            /// the name and type correction and namespace filling are delayed to validate step for naming schemas.
            if container.contains(.fields){
                var param = try RecordSchema(from: decoder)
                try param.validate(typeName: Types.record.rawValue,name: nil, nameSpace: nil)
                if param.type == "error" {
                    self = .errorSchema(param)
                    return
                }
                self = .recordSchema(param)
            } else if container.contains(.symbols){
                var schema = try EnumSchema(from: decoder)
                schema.validateName(typeName: Types.enums.rawValue, name: nil,nameSpace: nil)
                self = .enumSchema(schema)
            } else if container.contains(.items){
                let schema = try ArraySchema(from: decoder)
                self = .arraySchema(schema)
            } else if container.contains(.values){
                let schema = try MapSchema(from: decoder)
                self = .mapSchema(schema)
            } else if container.contains(.size){
                var schema = try FixedSchema(from: decoder)
                schema.validateName(typeName: Types.fixed.rawValue, name: nil, nameSpace: nil)
                self = .fixedSchema(schema)
            } /*else if container.contains(.protocolName) {
                var schema = try ProtocolSchema(from: decoder)
                try schema.validate(typeName: Types.protocolName.rawValue, name: nil, nameSpace: nil)
                self = .protocolSchema(schema)
            } else if container.contains(.messages) {
                let schema = try MessageSchema(from: decoder)
                self = .messageSchema(schema)
            } */else if container.contains(.branches) {
                var schema = try UnionSchema(from: decoder)
                try schema.validate(typeName: Types.union.rawValue, name: nil, nameSpace: nil)
                self = .unionSchema(schema)
            } else if container.contains(.type) {
                // reference type, set to unkown to validate later
                if container.contains(.name) && container.allKeys.count == 2 {
                    var n = ""
                    var t = ""
                    for k in container.allKeys {
                        switch k {
                        case .type:
                            t = try container.decode(String.self, forKey: .type)
                        default:
                            n = try container.decode(String.self, forKey: .name)
                        }
                    }
                    let schema = UnknownSchema(typeName: t, name: n)
                    self = .unknownSchema(schema)
                    return
                }
                    /// if the json schema use standard type, decode directly.
                if let type = try container.decodeIfPresent(Types.self, forKey: .type) {
                    switch type {
                    case .null:
                        self = .nullSchema
                    case .boolean:
                        self = .booleanSchema
                    case .int:
                        let logicalType = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                        if let logicType = logicalType {
                            self = .intSchema(IntSchema(type: type.rawValue, logicalType: logicType))
                        } else {
                            self = .intSchema(IntSchema())
                        }
                    case .long:
                        let logicalType = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                        if let logicType = logicalType {
                            self = .longSchema(IntSchema(type: type.rawValue, logicalType: logicType))
                        } else {
                            self = .longSchema(IntSchema(isLong: true))
                        }
                    case .float:
                        self = .floatSchema
                    case .double:
                        self = .doubleSchema
                    case .string:
                        self = .stringSchema
                    case .bytes:
                        let logicalType = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                        if let logicType = logicalType {
                            let precision = try container.decodeIfPresent(Int.self, forKey: .precision)!
                            let scale = try container.decodeIfPresent(Int.self, forKey: .scale)!
                            self = .bytesSchema(BytesSchema(logicalType: logicType,
                                                            precision: precision,
                                                            scale: scale))
                        } else {
                            self = .bytesSchema(BytesSchema())
                        }
                    default:
                        self = .unknownSchema(UnknownSchema(type.rawValue))
                    }
                } else {
                    self = .unknownSchema(UnknownSchema(""))
                }
            } else {
                    let primitive = try container.decode(String.self, forKey: .type)
                    self = AvroSchema(type: primitive)
                    return
            }
        } catch {
            throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: container.codingPath, debugDescription: "unkonw schema jasn format", underlyingError: AvroSchemaDecodingError.unknownSchemaJsonFormat))
        }
        return
    }

    // init from Decoder in Decodable protocol
    public init(from decoder: Decoder) throws {
        // get from type key
        if let container = try? decoder.container(keyedBy: CodingKeys.self) {
            try self.init(container: container, decoder: decoder)
        }// get from single value for primitive types and union
        else if let singlecontainer = try? decoder.singleValueContainer() {
            // primitive types
            if let type = try? singlecontainer.decode(String.self) {
                self = AvroSchema(type: type)
            }// union
            else if let type = try? singlecontainer.decode([AvroSchema].self) {
                self = .unionSchema(AvroSchema.UnionSchema(branches: type))
            } else {
                print("not present type,decoder")
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
        } else {
            throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "unkonw schema jasn format", underlyingError: AvroSchemaDecodingError.unknownSchemaJsonFormat))
        }
    }

    // Avro specifacation http://avro.apache.org/docs/current/spec.html#Transforming+into+Parsing+Canonical+Form
    // Transforming into Parsing Canonical Form
    // [PRIMITIVES] Convert primitive schemas to their simple form (e.g., int instead of {"type":"int"}).
    // But JSONSerialization(using by JSONEncoder to convert JSON string to Data) requires the top level object is
    // an NSArray or NSDictionary https://developer.apple.com/documentation/foundation/jsonserialization
    // So add a branch for primitives simple form
    public func encode(jsonEncoder: JSONEncoder) throws -> Data? {
        switch self {
        case .nullSchema:
            return encodePrimitives(Types.null)
        case .booleanSchema:
            return encodePrimitives(Types.boolean)
        case .intSchema(let attribute):
            if let logicalType = attribute.logicalType {
                return encodeLogicalType(type: Types.int, logicalType: logicalType)
            }
            return encodePrimitives(Types.int)
        case .longSchema(let attribute):
            if let logicalType = attribute.logicalType {
                return encodeLogicalType(type: Types.long, logicalType: logicalType)
            }
            return encodePrimitives(Types.long)
        case .floatSchema:
            return encodePrimitives(Types.float)
        case .doubleSchema:
            return encodePrimitives(Types.double)
        case .bytesSchema(let attribute):
            if attribute.logicalType != nil {
                return try jsonEncoder.encode(self)
            }
            return encodePrimitives(Types.bytes)
        case .stringSchema:
            return encodePrimitives(Types.string)
        default:
            return try jsonEncoder.encode(self)
        }
    }
    
    // encode from Encoder in Encodable protocol
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .nullSchema:
            try container.encode(Types.null)
        case .booleanSchema:
            try container.encode(Types.boolean)
        case .intSchema(let attribute):
            if let logicType = attribute.logicalType {
                try container.encode(logicType)
            } else {
                try container.encode(Types.int)
            }
        case .longSchema(let attribute):
            if let logicType = attribute.logicalType {
                try container.encode(logicType)
            } else {
                try container.encode(Types.long)
            }
        case .floatSchema:
            try container.encode(Types.float)
        case .doubleSchema:
            try container.encode(Types.double)
        case .bytesSchema(let attribute):
            if attribute.logicalType != nil {
                try attribute.encode(to: encoder)
                if !attribute.validate() {
                    throw BinaryEncodingError.invalidDecimal
                }
            } else {
                try container.encode(Types.bytes)
            }
        case .stringSchema:
            try container.encode(Types.string)
        case .recordSchema(let attribute):
            try attribute.encode(to: encoder)
        case .arraySchema(let attribute):
            try attribute.encode(to: encoder)
        case .mapSchema(let attribute):
            try attribute.encode(to: encoder)
        case .fixedSchema(let attribute):
            if let logic = attribute.logicalType, logic == .decimal {
                try attribute.encode(to: encoder)
                if !attribute.validate() {
                    throw BinaryEncodingError.invalidDecimal
                }
            } else {
                try attribute.encode(to: encoder)
            }
        case .enumSchema(let attribute):
            try attribute.encode(to: encoder)
        case .unionSchema(let attribute):
            try container.encode(attribute)
        case .fieldSchema(let attribute):
            try container.encode(attribute)
        case .fieldsSchema(let attribute):
            try container.encode(attribute)
        case .unknownSchema:
            throw EncodingError.invalidValue(self, EncodingError.Context(codingPath: container.codingPath, debugDescription: "Schema type invalid", underlyingError: AvroSchemaEncodingError.invalidSchemaType))
        case .errorSchema(let attribute):
            try attribute.encode(to: encoder)
        /*case .protocolSchema(let message):
            try message.encode(to: encoder)
        case .messageSchema(let attribute):
            try attribute.response.encode(to: encoder)*/
        }
    }
    
    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        switch self {
        case .recordSchema(var recordSchema):
            try recordSchema.validate(typeName: Types.record.rawValue, name: name, nameSpace: nameSpace)
            self = .recordSchema(recordSchema)
        case .enumSchema(var enumSchema):
            enumSchema.validateName(typeName: Types.enums.rawValue, name: name, nameSpace: nameSpace)
            self = .enumSchema(enumSchema)
        case .arraySchema(var arraySchema):
            try arraySchema.items.validate(typeName:Types.array.rawValue, name: name, nameSpace: nameSpace)
            self = .arraySchema(arraySchema)
        case .mapSchema(var mapSchema):
            try mapSchema.values.validate(typeName: Types.map.rawValue, name: name, nameSpace: nameSpace)
            self = .mapSchema(mapSchema)
        case .unionSchema(var unionSchema):
            try unionSchema.validate(typeName: Types.union.rawValue, name: name, nameSpace: nameSpace)
            self = .unionSchema(unionSchema)
        case .fixedSchema(var fixedSchema):
            fixedSchema.validateName(typeName: Types.fixed.rawValue, name: name, nameSpace: nameSpace)
            self = .fixedSchema(fixedSchema)
        /*case .protocolSchema(var protocolSchema):
            try protocolSchema.validate(typeName: Types.protocolName.rawValue, name: name, nameSpace: nameSpace)
            self = .protocolSchema(protocolSchema)*/
        case .errorSchema(var errorSchema):
            try errorSchema.validate(typeName: Types.error.rawValue, name: name, nameSpace: nameSpace)
            self = .errorSchema(errorSchema)
        default:
            return
        }
    }
}

enum NamedAttributesCodingKeys: CodingKey {
    case name, type, namespace, aliases
}

extension NameSchemaProtocol {
    func encodeHeader(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: NamedAttributesCodingKeys.self)
        try container.encode(getFullname(), forKey: .name)
        try container.encode(type, forKey: .type)
        if !encoder.userInfo.isEmpty {
            try container.encodeIfPresent(namespace, forKey: .namespace)
            try container.encodeIfPresent(aliases, forKey: .aliases)
        }
    }

    mutating func validateName(typeName: String,name: String?, nameSpace: String?) {
        if type != typeName, self.name == nil {
            self.name = type
            type = typeName
        }
        if let n = name {
            self.name = n
        }
        if let ns = nameSpace {
            namespace = ns
        }
    }
}

extension AvroSchema.UnionSchema  {
    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        for i in 0..<branches.count {
            try branches[i].validate(typeName: typeName, name: name, nameSpace: nameSpace)
        }
    }
    
    mutating func validate(typeName: String, typeMap: [String: AvroSchema], nameSpace: String?) throws {
        var uniqueMap = [String: AvroSchema]()
        for i in 0..<branches.count {
            switch branches[i] {
            case .unknownSchema(let unknownSchema):
                if var schema = typeMap[unknownSchema.name!] {
                    try schema.validate(typeName: schema.getTypeName(), name: unknownSchema.name, nameSpace: nameSpace)
                    branches[i] = schema
                } else {
                    for j in 0..<i {
                        if branches[j].getName() == unknownSchema.type {
                            var schema = branches[j]
                            try schema.validate(typeName: schema.getTypeName(), name: unknownSchema.name, nameSpace: nameSpace)
                            branches[i] = schema
                            break
                        }
                    }
                }
            case .recordSchema(var r):
                try r.validate(typeName: AvroSchema.Types.record.rawValue, typeMap: typeMap, nameSpace: nameSpace)
                branches[i] = .recordSchema(r)
            case .errorSchema(var r):
                try r.validate(typeName: AvroSchema.Types.error.rawValue, typeMap: typeMap,nameSpace: nameSpace)
                branches[i] = .errorSchema(r)
            default:
                try branches[i].validate(typeName: typeName, name: nil, nameSpace: nameSpace)
            }
            if i > 0 {
                if let n = uniqueMap[branches[i].getTypeName()] {
                    if n.getName() == branches[i].getName() {
                        throw AvroSchemaDecodingError.typeDuplicateBranchInUnion
                    }
                }
            }
            uniqueMap[branches[i].getTypeName()] = branches[i]
        }
    }
}

extension AvroSchema.RecordSchema {
    enum EncodeRecordCodingKeys: CodingKey {
        case fields, doc
    }
    /// as Avro spec defined:
    /// [ORDER] Order the appearance of fields of JSON objects as follows:
    /// name, type, fields, symbols, items, values, size.
    /// For example, if an object has type, name, and size fields,
    /// then the name field should appear first, followed by the type and then the size fields.
    public func encode(to encoder: Encoder) throws {
        try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeRecordCodingKeys.self)
        try container.encode(fields, forKey: .fields)
        if encoder.userInfo.isEmpty {return}
        if let userInfo = encoder.userInfo.first {
            if let option = userInfo.value as? AvroSchemaEncodingOption {
                switch option {
                case .PrettyPrintedForm:
                    try container.encodeIfPresent(doc, forKey: .doc)
                default:break
                }
            }
        }
    }
    /// correct the name and type for some guessed schema in decoding step
    /// filling the empty namespace field for inner named schemas
    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        validateName(typeName: typeName, name: name, nameSpace: nameSpace)
        for i in 0..<fields.count {
            switch fields[i].type {
            case .unknownSchema(let t):
                for j in 0..<i {
                    if fields[j].type.getName() == t.name {
                        fields[i].type = fields[j].type
                        var ns = nameSpace
                        switch fields[j].type {
                        case .enumSchema(let e):
                            ns = e.replaceParentNamespace(name: fields[i].name)
                        case .fixedSchema(let f):
                            ns = f.replaceParentNamespace(name: fields[i].name)
                        case .recordSchema(let r):
                            ns = r.replaceParentNamespace(name: fields[i].name)
                        default:
                            ns = nameSpace
                        }
                        try fields[i].type.validate(typeName: fields[i].type.getTypeName(), name: t.name, nameSpace: ns)
                        break
                    }
                }
            case .unionSchema(var u):
                var typeMap = [String: AvroSchema]()
                for j in 0..<i {
                    if let fn = fields[j].type.getName() {
                        typeMap[fn] = fields[j].type
                    } else {
                        typeMap["null"] = .nullSchema
                    }
                }
                try u.validate(typeName: typeName, typeMap: typeMap, nameSpace: getNamespace(name: fields[i].name))
                fields[i].type = .unionSchema(u)
            default:
                try fields[i].type.validate(typeName: typeName, name: nil, nameSpace: getNamespace(name: fields[i].name))
            }
            
        }
    }
    
    mutating func validate(typeName: String, typeMap: [String: AvroSchema], nameSpace: String?) throws {
        validateName(typeName: typeName, name: nil, nameSpace: nameSpace)
        for i in 0..<fields.count {
            try fields[i].validate(nameSpace: getNamespace(name: fields[i].name), typeMap:typeMap)
        }
    }
}


extension AvroSchema.FieldSchema {
    
    enum RecordFieldCodingKeys: String, CodingKey {
        case name
        case type
        case order
        case aliases
        case defaultValue = "default"
        case optional
        case doc
    }
    /// as Avro spec defined:
    /// [ORDER] Order the appearance of fields of JSON objects as follows:
    /// name, type, fields, symbols, items, values, size.
    /// For example, if an object has type, name, and size fields,
    /// then the name field should appear first, followed by the type and then the size fields.
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: RecordFieldCodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(type, forKey: .type)
        try container.encodeIfPresent(order, forKey: .order)
        try container.encodeIfPresent(defaultValue, forKey: .defaultValue)
        if encoder.userInfo.isEmpty {return}
        try container.encodeIfPresent(aliases, forKey: .aliases)
        if let userInfo = encoder.userInfo.first {
            if let option = userInfo.value as? AvroSchemaEncodingOption {
                switch option {
                case .PrettyPrintedForm:
                    try container.encodeIfPresent(doc, forKey: .doc)
                default:break
                }
            }
        }
    }
    
    public init(from decoder: Decoder) throws {
        self.resolution = .useDefault
        // get from type key
        if let container = try? decoder.container(keyedBy: RecordFieldCodingKeys.self) {
            if let name = try? container.decode(String.self, forKey: .name) {
                self.name = name
            } else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let t = try? container.decodeIfPresent(AvroSchema.self, forKey: .type), let type = t {
                self.type = type
            } else if let type = try container.decodeIfPresent([AvroSchema].self, forKey: .type) {
                self.type = .unionSchema(AvroSchema.UnionSchema(branches: type))
            } else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let order = try? container.decodeIfPresent(String.self, forKey: .order) {
                self.order = order
            }else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let als = try? container.decodeIfPresent(String.self, forKey: .aliases),let alias = als {
                self.aliases = [alias]
            } else if let aliases = try? container.decodeIfPresent([String].self, forKey: .aliases) {
                self.aliases = aliases
            }else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let defaultValue = try? container.decodeIfPresent(String.self, forKey: .defaultValue) {
                self.defaultValue = defaultValue
            }else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let optional = try? container.decodeIfPresent(Bool.self, forKey: .optional) {
                self.optional = optional
            }else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let doc = try? container.decodeIfPresent(String.self, forKey: .doc) {
                self.doc = doc
            }else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
        }else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }
    
    /// filling the empty namespace field for inner named schemas
    mutating func validate(nameSpace: String?, typeMap: [String: AvroSchema]) throws {
        switch type {
        case .unionSchema(var attributes):
            try attributes.validate(typeName: AvroSchema.Types.union.rawValue, typeMap: typeMap, nameSpace: nameSpace)
            type = .unionSchema(attributes)
        case .fixedSchema(var attribute):
            attribute.validateName(typeName: AvroSchema.Types.fixed.rawValue, name: nil, nameSpace: nameSpace)
            type = .fixedSchema(attribute)
        case .enumSchema(var attribute):
            attribute.validateName(typeName: AvroSchema.Types.enums.rawValue, name: nil, nameSpace: nameSpace)
            type = .enumSchema(attribute)
        case .recordSchema(var attribute):
            try attribute.validate(typeName: AvroSchema.Types.record.rawValue, typeMap: typeMap, nameSpace: nameSpace)
            type = .recordSchema(attribute)
        case .unknownSchema(let unknown):
            if var t = typeMap[unknown.name!] {
                try t.validate(typeName: t.getTypeName(), name: nil, nameSpace: nameSpace)
                type = t
            }
        default:break
        }
    }
}

extension AvroSchema.EnumSchema {
    enum EncodeEnumCodingKeys: CodingKey {
        case symbols, doc
    }
    public func encode(to encoder: Encoder) throws {
        try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeEnumCodingKeys.self)
        try container.encode(symbols, forKey: .symbols)
        if encoder.userInfo.isEmpty {return}
        if let userInfo = encoder.userInfo.first {
            if let option = userInfo.value as? AvroSchemaEncodingOption {
                switch option {
                case .PrettyPrintedForm:
                    try container.encodeIfPresent(doc, forKey: .doc)
                default:break
                }
            }
        }
    }
    
}

extension AvroSchema.LogicalType {
    enum EncodeLogicalTypeCodingKeys: CodingKey {
        case logicalType, type
    }
    func encode(to encoder: Encoder) throws {

        switch self {

        case .date:
            var container = encoder.container(keyedBy: EncodeLogicalTypeCodingKeys.self)
            try container.encode(self.rawValue, forKey: .logicalType)
            try container.encode("int", forKey: .type)

        default:
            var container = encoder.singleValueContainer()
            try container.encode(self.rawValue)
        }
    }
}

/*
extension AvroSchema.ProtocolSchema {
    enum EncodeProtocolCodingKeys: CodingKey {
        case types, messages, doc
    }
    enum HeaderCodingKeys: String,CodingKey {
        case name, type = "protocol", namespace, aliases
    }
    /// as Avro spec defined:
    /// [ORDER] Order the appearance of fields of JSON objects as follows:
    /// name, type, fields, symbols, items, values, size.
    /// For example, if an object has type, name, and size fields,
    /// then the name field should appear first, followed by the type and then the size fields.s
    ///
    func encodeHeader2(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: HeaderCodingKeys.self)
        try container.encodeIfPresent(namespace, forKey: .namespace)
        try container.encodeIfPresent(name, forKey: .type)
        //try container.encodeIfPresent(name, forKey: .name)
        try container.encodeIfPresent(aliases, forKey: .aliases)
    }

    public func encode(to encoder: Encoder) throws {
        try encodeHeader2(to: encoder)
        var container = encoder.container(keyedBy: EncodeProtocolCodingKeys.self)
        try container.encodeIfPresent(types, forKey: .types)
        try container.encodeIfPresent(messages, forKey: .messages)
        if encoder.userInfo.isEmpty {return}
        if let userInfo = encoder.userInfo.first {
            if let option = userInfo.value as? AvroSchemaEncodingOption {
                switch option {
                case .PrettyPrintedForm:
                    try container.encodeIfPresent(doc, forKey: .doc)
                default:break
                }
            }
        }
    }
    /// correct the name and type for some guessed schema in decoding step
    /// filling the empty namespace field for inner named schemas
    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        if "protocol" != typeName {
            throw AvroSchemaDecodingError.emptyType
        }
        if let ts = types {
            for t in ts {
                if t.getName() == "" {
                    throw AvroSchemaDecodingError.unnamedSchema
                }
            }
        }
    }
    struct StringCodingKey: CodingKey {
        var intValue: Int?
        
        let stringValue: String
        
        init?(stringValue: String) {
            self.stringValue = stringValue
            self.intValue = Int(stringValue)
        }
        
        init?(intValue: Int) {
            self.stringValue = "\(intValue)"
            self.intValue = intValue
        }
    }
    public init(from decoder: Decoder) throws {
        self.resolution = .useDefault
        if let container = try? decoder.container(keyedBy: CodingKeys.self) {
    
            if let protocolName = try? container.decodeIfPresent(String.self, forKey: .type), let pn = protocolName {
                self.name = pn
                self.type = "potocol"
            } else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let t = try? container.decodeIfPresent(String.self, forKey: .namespace), let type = t {
                self.namespace = type
            } else {
                self.namespace = ""
            }
            if let types = try container.decodeIfPresent([AvroSchema].self, forKey: .types) {
                self.types = types
            } else {
                self.types = []
            }
            if let aliases = try? container.decodeIfPresent(Set<String>.self, forKey: .aliases) {
                self.aliases = aliases
            } else {
                self.aliases = nil
            }
            if let doc = try? container.decodeIfPresent(String.self, forKey: .doc) {
                self.doc = doc
            } else {
                self.doc = ""
            }
            if let nested = try? container.nestedContainer(keyedBy: StringCodingKey.self, forKey: .messages){
            //if let messageMap = try? container.decodeIfPresent(Dictionary<String, AvroSchema.Message>.self,forKey: .messages) {
                var messageSchemaMap = Dictionary<String, AvroSchema.MessageSchema>()
                if let types = self.types {
                    for k in nested.allKeys {
                        let message = try nested.decodeIfPresent(AvroSchema.Message.self, forKey: k)
                        messageSchemaMap[k.stringValue] = try! AvroSchema.MessageSchema.init(from: message!, types:types)
                    }
                }
                self.messages = messageSchemaMap
            } else {
                self.messages = nil
            }
        } else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }
}
*/
