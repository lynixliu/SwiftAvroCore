
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

// implement codable protocol for Avro Schema encoder and decoder
extension AvroSchema  {

    
    init(type: String) throws {
        switch type {
        case "null":
            self = .nullSchema
        case "boolean":
            self = .booleanSchema
        case "int":
            self = .intSchema(IntSchema())
        case "long":
            self = .longSchema(IntSchema(isLong: true))
        case "float":
            self = .floatSchema
        case "double":
            self = .doubleSchema
        case "bytes":
            self = .bytesSchema(BytesSchema())
        case "string":
            self = .stringSchema
        default:
            self = .invalidSchema
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
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
                try self.init(type: schemaJson.replacingOccurrences(of: "\"", with: ""))
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
            let kes = container.allKeys
            /// when parsing json schema, some inner schemas of complex schema may use name or aliases
            /// defined in parent schema or previous brother schema as type, but JSONDecoder decode string
            /// in a deep first order, at this time, the parent's type of current schema cannot be retrieved
            /// whitout extra cache parameter or regression parsing, because the parent is not created yet.
            /// for simplicity, the type of current schema can be guessed from the required field such as:
            /// fields for record, sybmols for enum, items for array, values for map and size for fixed.
            /// the name and type correction and namespace filling are delayed to validate step for naming schemas.
            if container.contains(.fields){
                var param = try RecordSchema(from: decoder)
                param.validate(typeName: Types.record.rawValue)
                if param.type == "error" {
                    self = .errorSchema(param)
                    return
                }
                self = .recordSchema(param)
            } else if container.contains(.symbols){
                var schema = try EnumSchema(from: decoder)
                schema.validate(typeName: Types.enums.rawValue)
                self = .enumSchema(schema)
            } else if container.contains(.items){
                let schema = try ArraySchema(from: decoder)
                self = .arraySchema(schema)
            } else if container.contains(.values){
                let schema = try MapSchema(from: decoder)
                self = .mapSchema(schema)
            } else if container.contains(.size){
                var schema = try FixedSchema(from: decoder)
                schema.validate(typeName: Types.fixed.rawValue)
                self = .fixedSchema(schema)
            } else if container.contains(.protocolName) {
                var schema = try ProtocolSchema(from: decoder)
                try schema.validate(typeName: Types.protocolName.rawValue)
                self = .protocolSchema(schema)
            } else if container.contains(.messages) {
                let schema = try MessageSchema(from: decoder)
                self = .messageSchema(schema)
            } else if container.contains(.type) {
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
                        self = .invalidSchema
                    }
                } else {
                    self = .invalidSchema
                }
            } else {
                    let primitive = try container.decode(String.self, forKey: .type)
                    self = try AvroSchema(type: primitive)
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
                self = try AvroSchema(type: type)
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
        func encodePrimitives(_ value: Types) -> Data? {
            return "\"\(value.rawValue)\"".data(using: .utf8)
        }
        func encodeLogicalType(type: Types, logicalType: LogicalType) -> Data? {
            return "{\"type\":\"\(type.rawValue)\",\"logicalType\":\"\(logicalType.rawValue)\"}".data(using: .utf8)
        }
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
        case .invalidSchema:
            throw EncodingError.invalidValue(self, EncodingError.Context(codingPath: container.codingPath, debugDescription: "Schema type invalid", underlyingError: AvroSchemaEncodingError.invalidSchemaType))
        case .errorSchema(let attribute):
            try attribute.encode(to: encoder)
        case .protocolSchema(let message):
            try message.encode(to: encoder)
        case .messageSchema(let attribute):
           // for request in attribute.request{
             //   try request.encode(to: encoder)
            //}
            try attribute.response.encode(to: encoder)
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
    //func initHeader()
    mutating func validate(typeName: String) {
        if type != typeName, name == nil {
            name = type
            type = typeName
        }
    }
}

extension AvroSchema.ProtocolSchema {
   // enum EncodeProtocolCodingKeys: CodingKey {
    //    case type = "protocol", types, messages, doc
   // }
    /// as Avro spec defined:
    /// [ORDER] Order the appearance of fields of JSON objects as follows:
    /// name, type, fields, symbols, items, values, size.
    /// For example, if an object has type, name, and size fields,
    /// then the name field should appear first, followed by the type and then the size fields.
   /* public func encode(to encoder: Encoder) throws {
       // try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeProtocolCodingKeys.self)
        try container.encode(messages, forKey: .messages)
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
    }*/
    /// correct the name and type for some guessed schema in decoding step
    /// filling the empty namespace field for inner named schemas
    mutating func validate(typeName: String) throws {
        if "protocol" != typeName {
            throw AvroSchemaDecodingError.emptyType
        }
        /*if protocolName == "" {
            protocolName = typeName
        }*/
        if let ts = types {
            for t in ts {
               //guard let _ = t as? NameSchemaProtocol else {
                 
                //throw AvroSchemaDecodingError.unnamedSchema
               // }
                if t.getName() == "" {
                    throw AvroSchemaDecodingError.unnamedSchema
                }
            }
        }
    }
    /*
    public init(from decoder: Decoder) throws {
        self.resolution = .useDefault
        if let container = try? decoder.container(keyedBy: CodingKeys.self) {
    
            if let e = try? container.decodeIfPresent(String.self, forKey: .type), let type = e {
                self.type = type
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
            if let v = try? container.decodeIfPresent(Dictionary<String, AvroSchema.Message>.self, forKey: .messages){
                if let messages = v, let types = self.types {
                    var typeMap = [String:AvroSchema]()
                    for t in types {
                        if let tn = t.getName() {
                            typeMap[tn] = t
                        }
                    }
                    for (k,m) in messages {
                        let mscheme = try AvroSchema.MessageSchema.init(from: m, types: typeMap)
                        self.messages?[k] = mscheme
                    }
                }
            } else {
                self.messages  = Dictionary<String, AvroSchema.MessageSchema>()
            }
            //if container.contains(.messages) {
                //@DictionaryWrapper var messages = try [String:AvroSchema.MessageSchema].init(from: decoder)
                //self.messages = messages
                //let incontainer = try container.decodeIfPresent(DictionaryWrapper<String:AvroSchema.MessageSchema>.self, formKey: .messages)
            /*  if let messages = try? DictionaryWrapper<String, AvroSchema.MessageSchema>.init(from: decoder) {
                self.messages = messages
            } else {
                self.messages = DictionaryWrapper<String, AvroSchema.MessageSchema>()
            }
               for k in incontainer.allKeys {
                    
                }*/
                //self.messages = incontainer
           // }
        } else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }*/
}

extension AvroSchema.MessageSchema {
    enum MessageCodingKeys: CodingKey {
        case request, response, errors, optional, doc
    }
    /// as Avro spec defined:
    /// [ORDER] Order the appearance of fields of JSON objects as follows:
    /// name, type, fields, symbols, items, values, size.
    /// For example, if an object has type, name, and size fields,
    /// then the name field should appear first, followed by the type and then the size fields.
    public func encode(to encoder: Encoder) throws {
       // try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: MessageCodingKeys.self)
        try container.encode(request, forKey: .request)
        try container.encode(response, forKey: .response)
        try container.encode(errors, forKey: .errors)
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
    mutating func validate(schema: AvroSchema) {
       
    }

    init(from message: AvroSchema.Message, types: [String:AvroSchema]) throws {
        self.resolution = message.resolution
        self.doc = message.doc
        var requests = [AvroSchema]()
        if let request = message.request {
            for r in request {
                if let ty = types[r.type]{
                    requests.append(ty)
                }
            }
        }
        self.request = requests
        if let rsp = message.response {
            self.response = types[rsp]
        } else {
            self.response = nil
        }
        var errorSchema: AvroSchema.ErrorSchema? = nil
        if let errs = message.errors {
            for err in errs {
                if let errType = types[err], let errRecord = errType.getRecord() {
                    errorSchema = errRecord.toErrorSchema()
                    break
                }
            }
        }
        self.errors = errorSchema
        self.optional = message.optional
    }
}

extension AvroSchema.Message {
    enum MessageCodingKeys: CodingKey {
        case request, response, errors, optional, doc
    }
    /// as Avro spec defined:
    /// [ORDER] Order the appearance of fields of JSON objects as follows:
    /// name, type, fields, symbols, items, values, size.
    /// For example, if an object has type, name, and size fields,
    /// then the name field should appear first, followed by the type and then the size fields.
    public func encode(to encoder: Encoder) throws {
       // try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: MessageCodingKeys.self)
        //try container.encode(request, forKey: .request)
        try container.encode(response, forKey: .response)
        try container.encode(errors, forKey: .errors)
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
    mutating func validate(schema: AvroSchema) {
       
    }

    init(from decoder: Decoder) throws {
        self.resolution = .useDefault
        if let container = try? decoder.container(keyedBy: MessageCodingKeys.self) {
            self.doc = try container.decodeIfPresent(String.self, forKey: .doc)
            self.request = try container.decodeIfPresent([AvroSchema.RequestType].self, forKey: .request)
            self.response = try container.decodeIfPresent(String.self, forKey: .response)
            self.errors = try container.decodeIfPresent([String].self, forKey: .errors)
            self.optional = try container.decodeIfPresent(Bool.self, forKey: .optional)
        }else{
            self.doc = nil
            self.request = nil
            self.response = nil
            self.errors = nil
            self.optional = nil
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
    mutating func validate(typeName: String) {
        if type != typeName, name == nil {
            name = type
            type = typeName
        }
        for i in 0..<fields.count {
            fields[i].validate(nameSpace: getNamespace())
        }
    }
    
    func toErrorSchema()-> AvroSchema.ErrorSchema {
        return AvroSchema.ErrorSchema.init(name: self.name, namespace: self.namespace, type: "error", fields: self.fields, aliases: self.aliases, doc: self.doc, resolution: self.resolution)
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
            if let als = try? container.decodeIfPresent(String.self, forKey: .aliases), let alias = als {
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
    mutating func validate(nameSpace: String?) {
        if let ns = nameSpace {
            validate(nameSpace: ns, schema: type)
        }
    }
    
    /// filling the empty namespace field for inner named schemas
    mutating func validate(nameSpace: String?, schema: AvroSchema) {
        var ns = nameSpace
        
        switch schema {
        case .unionSchema(let attributes):
            for i in 0..<attributes.branches.count {
                validate(nameSpace: ns, schema: attributes.branches[i])
            }
            type = .unionSchema(attributes)
        case .fixedSchema(var attribute):
            if nil == attribute.namespace {
                attribute.namespace = ns
                type = .fixedSchema(attribute)
            } else {
                if ns == nil {
                    ns = attribute.namespace
                }
            }
        case .enumSchema(var attribute):
            if nil == attribute.namespace {
                attribute.namespace = ns
                type = .enumSchema(attribute)
            } else {
                if ns == nil {
                    ns = attribute.namespace
                }
            }
        case .recordSchema(var attribute):
            if nil == attribute.namespace {
                attribute.namespace = ns
                type = .recordSchema(attribute)
            } else {
                if ns == nil {
                    ns = attribute.namespace
                }
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

