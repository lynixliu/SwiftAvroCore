
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

// MARK: - Codable

extension AvroSchema {

    // MARK: Initialiser from a type-name string

    init(type: String) {
        switch type {
        case Types.null.rawValue:    self = .nullSchema
        case Types.boolean.rawValue: self = .booleanSchema
        case Types.int.rawValue:     self = .intSchema(IntSchema())
        case Types.long.rawValue:    self = .longSchema(IntSchema(isLong: true))
        case Types.float.rawValue:   self = .floatSchema
        case Types.double.rawValue:  self = .doubleSchema
        case Types.bytes.rawValue:   self = .bytesSchema(BytesSchema())
        case Types.string.rawValue:  self = .stringSchema
        default:                     self = .unknownSchema(UnknownSchema(type))
        }
    }

    // MARK: Convenience initialisers

    /// Decodes from a JSON string, handling Avro's primitive short-form
    /// (e.g. `"int"` instead of `{"type":"int"}`).
    public init(schemaJson: String, decoder: JSONDecoder) throws {
        // Avro canonical form: primitives may appear as bare quoted strings.
        // JSONDecoder requires a top-level array or object, so short strings
        // are handled here before falling through to the standard decoder.
        let maxPrimitiveNameLength = 10 // len(`"boolean"`) + 1
        if schemaJson.count < maxPrimitiveNameLength {
            self.init(type: schemaJson.replacingOccurrences(of: "\"", with: ""))
        } else {
            guard let data = schemaJson.data(using: .utf8) else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            try self.init(schema: data, decoder: decoder)
        }
    }

    init(schema: Data, decoder: JSONDecoder) throws {
        self = try decoder.decode(AvroSchema.self, from: schema)
    }

    // MARK: Coding keys

    enum FieldCodingKeys: String, CodingKey {
        case fields, symbols, items, values, size, type, namespace
    }

    private enum CodingKeys: String, CodingKey {
        case type, name, namespace, aliases, doc
        case protocolName = "protocol"
        case items, values, fields, symbols, size, order
        case defaultValue = "default"
        case logicalType, precision, scale
        case error, messages
        case optional               // Avro 2.0 proposal
        case union, branches        // named union proposal
    }

    // MARK: Decoding

    private init(container _: KeyedDecodingContainer<CodingKeys>, decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Infer the schema kind from discriminating keys (fields → record,
        // symbols → enum, etc.) rather than the "type" field which may be
        // absent or point to a forward reference.
        if container.contains(.fields) {
            var param = try RecordSchema(from: decoder)
            try param.validate(typeName: Types.record.rawValue, name: nil, nameSpace: nil)
            self = param.type == "error" ? .errorSchema(param) : .recordSchema(param)

        } else if container.contains(.symbols) {
            var schema = try EnumSchema(from: decoder)
            schema.validateName(typeName: Types.enums.rawValue, name: nil, nameSpace: nil)
            self = .enumSchema(schema)

        } else if container.contains(.items) {
            self = .arraySchema(try ArraySchema(from: decoder))

        } else if container.contains(.values) {
            self = .mapSchema(try MapSchema(from: decoder))

        } else if container.contains(.size) {
            var schema = try FixedSchema(from: decoder)
            schema.validateName(typeName: Types.fixed.rawValue, name: nil, nameSpace: nil)
            self = .fixedSchema(schema)

        } else if container.contains(.branches) {
            var schema = try UnionSchema(from: decoder)
            try schema.validate(typeName: Types.union.rawValue, name: nil, nameSpace: nil)
            self = .unionSchema(schema)

        } else if container.contains(.type) {
            // {"type": T, "name": N} with exactly two keys — a forward reference.
            if container.contains(.name), container.allKeys.count == 2 {
                let t = try container.decode(String.self, forKey: .type)
                let n = try container.decode(String.self, forKey: .name)
                self = .unknownSchema(UnknownSchema(typeName: t, name: n))
                return
            }
            guard let type = try container.decodeIfPresent(Types.self, forKey: .type) else {
                let primitive = try container.decode(String.self, forKey: .type)
                self = AvroSchema(type: primitive)
                return
            }
            switch type {
            case .null:    self = .nullSchema
            case .boolean: self = .booleanSchema
            case .float:   self = .floatSchema
            case .double:  self = .doubleSchema
            case .string:  self = .stringSchema

            case .int:
                let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                self = .intSchema(lt.map { IntSchema(type: type.rawValue, logicalType: $0) } ?? IntSchema())

            case .long:
                let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                self = .longSchema(lt.map { IntSchema(type: type.rawValue, logicalType: $0) } ?? IntSchema(isLong: true))

            case .bytes:
                if let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType) {
                    guard let precision = try container.decodeIfPresent(Int.self, forKey: .precision),
                          let scale     = try container.decodeIfPresent(Int.self, forKey: .scale)
                    else { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }
                    self = .bytesSchema(BytesSchema(logicalType: lt, precision: precision, scale: scale))
                } else {
                    self = .bytesSchema(BytesSchema())
                }

            default:
                self = .unknownSchema(UnknownSchema(type.rawValue))
            }
        } else {
            let primitive = try container.decode(String.self, forKey: .type)
            self = AvroSchema(type: primitive)
        }
    }

    public init(from decoder: Decoder) throws {
        if let container = try? decoder.container(keyedBy: CodingKeys.self) {
            try self.init(container: container, decoder: decoder)
        } else if let single = try? decoder.singleValueContainer() {
            if let type = try? single.decode(String.self) {
                self = AvroSchema(type: type)
            } else if let branches = try? single.decode([AvroSchema].self) {
                self = .unionSchema(UnionSchema(branches: branches))
            } else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
        } else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [],
                    debugDescription: "Unknown Avro schema JSON format",
                    underlyingError: AvroSchemaDecodingError.unknownSchemaJsonFormat
                )
            )
        }
    }

    // MARK: Encoding

    /// Encodes the schema to `Data` using Avro's canonical primitive short-form.
    public func encode(jsonEncoder: JSONEncoder) throws -> Data? {
        switch self {
        case .nullSchema:    return encodePrimitive(Types.null)
        case .booleanSchema: return encodePrimitive(Types.boolean)
        case .floatSchema:   return encodePrimitive(Types.float)
        case .doubleSchema:  return encodePrimitive(Types.double)
        case .stringSchema:  return encodePrimitive(Types.string)
        case .intSchema(let a):
            return a.logicalType.map { encodeLogicalType(.int,  logicalType: $0) }
                ?? encodePrimitive(Types.int)
        case .longSchema(let a):
            return a.logicalType.map { encodeLogicalType(.long, logicalType: $0) }
                ?? encodePrimitive(Types.long)
        case .bytesSchema(let a):
            return a.logicalType != nil ? try jsonEncoder.encode(self) : encodePrimitive(Types.bytes)
        default:
            return try jsonEncoder.encode(self)
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .nullSchema:    try container.encode(Types.null)
        case .booleanSchema: try container.encode(Types.boolean)
        case .floatSchema:   try container.encode(Types.float)
        case .doubleSchema:  try container.encode(Types.double)
        case .stringSchema:  try container.encode(Types.string)

        case .intSchema(let a):
            if let lt = a.logicalType { try container.encode(lt) }
            else                      { try container.encode(Types.int) }

        case .longSchema(let a):
            if let lt = a.logicalType { try container.encode(lt) }
            else                      { try container.encode(Types.long) }

        case .bytesSchema(let a):
            if a.logicalType != nil {
                try a.encode(to: encoder)
                if !a.validate() { throw BinaryEncodingError.invalidDecimal }
            } else {
                try container.encode(Types.bytes)
            }

        case .fixedSchema(let a):
            try a.encode(to: encoder)
            if a.logicalType == .decimal, !a.validate() { throw BinaryEncodingError.invalidDecimal }

        case .recordSchema(let a):  try a.encode(to: encoder)
        case .arraySchema(let a):   try a.encode(to: encoder)
        case .mapSchema(let a):     try a.encode(to: encoder)
        case .enumSchema(let a):    try a.encode(to: encoder)
        case .errorSchema(let a):   try a.encode(to: encoder)
        case .unionSchema(let a):   try container.encode(a)
        case .fieldSchema(let a):   try container.encode(a)
        case .fieldsSchema(let a):  try container.encode(a)

        case .unknownSchema:
            throw EncodingError.invalidValue(
                self,
                EncodingError.Context(
                    codingPath: container.codingPath,
                    debugDescription: "Schema type invalid",
                    underlyingError: AvroSchemaEncodingError.invalidSchemaType
                )
            )
        }
    }
}

// MARK: - Private encoding helpers

private func encodePrimitive(_ value: AvroSchema.Types) -> Data? {
    "\"\(value.rawValue)\"".data(using: .utf8)
}

private func encodeLogicalType(_ type: AvroSchema.Types,
                               logicalType: AvroSchema.LogicalType) -> Data? {
    "{\"type\":\"\(type.rawValue)\",\"logicalType\":\"\(logicalType.rawValue)\"}".data(using: .utf8)
}

// MARK: - NameSchemaProtocol

/// Shared interface for all named Avro schema types (record, enum, fixed, …).
protocol NameSchemaProtocol: Codable {
    var type:       String                      { get set }
    var name:       String?                     { get set }
    var namespace:  String?                     { get set }
    var aliases:    Set<String>?                { get set }
    /// Controls schema-resolution behaviour during decoding.
    var resolution: AvroSchema.ResolutionMethod { get set }
}

extension NameSchemaProtocol {

    /// Returns `"namespace.name"` when both are set, bare `name` when only the
    /// name is present, or `type` as a last resort.
    public func getFullname() -> String {
        guard let n = name else { return type }
        if n.contains(".") { return n }
        if let ns = namespace { return "\(ns).\(n)" }
        return n
    }

    public func getNamespace() -> String? {
        if let n = name, let dotIdx = n.lastIndex(of: ".") {
            return String(n[..<dotIdx])
        }
        return namespace
    }

    /// Returns `"<fullname>.<n>"` — used to build child-field namespaces.
    func getNamespace(name: String) -> String? {
        "\(getFullname()).\(name)"
    }

    /// Returns the parent portion of a dotted namespace (`"a.b.c"` → `"a.b"`).
    func parentNamespace() -> String? {
        guard let ns = namespace, let dotIdx = ns.lastIndex(of: ".") else { return nil }
        return String(ns[..<dotIdx])
    }

    func replaceParentNamespace(name: String?) -> String? {
        guard let n = name, let parent = parentNamespace() else { return namespace }
        return "\(parent).\(n)"
    }

    mutating func setName(name: String?) { self.name = name }

    // MARK: Encoding helper

    func encodeHeader(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: NamedAttributesCodingKeys.self)
        try container.encode(getFullname(), forKey: .name)
        try container.encode(type,          forKey: .type)
        guard !encoder.userInfo.isEmpty else { return }
        try container.encodeIfPresent(namespace, forKey: .namespace)
        try container.encodeIfPresent(aliases,   forKey: .aliases)
    }

    // MARK: Validation helper

    mutating func validateName(typeName: String, name: String?, nameSpace: String?) {
        if type != typeName, self.name == nil {
            self.name = type
            type = typeName
        }
        if let n  = name      { self.name = n }
        if let ns = nameSpace { namespace = ns }
    }
}

// MARK: - NamedAttributesCodingKeys

enum NamedAttributesCodingKeys: CodingKey {
    case name, type, namespace, aliases
}

// MARK: - Nested struct declarations

extension AvroSchema {

    // MARK: RecordSchema

    /// Encodes/decodes an Avro `record` or `error` schema.
    public struct RecordSchema: Equatable, NameSchemaProtocol, Sendable {
        public var name:       String?
        public var namespace:  String?
        public var type:       String
        public var fields:     [FieldSchema]
        public var aliases:    Set<String>?
        public let doc:        String?
        var resolution: ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey {
            case name, type, namespace, aliases, fields, doc
        }

        public mutating func addField(_ field: AvroSchema) {
            guard let fieldName = field.getName() else {
                preconditionFailure("Cannot add a field with no name: \(field)")
            }
            fields.append(
                FieldSchema(name: fieldName, type: field,
                            doc: nil, order: nil, aliases: nil,
                            defaultValue: nil, optional: nil)
            )
        }

        func findSchema(name: String) -> AvroSchema? {
            if name == "fields" { return .fieldsSchema(fields) }
            return fields.first { $0.name == name }?.type
        }
    }

    // MARK: FieldSchema

    public struct FieldSchema: Equatable, Codable, Sendable {
        public let  name:         String
        public var  type:         AvroSchema
        public let  doc:          String?
        public let  order:        String?
        public let  aliases:      [String]?
        public let  defaultValue: String?
        public let  optional:     Bool?
        var  resolution:   ResolutionMethod = .useDefault
    }

    // MARK: EnumSchema

    public struct EnumSchema: Equatable, NameSchemaProtocol, Sendable {
        public var name:       String?
        public var namespace:  String?
        public var type:       String
        public var aliases:    Set<String>?
        public let doc:        String?
        public var symbols:    [String]
        var resolution: ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey {
            case name, type, namespace, aliases, symbols, doc
        }
    }

    // MARK: ArraySchema

    public struct ArraySchema: Equatable, Codable, Sendable {
        public let type:       String
        public var items:      AvroSchema
        var resolution: ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey {
            case type, items
        }
    }

    // MARK: MapSchema

    public struct MapSchema: Equatable, Codable, Sendable {
        public let type:       String
        public var values:     AvroSchema
        var resolution: ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey {
            case type, values
        }
    }

    // MARK: FixedSchema

    public struct FixedSchema: Equatable, NameSchemaProtocol, Sendable {
        public var name:        String?       = nil
        public var namespace:   String?       = nil
        public var type:        String        = "fixed"
        public var aliases:     Set<String>?  = nil
        /// Must be `.duration` or `.decimal` when set; duration requires `size == 12`.
        var logicalType: LogicalType?  = nil
        public var size:        Int           = 0
        public var precision:   Int?          = nil
        public var scale:       Int?          = nil
        var resolution:  ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey {
            case name, type, namespace, aliases, size, logicalType, precision, scale
        }

        func validate() -> Bool {
            guard let logic = logicalType, logic == .decimal else { return true }
            guard let p = precision, p > 0                   else { return false }
            if let s = scale, s > p || s < 0                 { return false }
            guard p > size                                    else { return true }

            // Derive maximum representable decimal precision from the fixed size.
            // TODO: replace Darwin-only log10 with a cross-platform implementation.
            let bits          = (size - 1) << 3
            var realPrecision = (bits / 10) * 3
            if p <= realPrecision { return true }

            let lowerBits = bits % 10
            if lowerBits > 0 {
                // Parentheses are required: `1 << lowerBits - 1` parses as
                // `1 << (lowerBits - 1)` due to Swift operator precedence.
                var lowerNum = (1 << lowerBits) - 1
                while lowerNum > 10 {
                    lowerNum      /= 10
                    realPrecision += 1
                    if p <= realPrecision { return true }
                }
            }
            return p <= realPrecision
        }
    }

    // MARK: BytesSchema

    public struct BytesSchema: Equatable, Codable, Sendable {
        public var type:        String       = "bytes"
        var logicalType: LogicalType? = nil
        public var precision:   Int?         = nil
        public var scale:       Int?         = nil

        public init() {}

        init(logicalType: LogicalType, precision: Int, scale: Int) {
            self.logicalType = logicalType
            self.precision   = precision
            self.scale       = scale
        }

        func validate() -> Bool {
            guard logicalType != nil       else { return true  }
            guard let p = precision, p > 0 else { return false }
            guard let s = scale, s <= p    else { return false }
            return true
        }
    }

    // MARK: UnionSchema

    public struct UnionSchema: Equatable, Codable, Sendable {
        public var name:     String?
        public let optional: String?
        public var branches: [AvroSchema]

        public init(branches: [AvroSchema]) {
            name     = nil
            optional = nil
            self.branches = branches
        }

        public init(name: String, optional: String, branches: [AvroSchema]) {
            self.name     = name
            self.optional = optional
            self.branches = branches
        }
    }

    // MARK: IntSchema

    /// Encodes/decodes an Avro `int` or `long` schema, optionally with a
    /// date/time logical type.
    public struct IntSchema: Equatable, Codable, Sendable {
        public let type:        String
        var logicalType: LogicalType? = nil

        public init() {
            type = Types.int.rawValue
        }

        public init(isLong: Bool) {
            type = isLong ? Types.long.rawValue : Types.int.rawValue
        }

        init(type: String, logicalType: LogicalType) {
            self.type        = type
            self.logicalType = logicalType
        }
    }

    // MARK: UnknownSchema

    public struct UnknownSchema: NameSchemaProtocol, Sendable {
        public var type:       String
        public var name:       String?
        public var namespace:  String?
        public var aliases:    Set<String>?
        var resolution: ResolutionMethod

        public init(_ typeName: String) {
            type       = ""
            name       = typeName
            namespace  = nil
            aliases    = nil
            resolution = .useDefault
        }

        public init(typeName: String, name: String?) {
            type       = typeName
            self.name  = name
            namespace  = nil
            aliases    = nil
            resolution = .useDefault
        }
    }

    // MARK: StringCodingKey

    struct StringCodingKey: CodingKey {
        let stringValue: String
        var intValue:    Int?

        init?(stringValue: String) {
            self.stringValue = stringValue
            self.intValue    = Int(stringValue)
        }

        init?(intValue: Int) {
            self.stringValue = "\(intValue)"
            self.intValue    = intValue
        }
    }

    // MARK: ResolutionMethod

    enum ResolutionMethod: Int, Codable {
        case useDefault
        case accept
        case skip
    }
}

// MARK: - Type alias

public typealias ErrorSchema = AvroSchema.RecordSchema

// MARK: - RecordSchema Equatable, resolution & encoding

extension AvroSchema.RecordSchema {

    public static func == (lhs: AvroSchema.RecordSchema, rhs: AvroSchema.RecordSchema) -> Bool {
        guard lhs.getFullname() == rhs.getFullname() else { return false }
        guard lhs.fields.count >= rhs.fields.count   else { return false }
        return lhs.fields.allSatisfy { field in
            rhs.fields.contains(field) || field.defaultValue != nil
        }
    }

    mutating func resolving(from writerRecord: AvroSchema.RecordSchema) throws {
        for field in writerRecord.fields {
            if let idx = fields.firstIndex(of: field) {
                fields[idx].resolution = .accept
                try fields[idx].resolving(from: field)
            } else {
                var skipField = field
                skipField.resolution = .skip
                fields.append(skipField)
            }
        }
    }

    enum EncodeRecordCodingKeys: CodingKey { case fields, doc }

    public func encode(to encoder: Encoder) throws {
        try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeRecordCodingKeys.self)
        try container.encode(fields, forKey: .fields)
        guard !encoder.userInfo.isEmpty else { return }
        if encoder.userInfo.values.contains(where: { ($0 as? AvroSchemaEncodingOption) == .PrettyPrintedForm }) {
            try container.encodeIfPresent(doc, forKey: .doc)
        }
    }

    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        validateName(typeName: typeName, name: name, nameSpace: nameSpace)
        for i in fields.indices {
            switch fields[i].type {
            case .unknownSchema(let t):
                for j in 0..<i where fields[j].type.getName() == t.name {
                    fields[i].type = fields[j].type
                    let ns: String?
                    switch fields[j].type {
                    case .enumSchema(let e):   ns = e.replaceParentNamespace(name: fields[i].name)
                    case .fixedSchema(let f):  ns = f.replaceParentNamespace(name: fields[i].name)
                    case .recordSchema(let r): ns = r.replaceParentNamespace(name: fields[i].name)
                    default:                   ns = nameSpace
                    }
                    try fields[i].type.validate(typeName: fields[i].type.getTypeName(),
                                                name: t.name, nameSpace: ns)
                    break
                }
            case .unionSchema(var u):
                var typeMap = [String: AvroSchema]()
                for j in 0..<i {
                    typeMap[fields[j].type.getName() ?? "null"] = fields[j].type.getName() != nil
                        ? fields[j].type : .nullSchema
                }
                try u.validate(typeName: typeName, typeMap: typeMap,
                               nameSpace: getNamespace(name: fields[i].name))
                fields[i].type = .unionSchema(u)
            default:
                try fields[i].type.validate(typeName: typeName, name: nil,
                                            nameSpace: getNamespace(name: fields[i].name))
            }
        }
    }

    mutating func validate(typeName: String, typeMap: [String: AvroSchema], nameSpace: String?) throws {
        validateName(typeName: typeName, name: nil, nameSpace: nameSpace)
        for i in fields.indices {
            try fields[i].validate(nameSpace: getNamespace(name: fields[i].name), typeMap: typeMap)
        }
    }
}

// MARK: - FieldSchema Equatable, resolution & encoding

extension AvroSchema.FieldSchema {

    public static func == (lhs: AvroSchema.FieldSchema, rhs: AvroSchema.FieldSchema) -> Bool {
        lhs.name == rhs.name && lhs.type == rhs.type
    }

    mutating func resolving(from field: AvroSchema.FieldSchema) throws {
        try type.resolving(from: field.type)
    }

    enum RecordFieldCodingKeys: String, CodingKey {
        case name, type, order, aliases
        case defaultValue = "default"
        case optional, doc
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: RecordFieldCodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(type, forKey: .type)
        try container.encodeIfPresent(order,        forKey: .order)
        try container.encodeIfPresent(defaultValue, forKey: .defaultValue)
        guard !encoder.userInfo.isEmpty else { return }
        try container.encodeIfPresent(aliases, forKey: .aliases)
        if encoder.userInfo.values.contains(where: { ($0 as? AvroSchemaEncodingOption) == .PrettyPrintedForm }) {
            try container.encodeIfPresent(doc, forKey: .doc)
        }
    }

    public init(from decoder: Decoder) throws {
        resolution = .useDefault
        let container = try decoder.container(keyedBy: RecordFieldCodingKeys.self)

        name = try container.decode(String.self, forKey: .name)

        if let schema = try? container.decodeIfPresent(AvroSchema.self, forKey: .type) {
            type = schema
        } else if let branches = try container.decodeIfPresent([AvroSchema].self, forKey: .type) {
            type = .unionSchema(AvroSchema.UnionSchema(branches: branches))
        } else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }

        order        = container.contains(.order)        ? try container.decodeIfPresent(String.self, forKey: .order)        : ""
        defaultValue = container.contains(.defaultValue) ? try container.decodeIfPresent(String.self, forKey: .defaultValue) : ""
        optional     = container.contains(.optional)     ? try container.decodeIfPresent(Bool.self,   forKey: .optional)     : false
        doc          = container.contains(.doc)          ? try container.decodeIfPresent(String.self, forKey: .doc)          : ""

        if container.contains(.aliases) {
            if let single = try? container.decodeIfPresent(String.self, forKey: .aliases) {
                aliases = [single]
            } else {
                aliases = try container.decodeIfPresent([String].self, forKey: .aliases)
            }
        } else {
            aliases = nil
        }

        // A key present but producing nil means a wrong-type value in the JSON.
        if container.contains(.order)        && order        == nil { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }
        if container.contains(.defaultValue) && defaultValue == nil { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }
        if container.contains(.optional)     && optional     == nil { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }
        if container.contains(.doc)          && doc          == nil { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }
    }

    mutating func validate(nameSpace: String?, typeMap: [String: AvroSchema]) throws {
        switch type {
        case .unionSchema(var u):
            try u.validate(typeName: AvroSchema.Types.union.rawValue, typeMap: typeMap, nameSpace: nameSpace)
            type = .unionSchema(u)
        case .fixedSchema(var s):
            s.validateName(typeName: AvroSchema.Types.fixed.rawValue, name: nil, nameSpace: nameSpace)
            type = .fixedSchema(s)
        case .enumSchema(var s):
            s.validateName(typeName: AvroSchema.Types.enums.rawValue, name: nil, nameSpace: nameSpace)
            type = .enumSchema(s)
        case .recordSchema(var s):
            try s.validate(typeName: AvroSchema.Types.record.rawValue, typeMap: typeMap, nameSpace: nameSpace)
            type = .recordSchema(s)
        case .unknownSchema(let unknown):
            guard let unknownName = unknown.name, var t = typeMap[unknownName] else { break }
            try t.validate(typeName: t.getTypeName(), name: nil, nameSpace: nameSpace)
            type = t
        default: break
        }
    }
}

// MARK: - EnumSchema Equatable & encoding

extension AvroSchema.EnumSchema {

    public static func == (lhs: AvroSchema.EnumSchema, rhs: AvroSchema.EnumSchema) -> Bool {
        guard lhs.getFullname() == rhs.getFullname()  else { return false }
        guard lhs.symbols.count >= rhs.symbols.count  else { return false }
        return rhs.symbols.allSatisfy { lhs.symbols.contains($0) }
    }

    enum EncodeEnumCodingKeys: CodingKey { case symbols, doc }

    public func encode(to encoder: Encoder) throws {
        try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeEnumCodingKeys.self)
        try container.encode(symbols, forKey: .symbols)
        guard !encoder.userInfo.isEmpty else { return }
        if encoder.userInfo.values.contains(where: { ($0 as? AvroSchemaEncodingOption) == .PrettyPrintedForm }) {
            try container.encodeIfPresent(doc, forKey: .doc)
        }
    }
}

// MARK: - Schema-level Equatable conformances

extension AvroSchema.ArraySchema {
    public static func == (lhs: AvroSchema.ArraySchema, rhs: AvroSchema.ArraySchema) -> Bool {
        lhs.items == rhs.items
    }
}

extension AvroSchema.MapSchema {
    public static func == (lhs: AvroSchema.MapSchema, rhs: AvroSchema.MapSchema) -> Bool {
        lhs.values == rhs.values
    }
}

extension AvroSchema.FixedSchema {
    public static func == (lhs: AvroSchema.FixedSchema, rhs: AvroSchema.FixedSchema) -> Bool {
        lhs.size == rhs.size && lhs.name == rhs.name && lhs.logicalType == rhs.logicalType
    }
}

extension AvroSchema.IntSchema {
    public static func == (lhs: AvroSchema.IntSchema, rhs: AvroSchema.IntSchema) -> Bool {
        lhs.type == rhs.type && lhs.logicalType == rhs.logicalType
    }
}

extension AvroSchema.BytesSchema {
    public static func == (lhs: AvroSchema.BytesSchema, rhs: AvroSchema.BytesSchema) -> Bool {
        lhs.type        == rhs.type        &&
        lhs.logicalType == rhs.logicalType &&
        lhs.precision   == rhs.precision   &&
        lhs.scale       == rhs.scale
    }
}

// MARK: - UnionSchema validation

extension AvroSchema.UnionSchema {

    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        for i in branches.indices {
            try branches[i].validate(typeName: typeName, name: name, nameSpace: nameSpace)
        }
    }

    mutating func validate(typeName: String, typeMap: [String: AvroSchema], nameSpace: String?) throws {
        var uniqueMap = [String: AvroSchema]()
        for i in branches.indices {
            switch branches[i] {
            case .unknownSchema(let unknown):
                guard let unknownName = unknown.name else { break }
                if var schema = typeMap[unknownName] {
                    try schema.validate(typeName: schema.getTypeName(),
                                        name: unknownName, nameSpace: nameSpace)
                    branches[i] = schema
                } else {
                    for j in 0..<i where branches[j].getName() == unknown.type {
                        var schema = branches[j]
                        try schema.validate(typeName: schema.getTypeName(),
                                            name: unknownName, nameSpace: nameSpace)
                        branches[i] = schema
                        break
                    }
                }
            case .recordSchema(var r):
                try r.validate(typeName: AvroSchema.Types.record.rawValue,
                               typeMap: typeMap, nameSpace: nameSpace)
                branches[i] = .recordSchema(r)
            case .errorSchema(var r):
                try r.validate(typeName: AvroSchema.Types.error.rawValue,
                               typeMap: typeMap, nameSpace: nameSpace)
                branches[i] = .errorSchema(r)
            default:
                try branches[i].validate(typeName: typeName, name: nil, nameSpace: nameSpace)
            }

            // Avro rule: all union branches must have unique type names.
            if i > 0,
               let existing = uniqueMap[branches[i].getTypeName()],
               existing.getName() == branches[i].getName() {
                throw AvroSchemaDecodingError.typeDuplicateBranchInUnion
            }
            uniqueMap[branches[i].getTypeName()] = branches[i]
        }
    }
}

// MARK: - LogicalType encoding

extension AvroSchema.LogicalType {

    enum EncodeLogicalTypeCodingKeys: CodingKey { case logicalType, type }

    func encode(to encoder: Encoder) throws {
        switch self {
        case .date:
            // date must encode as {"logicalType":"date","type":"int"}.
            var container = encoder.container(keyedBy: EncodeLogicalTypeCodingKeys.self)
            try container.encode(rawValue, forKey: .logicalType)
            try container.encode("int",    forKey: .type)
        default:
            var container = encoder.singleValueContainer()
            try container.encode(rawValue)
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
