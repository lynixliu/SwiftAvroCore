
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

// MARK: - AvroSchema Codable

extension AvroSchema {

    // MARK: Init from type-name string

    init(type: String) {
        switch type {
        case Types.null.rawValue:    self = .nullSchema
        case Types.boolean.rawValue: self = .booleanSchema
        case Types.int.rawValue:     self = .intSchema(IntSchema())
        case Types.long.rawValue:    self = .longSchema(IntSchema(isLong: true))
        case Types.float.rawValue:   self = .floatSchema
        case Types.double.rawValue:  self = .doubleSchema
        case Types.bytes.rawValue:   self = .bytesSchema(BytesSchema())
        case Types.string.rawValue:  self = .stringSchema(StringSchema())
        default:                     self = .unknownSchema(UnknownSchema(type))
        }
    }

    // MARK: Convenience initialisers

    /// Decodes from a JSON string, handling Avro's primitive short-form
    /// (e.g. `"int"` instead of `{"type":"int"}`).
    public init(schemaJson: String, decoder: JSONDecoder) throws {
        // Short bare strings are Avro's canonical primitive form.
        let maxPrimitiveLength = 10  // len(`"boolean"`) + 1
        if schemaJson.count < maxPrimitiveLength {
            self.init(type: schemaJson.replacingOccurrences(of: "\"", with: ""))
        } else {
            self = try decoder.decode(AvroSchema.self, from: Data(schemaJson.utf8))
        }
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
        case optional
        case union, branches
    }

    // MARK: Decoding

    public init(from decoder: Decoder) throws {
        if let container = try? decoder.container(keyedBy: CodingKeys.self) {
            try self.init(keyedContainer: container, decoder: decoder)
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

    private init(keyedContainer container: KeyedDecodingContainer<CodingKeys>,
                 decoder: Decoder) throws {
        // Discriminate by which keys are present rather than "type" alone,
        // since "type" may be absent or a forward reference.
        if container.contains(.fields) {
            var schema = try RecordSchema(from: decoder)
            try schema.validate(typeName: Types.record.rawValue, name: nil, nameSpace: nil)
            self = schema.type == "error" ? .errorSchema(schema) : .recordSchema(schema)

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
            // {type, name} with exactly two keys is a forward reference.
            if container.contains(.name), container.allKeys.count == 2 {
                let t = try container.decode(String.self, forKey: .type)
                let n = try container.decode(String.self, forKey: .name)
                self = .unknownSchema(UnknownSchema(typeName: t, name: n))
                return
            }
            let type = try container.decode(Types.self, forKey: .type)
            switch type {
            case .null:    self = .nullSchema
            case .boolean: self = .booleanSchema
            case .float:   self = .floatSchema
            case .double:  self = .doubleSchema
            case .string:
                let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                self = lt.map { .stringSchema(StringSchema(logicalType: $0)) }
                    ?? .stringSchema(StringSchema())
            case .int:
                let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                self = lt.map { .intSchema(IntSchema(type: type.rawValue, logicalType: $0)) }
                    ?? .intSchema(IntSchema())
            case .long:
                let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType)
                self = lt.map { .longSchema(IntSchema(type: type.rawValue, logicalType: $0)) }
                    ?? .longSchema(IntSchema(isLong: true))
            case .bytes:
                if let lt = try container.decodeIfPresent(LogicalType.self, forKey: .logicalType) {
                    guard
                        let precision = try container.decodeIfPresent(Int.self, forKey: .precision),
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
            // No discriminating key found — schema JSON is malformed.
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }

    // MARK: Encoding

    /// Encodes the schema, using Avro's canonical primitive short-form where possible.
    public func encode(jsonEncoder: JSONEncoder) throws -> Data {
        switch self {
        case .nullSchema:       return encodePrimitive(Types.null)
        case .booleanSchema:    return encodePrimitive(Types.boolean)
        case .floatSchema:      return encodePrimitive(Types.float)
        case .doubleSchema:     return encodePrimitive(Types.double)
        case .stringSchema(_):  return encodePrimitive(Types.string)
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
        case .stringSchema(let a):
            if a.logicalType != nil { try a.encode(to: encoder) }
            else                    { try container.encode(Types.string) }
        case .intSchema(let a):
            if a.logicalType != nil { try a.encode(to: encoder) }
            else                    { try container.encode(Types.int) }
        case .longSchema(let a):
            if a.logicalType != nil { try a.encode(to: encoder) }
            else                    { try container.encode(Types.long) }
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

// MARK: - Encoding helpers

private func encodePrimitive(_ value: AvroSchema.Types) -> Data {
    Data("\"\(value.rawValue)\"".utf8)
}

private func encodeLogicalType(_ type: AvroSchema.Types,
                               logicalType: AvroSchema.LogicalType) -> Data {
    Data("{\"type\":\"\(type.rawValue)\",\"logicalType\":\"\(logicalType.rawValue)\"}".utf8)
}

// MARK: - NameSchemaProtocol

/// Shared interface for all named Avro schema types (record, enum, fixed, …).
protocol NameSchemaProtocol: Codable {
    var type:       String                      { get set }
    var name:       String?                     { get set }
    var namespace:  String?                     { get set }
    var aliases:    Set<String>?                { get set }
    var resolution: AvroSchema.ResolutionMethod { get set }
}

extension NameSchemaProtocol {

    public func getFullname() -> String {
        guard let n = name else { return type }
        if n.contains(".") { return n }
        if let ns = namespace { return "\(ns).\(n)" }
        return n
    }

    public func getNamespace() -> String? {
        if let n = name, let dot = n.lastIndex(of: ".") { return String(n[..<dot]) }
        return namespace
    }

    func getNamespace(name: String) -> String? { "\(getFullname()).\(name)" }

    func parentNamespace() -> String? {
        guard let ns = namespace, let dot = ns.lastIndex(of: ".") else { return nil }
        return String(ns[..<dot])
    }

    func replaceParentNamespace(name: String?) -> String? {
        guard let n = name, let parent = parentNamespace() else { return namespace }
        return "\(parent).\(n)"
    }

    mutating func setName(name: String?) { self.name = name }

    func encodeHeader(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: NamedAttributesCodingKeys.self)
        try container.encode(getFullname(), forKey: .name)
        try container.encode(type,          forKey: .type)
        guard !encoder.userInfo.isEmpty else { return }
        try container.encodeIfPresent(namespace, forKey: .namespace)
        try container.encodeIfPresent(aliases,   forKey: .aliases)
    }

    mutating func validateName(typeName: String, name: String?, nameSpace: String?) {
        if type != typeName, self.name == nil { self.name = type; type = typeName }
        if let n  = name      { self.name = n }
        if let ns = nameSpace { namespace = ns }
    }
}

// MARK: - Coding key helpers

enum NamedAttributesCodingKeys: CodingKey {
    case name, type, namespace, aliases
}

// MARK: - Nested struct declarations

extension AvroSchema {

    // MARK: RecordSchema

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
            guard let fieldName = field.getName() else { return }
            fields.append(FieldSchema(
                name: fieldName, type: field,
                doc: nil, order: nil, aliases: nil,
                defaultValue: nil, optional: nil
            ))
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
        public let type:  String
        public var items: AvroSchema
        var resolution:   ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey { case type, items }
    }

    // MARK: MapSchema

    public struct MapSchema: Equatable, Codable, Sendable {
        public let type:   String
        public var values: AvroSchema
        var resolution:    ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey { case type, values }
    }

    // MARK: FixedSchema

    public struct FixedSchema: Equatable, NameSchemaProtocol, Sendable {
        public var name:        String?       = nil
        public var namespace:   String?       = nil
        public var type:        String        = "fixed"
        public var aliases:     Set<String>?  = nil
        public var logicalType:  LogicalType?  = nil
        public var size:        Int           = 0
        public var precision:   Int?          = nil
        public var scale:       Int?          = nil
        var resolution:         ResolutionMethod = .useDefault

        private enum CodingKeys: CodingKey {
            case name, type, namespace, aliases, size, logicalType, precision, scale
        }

        func validate() -> Bool {
            guard let logic = logicalType, logic == .decimal else { return true }
            guard let p = precision, p > 0                   else { return false }
            if let s = scale, s > p || s < 0                 { return false }
            guard p > size                                    else { return true }
            // Derive maximum representable decimal precision from the fixed size.
            let bits = (size - 1) << 3
            var realPrecision = (bits / 10) * 3
            if p <= realPrecision { return true }
            let lowerBits = bits % 10
            if lowerBits > 0 {
                var lowerNum = (1 << lowerBits) - 1
                while lowerNum > 10 {
                    lowerNum /= 10
                    realPrecision += 1
                    if p <= realPrecision { return true }
                }
            }
            return p <= realPrecision
        }
    }

    // MARK: BytesSchema

    public struct BytesSchema: Equatable, Codable, Sendable {
        public var type:      String       = "bytes"
        public var logicalType:      LogicalType? = nil
        public var precision: Int?         = nil
        public var scale:     Int?         = nil

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
            name = nil; optional = nil; self.branches = branches
        }

        public init(name: String, optional: String, branches: [AvroSchema]) {
            self.name = name; self.optional = optional; self.branches = branches
        }
    }

    // MARK: IntSchema

    public struct IntSchema: Equatable, Codable, Sendable {
        public let type:      String
        public var logicalType:      LogicalType? = nil

        public init()              { type = Types.int.rawValue }
        public init(isLong: Bool)  { type = isLong ? Types.long.rawValue : Types.int.rawValue }

        init(type: String, logicalType: LogicalType) {
            self.type = type; self.logicalType = logicalType
        }
    }

    // MARK: StringSchema

    public struct StringSchema: Equatable, Codable, Sendable {
        public let type:        String
        public var logicalType:        LogicalType? = nil

        public init() { type = Types.string.rawValue }

        init(logicalType: LogicalType) {
            self.type = Types.string.rawValue; self.logicalType = logicalType
        }
    }

    // MARK: UnknownSchema

    public struct UnknownSchema: NameSchemaProtocol, Sendable {
        public var type:       String
        public var name:       String?
        public var namespace:  String?
        public var aliases:    Set<String>?
        var resolution:        ResolutionMethod

        public init(_ typeName: String) {
            type = ""; name = typeName; namespace = nil; aliases = nil; resolution = .useDefault
        }

        public init(typeName: String, name: String?) {
            type = typeName; self.name = name; namespace = nil; aliases = nil; resolution = .useDefault
        }
    }

    // MARK: ResolutionMethod

    enum ResolutionMethod: Int, Codable { case useDefault, accept, skip }
}

// MARK: - Type alias

public typealias ErrorSchema = AvroSchema.RecordSchema

// MARK: - RecordSchema: Equatable, resolution & encoding

extension AvroSchema.RecordSchema {

    public static func == (lhs: AvroSchema.RecordSchema, rhs: AvroSchema.RecordSchema) -> Bool {
        guard lhs.getFullname() == rhs.getFullname() else { return false }
        guard lhs.fields.count >= rhs.fields.count   else { return false }
        return lhs.fields.allSatisfy { rhs.fields.contains($0) || $0.defaultValue != nil }
    }

    mutating func resolving(from writerRecord: AvroSchema.RecordSchema) throws {
        for field in writerRecord.fields {
            if let idx = fields.firstIndex(of: field) {
                fields[idx].resolution = .accept
                try fields[idx].resolving(from: field)
            } else {
                var skip = field; skip.resolution = .skip
                fields.append(skip)
            }
        }
    }

    enum EncodeRecordCodingKeys: CodingKey { case fields, doc }

    public func encode(to encoder: Encoder) throws {
        try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeRecordCodingKeys.self)
        try container.encode(fields, forKey: .fields)
        guard !encoder.userInfo.isEmpty else { return }
        if isPrettyPrinted(encoder) {
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

// MARK: - FieldSchema: Equatable, resolution & encoding

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
        if isPrettyPrinted(encoder) { try container.encodeIfPresent(doc, forKey: .doc) }
    }

    public init(from decoder: Decoder) throws {
        resolution = .useDefault
        let container = try decoder.container(keyedBy: RecordFieldCodingKeys.self)

        name = try container.decode(String.self, forKey: .name)

        // AvroSchema.init handles both keyed (single type) and unkeyed (union)
        // forms, so the optional + bind covers all valid JSON. If decoding
        // fails or returns nil, the field is malformed.
        guard let schema = try container.decodeIfPresent(AvroSchema.self, forKey: .type) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        type = schema

        // Present-but-nil means wrong type in JSON; treat as error.
        order        = try decodeOptionalField(container, key: .order)
        defaultValue = try decodeOptionalField(container, key: .defaultValue)
        optional     = try decodeOptionalField(container, key: .optional)
        doc          = try decodeOptionalField(container, key: .doc)

        if container.contains(.aliases) {
            if let single = try? container.decodeIfPresent(String.self, forKey: .aliases) {
                aliases = [single]
            } else {
                aliases = try container.decodeIfPresent([String].self, forKey: .aliases)
            }
        } else {
            aliases = nil
        }
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

/// Decodes an optional field, throwing if the key is present but decoding fails.
private func decodeOptionalField<K: CodingKey, T: Decodable>(
    _ container: KeyedDecodingContainer<K>, key: K
) throws -> T? {
    guard container.contains(key) else { return nil }
    guard let value = try container.decodeIfPresent(T.self, forKey: key) else {
        throw AvroSchemaDecodingError.unknownSchemaJsonFormat
    }
    return value
}

// MARK: - EnumSchema: Equatable & encoding

extension AvroSchema.EnumSchema {

    public static func == (lhs: AvroSchema.EnumSchema, rhs: AvroSchema.EnumSchema) -> Bool {
        guard lhs.getFullname() == rhs.getFullname() else { return false }
        guard lhs.symbols.count >= rhs.symbols.count else { return false }
        return rhs.symbols.allSatisfy { lhs.symbols.contains($0) }
    }

    enum EncodeEnumCodingKeys: CodingKey { case symbols, doc }

    public func encode(to encoder: Encoder) throws {
        try encodeHeader(to: encoder)
        var container = encoder.container(keyedBy: EncodeEnumCodingKeys.self)
        try container.encode(symbols, forKey: .symbols)
        guard !encoder.userInfo.isEmpty else { return }
        if isPrettyPrinted(encoder) { try container.encodeIfPresent(doc, forKey: .doc) }
    }
}

// MARK: - Simple Equatable conformances

extension AvroSchema.ArraySchema {
    public static func == (lhs: AvroSchema.ArraySchema, rhs: AvroSchema.ArraySchema) -> Bool { lhs.items == rhs.items }
}
extension AvroSchema.MapSchema {
    public static func == (lhs: AvroSchema.MapSchema, rhs: AvroSchema.MapSchema) -> Bool { lhs.values == rhs.values }
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
        lhs.type == rhs.type && lhs.logicalType == rhs.logicalType
            && lhs.precision == rhs.precision && lhs.scale == rhs.scale
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
                try r.validate(typeName: AvroSchema.Types.record.rawValue, typeMap: typeMap, nameSpace: nameSpace)
                branches[i] = .recordSchema(r)
            case .errorSchema(var r):
                try r.validate(typeName: AvroSchema.Types.error.rawValue, typeMap: typeMap, nameSpace: nameSpace)
                branches[i] = .errorSchema(r)
            default:
                try branches[i].validate(typeName: typeName, name: nil, nameSpace: nameSpace)
            }

            // All union branches must have unique type names.
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

    private enum Keys: CodingKey { case logicalType, type }

    public func encode(to encoder: Encoder) throws {
        if self == .date {
            var container = encoder.container(keyedBy: Keys.self)
            try container.encode(rawValue, forKey: .logicalType)
            try container.encode("int",    forKey: .type)
        } else {
            var container = encoder.singleValueContainer()
            try container.encode(rawValue)
        }
    }
}

// MARK: - Pretty-print helper

private func isPrettyPrinted(_ encoder: Encoder) -> Bool {
    encoder.userInfo.values.contains { ($0 as? AvroSchemaEncodingOption) == .PrettyPrintedForm }
}
