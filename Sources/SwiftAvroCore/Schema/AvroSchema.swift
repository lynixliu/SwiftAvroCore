//
//  AvroClient/AvroSchema.swift
//
//  Created by Yang Liu on 24/08/18.
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

// MARK: - AvroSchema

/// The top-level Avro schema type, covering all primitive, complex, and RPC
/// schema variants defined by the Avro specification.
public enum AvroSchema: Codable, Hashable, Sendable {

    // MARK: Primitive types
    case nullSchema
    case booleanSchema
    case intSchema(IntSchema)
    case longSchema(IntSchema)
    case floatSchema
    case doubleSchema
    case bytesSchema(BytesSchema)
    case stringSchema

    // MARK: Complex types
    indirect case recordSchema(RecordSchema)
    indirect case enumSchema(EnumSchema)
    indirect case arraySchema(ArraySchema)
    indirect case mapSchema(MapSchema)
    indirect case unionSchema(UnionSchema)
    case fixedSchema(FixedSchema)

    // MARK: RPC types
    indirect case errorSchema(ErrorSchema)

    // MARK: Private / structural types
    indirect case fieldsSchema([FieldSchema])
    indirect case fieldSchema(FieldSchema)

    // MARK: Sentinel
    case unknownSchema(UnknownSchema)

    // MARK: - Nested enums

    internal enum LogicalType: String, Codable {
        case decimal
        case date
        case timeMillis      = "time-millis"
        case timeMicros      = "time-micros"
        case timestampMillis = "timestamp-millis"
        case timestampMicros = "timestamp-micros"
        case duration
    }

    internal enum Types: String, Codable {
        // Primitives
        case null, boolean, int, long, float, double, bytes, string
        // Complex
        case record, enums = "enum", array, map, union, fixed
        // RPC
        case protocolName = "protocol", message, errors
        // Private
        case field, error
        // Sentinel
        case invalid
    }

    // MARK: - Init

    /// Creates a default (invalid/unknown) schema.
    public init() {
        self = .unknownSchema(UnknownSchema(""))
    }

    // MARK: - Schema lookup

    func findSchema(name: String) -> AvroSchema? {
        switch self {
        case .recordSchema(let schema):
            return schema.findSchema(name: name)
        case .unionSchema(let schema):
            return schema.branches.lazy.compactMap { $0.findSchema(name: name) }.first
        case .enumSchema(let schema):
            return schema.symbols.contains(name) ? self : nil
        default:
            return getName() == name ? self : nil
        }
    }

    // MARK: - Name accessors

    public func getName() -> String? {
        switch self {
        case .nullSchema:          return Types.null.rawValue
        case .booleanSchema:       return Types.boolean.rawValue
        case .floatSchema:         return Types.float.rawValue
        case .doubleSchema:        return Types.double.rawValue
        case .stringSchema:        return Types.string.rawValue
        case .intSchema(let p):    return p.logicalType?.rawValue ?? Types.int.rawValue
        case .longSchema(let p):   return p.logicalType?.rawValue ?? Types.long.rawValue
        case .bytesSchema(let p):  return p.logicalType?.rawValue ?? Types.bytes.rawValue
        case .recordSchema(let p): return p.name
        case .enumSchema(let p):   return p.name
        case .arraySchema(let p):  return p.type
        case .mapSchema(let p):    return p.type
        case .unionSchema:         return Types.union.rawValue
        case .fixedSchema(let p):  return p.name
        case .fieldsSchema:        return "fields"
        case .fieldSchema(let p):  return p.name
        case .errorSchema(let p):  return p.name
        default:                   return nil
        }
    }

    public func getFullname() -> String? {
        switch self {
        case .recordSchema(let p):  return p.getFullname()
        case .enumSchema(let p):    return p.getFullname()
        case .arraySchema(let p):   return p.items.getFullname()
        case .mapSchema(let p):     return p.values.getFullname()
        case .fixedSchema(let p):   return p.getFullname()
        case .errorSchema(let p):   return p.getFullname()
        default:                    return getName()
        }
    }

    public func getTypeName() -> String {
        switch self {
        case .nullSchema:          return Types.null.rawValue
        case .booleanSchema:       return Types.boolean.rawValue
        case .floatSchema:         return Types.float.rawValue
        case .doubleSchema:        return Types.double.rawValue
        case .stringSchema:        return Types.string.rawValue
        case .intSchema(let p):    return p.logicalType?.rawValue ?? Types.int.rawValue
        case .longSchema(let p):   return p.logicalType?.rawValue ?? Types.long.rawValue
        case .bytesSchema(let p):  return p.logicalType?.rawValue ?? Types.bytes.rawValue
        case .recordSchema(let p): return p.type
        case .enumSchema(let p):   return p.type
        case .arraySchema(let p):  return p.type
        case .mapSchema(let p):    return p.type
        case .unionSchema:         return Types.union.rawValue
        case .fixedSchema(let p):  return p.type
        case .fieldsSchema:        return "fields"
        case .fieldSchema:         return Types.field.rawValue
        case .errorSchema(let p):  return p.type
        default:                   return Types.invalid.rawValue
        }
    }

    // MARK: - Type predicates

    public func isNull()      -> Bool { if case .nullSchema    = self { return true }; return false }
    public func isBoolean()   -> Bool { if case .booleanSchema = self { return true }; return false }
    public func isInt()       -> Bool { if case .intSchema     = self { return true }; return false }
    public func isLong()      -> Bool { if case .longSchema    = self { return true }; return false }
    public func isFloat()     -> Bool { if case .floatSchema   = self { return true }; return false }
    public func isDouble()    -> Bool { if case .doubleSchema  = self { return true }; return false }
    public func isString()    -> Bool { if case .stringSchema  = self { return true }; return false }
    public func isRecord()    -> Bool { if case .recordSchema  = self { return true }; return false }
    public func isArray()     -> Bool { if case .arraySchema   = self { return true }; return false }
    public func isMap()       -> Bool { if case .mapSchema     = self { return true }; return false }
    public func isEnum()      -> Bool { if case .enumSchema    = self { return true }; return false }
    public func isFixed()     -> Bool { if case .fixedSchema   = self { return true }; return false }
    public func isUnion()     -> Bool { if case .unionSchema   = self { return true }; return false }
    public func isField()     -> Bool { if case .fieldSchema   = self { return true }; return false }
    public func isUnknown()   -> Bool { if case .unknownSchema = self { return true }; return false }

    public func isInteger() -> Bool {
        switch self {
        case .intSchema, .longSchema: return true
        default:                      return false
        }
    }

    public func isBytes() -> Bool {
        if case .bytesSchema = self { return true }; return false
    }

    /// Returns `true` for both `bytes` and `fixed` schemas.
    public func isByte() -> Bool {
        switch self {
        case .bytesSchema, .fixedSchema: return true
        default:                         return false
        }
    }

    public func isDecimal() -> Bool {
        switch self {
        case .bytesSchema(let p): return p.logicalType == .decimal
        case .fixedSchema(let p): return p.logicalType == .decimal
        default:                  return false
        }
    }

    public func isNamed() -> Bool {
        switch self {
        case .recordSchema, .enumSchema, .fixedSchema: return true
        default:                                       return false
        }
    }

    public func isContainer() -> Bool {
        switch self {
        case .arraySchema, .mapSchema, .recordSchema,
             .unionSchema, .fieldsSchema, .fieldSchema, .fixedSchema:
            return true
        default:
            return false
        }
    }

    // MARK: - Associated-value accessors

    func getEnumSymbols() -> [String] {
        guard case .enumSchema(let e) = self else { return [] }
        return e.symbols
    }

    func getEnumIndex(_ value: String) -> Int? {
        guard case .enumSchema(let e) = self else { return nil }
        return e.symbols.firstIndex(of: value)
    }

    func getMapAttribute() -> MapSchema? {
        guard case .mapSchema(let a) = self else { return nil }
        return a
    }

    func getInt() -> IntSchema? {
        guard case .intSchema(let s) = self else { return nil }
        return s
    }

    func getLong() -> IntSchema? {
        guard case .longSchema(let s) = self else { return nil }
        return s
    }

    func getBytes() -> BytesSchema? {
        guard case .bytesSchema(let s) = self else { return nil }
        return s
    }

    func getFixedSize() -> Int? {
        guard case .fixedSchema(let s) = self else { return nil }
        return s.size
    }

    func getArrayItems() -> AvroSchema? {
        guard case .arraySchema(let s) = self else { return nil }
        return s.items
    }

    func getMapValues() -> AvroSchema? {
        guard case .mapSchema(let s) = self else { return nil }
        return s.values
    }

    func getUnionList() -> [AvroSchema] {
        guard case .unionSchema(let s) = self else { return [] }
        return s.branches
    }

    func getField() -> FieldSchema? {
        guard case .fieldSchema(let s) = self else { return nil }
        return s
    }

    func getRecord() -> RecordSchema? {
        guard case .recordSchema(let s) = self else { return nil }
        return s
    }

    func getRecordInnerTypes() -> [AvroSchema] {
        guard case .recordSchema(let r) = self else { return [] }
        return r.fields.map(\.type)
    }

    func getError() -> ErrorSchema? {
        guard case .errorSchema(let s) = self else { return nil }
        return s
    }

    // MARK: - Serialization helper

    func getSerializedSchema() -> [AvroSchema] {
        switch self {
        case .recordSchema(let r):  return r.fields.map { .fieldSchema($0) }
        case .arraySchema(let a):   return [a.items]
        case .mapSchema(let m):     return [m.values]
        case .unionSchema:          return [self]
        case .fieldSchema(let f):   return [f.type]
        default:                    return [self]
        }
    }
}

// MARK: - Schema resolution

extension AvroSchema {

    /// Resolves this (reader) schema against `schema` (writer), mutating `self`
    /// to reflect resolution decisions per the Avro specification.
    public mutating func resolving(from schema: AvroSchema) throws {
        guard self == schema else {
            if try resolvingDifferent(from: schema) { return }
            throw AvroSchemaResolutionError.SchemaMismatch
        }
        switch self {
        case .recordSchema(let resolvingRecord):
            guard let writerRecord = schema.getRecord() else { return }
            var mutable = resolvingRecord
            try mutable.resolving(from: writerRecord)
            self = .recordSchema(writerRecord)

        case .fixedSchema(var f):
            f.resolution = .accept
            self = .fixedSchema(f)

        case .enumSchema(var e):
            e.resolution = .accept
            self = .enumSchema(e)

        case .arraySchema(var a):
            guard let writerItems = schema.getArrayItems() else { return }
            try a.items.resolving(from: writerItems)
            self = .arraySchema(a)

        case .mapSchema(var m):
            guard let writerValues = schema.getMapValues() else { return }
            try m.values.resolving(from: writerValues)
            self = .mapSchema(m)

        case .unionSchema(var u):
            // Both are unions: the first reader branch matching any writer branch wins.
            let writerBranches = schema.getUnionList()
            for i in u.branches.indices {
                if let idx = writerBranches.firstIndex(of: u.branches[i]) {
                    try u.branches[i].resolving(from: writerBranches[idx])
                    self = .unionSchema(u)
                    return
                }
            }
            throw AvroSchemaResolutionError.SchemaMismatch

        default:
            return
        }
    }

    /// Handles cross-type resolution (reader and writer schemas differ).
    /// Returns `true` if resolution succeeded, throws on mismatch.
    mutating func resolvingDifferent(from schema: AvroSchema) throws -> Bool {
        // Reader is a union: match first branch against writer.
        if case .unionSchema(var u) = self {
            if let idx = u.branches.firstIndex(of: schema) {
                try u.branches[idx].resolving(from: schema)
                self = .unionSchema(u)
                return true
            }
            return false
        }

        // Writer is a union: match reader against first matching writer branch.
        if case .unionSchema(let writerUnion) = schema {
            if let idx = writerUnion.branches.firstIndex(of: self) {
                try self.resolving(from: writerUnion.branches[idx])
                return true
            }
            throw AvroSchemaResolutionError.SchemaMismatch
        }

        // Numeric and bytes/string promotions per the Avro spec.
        switch schema {
        case .intSchema:
            switch self {
            case .longSchema, .floatSchema, .doubleSchema: return true
            default: throw AvroSchemaResolutionError.SchemaMismatch
            }
        case .longSchema:
            switch self {
            case .floatSchema, .doubleSchema: return true
            default: throw AvroSchemaResolutionError.SchemaMismatch
            }
        case .floatSchema:
            if case .doubleSchema = self { return true }
            throw AvroSchemaResolutionError.SchemaMismatch

        case .stringSchema:
            if case .bytesSchema = self { return true }
            throw AvroSchemaResolutionError.SchemaMismatch

        case .bytesSchema(let param):
            switch self {
            case .stringSchema: return true
            case .fixedSchema(let fixed)
                where param.logicalType == fixed.logicalType
                   && param.precision   == fixed.precision
                   && param.scale       == fixed.scale:
                self = schema; return true
            default: throw AvroSchemaResolutionError.SchemaMismatch
            }

        case .fixedSchema(let param):
            if case .bytesSchema(let bytes) = self,
               param.logicalType == bytes.logicalType,
               param.precision   == bytes.precision,
               param.scale       == bytes.scale {
                self = schema; return true
            }
            throw AvroSchemaResolutionError.SchemaMismatch

        default:
            throw AvroSchemaResolutionError.SchemaMismatch
        }
    }
}

// MARK: - Validation

extension AvroSchema {

    mutating func validate(typeName: String, name: String?, nameSpace: String?) throws {
        switch self {
        case .recordSchema(var s):
            try s.validate(typeName: Types.record.rawValue, name: name, nameSpace: nameSpace)
            self = .recordSchema(s)
        case .enumSchema(var s):
            s.validateName(typeName: Types.enums.rawValue, name: name, nameSpace: nameSpace)
            self = .enumSchema(s)
        case .arraySchema(var s):
            try s.items.validate(typeName: Types.array.rawValue, name: name, nameSpace: nameSpace)
            self = .arraySchema(s)
        case .mapSchema(var s):
            try s.values.validate(typeName: Types.map.rawValue, name: name, nameSpace: nameSpace)
            self = .mapSchema(s)
        case .unionSchema(var s):
            try s.validate(typeName: Types.union.rawValue, name: name, nameSpace: nameSpace)
            self = .unionSchema(s)
        case .fixedSchema(var s):
            s.validateName(typeName: Types.fixed.rawValue, name: name, nameSpace: nameSpace)
            self = .fixedSchema(s)
        case .errorSchema(var s):
            try s.validate(typeName: Types.error.rawValue, name: name, nameSpace: nameSpace)
            self = .errorSchema(s)
        default:
            return
        }
    }
}
