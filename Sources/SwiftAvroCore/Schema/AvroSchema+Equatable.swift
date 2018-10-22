//
//  AvroClient/AvroSchema+Equatable.swift
//
//  Created by Yang Liu on 30/08/18.
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

extension AvroSchema {
    /// resolve from writter's local schema
    mutating func resolving(from schema: AvroSchema) throws {
        guard self == schema else {
            if try resolvingDifferent(from: schema) {
                return
            }
            throw AvroSchemaResolutionError.SchemaMismatch
        }
        switch self {
        case .recordSchema(var resolvingRecord):
            let writterRecord = schema.getRecord()!
            try resolvingRecord.resolving(from: writterRecord)
            self = .recordSchema(writterRecord)
        case .fixedSchema(var resolvingFixed):
            resolvingFixed.resolution = .accept
            self = .fixedSchema(resolvingFixed)
        case .enumSchema(var resolvingEnum):
            resolvingEnum.resolution = .accept
            self = .enumSchema(resolvingEnum)
        case .arraySchema(var resolvingArray):
            let writterArray = schema.getArrayItems()!
            try resolvingArray.items.resolving(from: writterArray)
            self = .arraySchema(resolvingArray)
        case .mapSchema(var resolvingMap):
            let writterMap = schema.getMapValues()!
            try resolvingMap.values.resolving(from: writterMap)
            self = .mapSchema(resolvingMap)
        case .unionSchema(var resolvingUnion):
            /// if both are unions:
            /// The first schema in the reader's union that matches the selected writer's union schema is recursively
            /// resolved against it. if none match, an error is signalled.
            let writterUnion = schema.getUnionList()
            for i in 0..<resolvingUnion.branches.count {
                if let index = writterUnion.firstIndex(of: resolvingUnion.branches[i]) {
                    try resolvingUnion.branches[i].resolving(from: writterUnion[index])
                    self = .unionSchema(resolvingUnion)
                    return
                }
            }
            throw AvroSchemaResolutionError.SchemaMismatch
        default:
            return
        }
    }
    
    mutating func resolvingDifferent(from schema: AvroSchema) throws -> Bool {
        switch self {
        /// If reader's is a union, but writer's is not:
        /// The first schema in the reader's union that matches the writer's schema
        /// is recursively resolved against it. If none match, an error is signalled.
        case .unionSchema(var resolvingUnion):
            if let index = resolvingUnion.branches.firstIndex(of: schema) {
                try resolvingUnion.branches[index].resolving(from: schema)
                self = .unionSchema(resolvingUnion)
                return true
            }
        default:
            /// If writer's is a union, but reader's is not:
            /// If the reader's schema matches the selected writer's schema, it is recursively resolved against it.
            /// If they do not match, an error is signalled.
            switch schema {
            case .unionSchema(let writterUnion):
                if let index = writterUnion.branches.firstIndex(of: self) {
                    try self.resolving(from: writterUnion.branches[index])
                    return true
                }
            /// the writer's schema may be promoted to the reader's as follows:
            /// int is promotable to long, float, or double
            case .intSchema:
                switch self {
                case .longSchema, .floatSchema, .doubleSchema:
                    return true
                default: throw AvroSchemaResolutionError.SchemaMismatch
                }
            /// long is promotable to float or double
            case .longSchema:
                switch self {
                case .floatSchema, .doubleSchema:
                    return true
                default: throw AvroSchemaResolutionError.SchemaMismatch
                }
            /// float is promotable to double
            case .floatSchema:
                switch self {
                case .doubleSchema:
                    return true
                default: throw AvroSchemaResolutionError.SchemaMismatch
                }
            /// string is promotable to bytes
            case .stringSchema:
                switch self {
                case .bytesSchema:
                    return true
                default: throw AvroSchemaResolutionError.SchemaMismatch
                }
            /// bytes is promotable to string
            case .bytesSchema(let param):
                switch self {
                case .stringSchema:
                    return true
                case .fixedSchema(let fixed):
                    if param.logicalType == fixed.logicalType,
                        param.precision == fixed.precision,
                        param.scale == fixed.scale {
                        self = schema
                        return true
                    }
                default: throw AvroSchemaResolutionError.SchemaMismatch
                }
            case .fixedSchema(let param):
                switch self {
                case .bytesSchema(let bytes):
                    if param.logicalType == bytes.logicalType,
                            param.precision == bytes.precision,
                        param.scale == bytes.scale {
                        self = schema
                        return true
                    }
                default: throw AvroSchemaResolutionError.SchemaMismatch
                }
            default:
                throw AvroSchemaResolutionError.SchemaMismatch
            }
        }
        throw AvroSchemaResolutionError.SchemaMismatch
    }
}
extension AvroSchema.RecordSchema {
    public static func ==(lhs: AvroSchema.RecordSchema, rhs: AvroSchema.RecordSchema) -> Bool {
        if (lhs.getFullname() != rhs.getFullname()) {return false}
        if lhs.fields.count < rhs.fields.count {return false}
        for field in lhs.fields {
            if !rhs.fields.contains(field) {
                if field.defaultValue == nil {
                    return false
                }
            }
        }
        return true
    }
    
    mutating func resolving(from writterRecord: AvroSchema.RecordSchema) throws {
        for field in writterRecord.fields {
            /// set the resolution to accept if the writter's filed match with reader's field
            /// add the skip field if no writter's field found in reader's record
            /// the useDefault(init value) for reader's field not including the writter's record
            if let index = fields.firstIndex(of: field) {
                fields[index].resolution = .accept
                try fields[index].resolving(from: field)
            } else {
                // if a field is set to skip, all the inner types are skipped
                var skipField = field
                skipField.resolution = .skip
                fields.append(skipField)
            }
        }
    }
}
extension AvroSchema.FieldSchema {
    public static func ==(lhs: AvroSchema.FieldSchema, rhs: AvroSchema.FieldSchema) -> Bool {
        if (lhs.name != rhs.name) {return false}
        if lhs.type != rhs.type {return false}
        return true
    }
    mutating func resolving(from field: AvroSchema.FieldSchema) throws {
        try type.resolving(from: field.type)
    }
}

extension AvroSchema.EnumSchema {
    public static func ==(lhs: AvroSchema.EnumSchema, rhs: AvroSchema.EnumSchema) -> Bool {
        if lhs.getFullname() != rhs.getFullname() {return false}
        if lhs.symbols.count < rhs.symbols.count {return false}
        for symbol in rhs.symbols {
            if !lhs.symbols.contains(symbol) {return false}
        }
        return true
    }
}

extension AvroSchema.ArraySchema {
    public static func ==(lhs: AvroSchema.ArraySchema, rhs: AvroSchema.ArraySchema) -> Bool {
        return (lhs.items == rhs.items)
    }
}

extension AvroSchema.MapSchema {
    public static func ==(lhs: AvroSchema.MapSchema, rhs: AvroSchema.MapSchema) -> Bool {
        return (lhs.values == rhs.values)
    }
}

extension AvroSchema.FixedSchema {
    public static func ==(lhs: AvroSchema.FixedSchema, rhs: AvroSchema.FixedSchema) -> Bool {
        return (lhs.size == rhs.size && lhs.name == rhs.name && lhs.logicalType == rhs.logicalType)
    }
}

extension AvroSchema.IntSchema {
    public static func ==(lhs: AvroSchema.IntSchema, rhs: AvroSchema.IntSchema) -> Bool {
        return (lhs.type == rhs.type && lhs.logicalType == rhs.logicalType)
    }
}

extension AvroSchema.BytesSchema {
    public static func ==(lhs: AvroSchema.BytesSchema, rhs: AvroSchema.BytesSchema) -> Bool {
        return (lhs.type == rhs.type &&
                lhs.logicalType == rhs.logicalType &&
                lhs.precision == rhs.precision &&
                lhs.scale == rhs.scale)
    }
}

extension AvroSchema {
    public static func ==(lhs: AvroSchema, rhs: AvroSchema) -> Bool {
        switch (lhs, rhs) {
        // compare primitive types
        case (.nullSchema, .nullSchema): return true
        case (.booleanSchema, .booleanSchema): return true
        case let (.intSchema(l), .intSchema(r)): return l == r
        case let (.longSchema(l), .longSchema(r)): return l == r
        case (.floatSchema, .floatSchema): return true
        case (.doubleSchema, .doubleSchema): return true
        case let (.bytesSchema(l), .bytesSchema(r)): return l == r
        case (.stringSchema, .stringSchema): return true
        // compare complex types
        case let (.recordSchema(l), .recordSchema(r)): return l == r
        case let (.arraySchema(l), .arraySchema(r)): return l == r
        case let (.mapSchema(l), .mapSchema(r)): return l == r
        case let (.fixedSchema(l), .fixedSchema(r)): return l == r
        case let (.enumSchema(l), .enumSchema(r)): return l == r
        case let (.unionSchema(l), .unionSchema(r)): return l == r
            
        case let (.fieldSchema(l), .fieldSchema(r)): return l == r
        case let (.fieldsSchema(l), .fieldsSchema(r)): return l == r
        // otherwise
        default: return false
        }
    }
    
    public func hash(into hasher: inout Hasher) {
        switch self {
        // set attribute as hasher for primitive types
        case .nullSchema: hasher.combine(Types.null)
        case .booleanSchema: hasher.combine(Types.boolean)
        case .intSchema(let schema): hasher.combine(schema.type)
        case .longSchema(let schema): hasher.combine(schema.type)
        case .floatSchema: hasher.combine(Types.float)
        case .doubleSchema: hasher.combine(Types.double)
        case .bytesSchema(let schema): hasher.combine(schema.type)
        case .stringSchema: hasher.combine(Types.string)
            // set name and namespace as hasher for named types
        ///TODO: should be more strictly
        case .recordSchema(let schema):
            hasher.combine(schema.namespace)
            hasher.combine(schema.name)
        case .arraySchema(let schema):
            hasher.combine(schema.type)
            hasher.combine(schema.items)
        case .mapSchema(let schema):
            hasher.combine(schema.type)
            hasher.combine(schema.values)
        case .fixedSchema(let schema):
            hasher.combine(schema.namespace)
            hasher.combine(schema.name)
        case .enumSchema(let schema):
            hasher.combine(schema.namespace)
            hasher.combine(schema.name)
        // set all inner type as hasher for union
        case .unionSchema(let schema):
            for attribute in schema.branches {
                hasher.combine(attribute)
            }
        case .fieldSchema(let schema):
            hasher.combine(schema.name)
        case .fieldsSchema(let schemas):
            for schema in schemas {
                hasher.combine(schema.name)
            }
        case .invalidSchema:
            hasher.combine(self.hashValue)
        }
    }
    
    public func isNull() -> Bool {
        switch self {
        case .nullSchema: return true
        default: return false
        }
    }
    public func isBoolean() -> Bool {
        switch self {
        case .booleanSchema: return true
        default: return false
        }
    }
    
    public func isInteger() -> Bool {
        switch self {
        case .intSchema, .longSchema: return true
        default: return false
        }
    }
    
    public func isInt() -> Bool {
        switch self {
        case .intSchema: return true
        default: return false
        }
    }
    public func isLong() -> Bool {
        switch self {
        case .longSchema: return true
        default: return false
        }
    }
    public func isFloat() -> Bool {
        switch self {
        case .floatSchema: return true
        default: return false
        }
    }
    public func isDouble() -> Bool {
        switch self {
        case .doubleSchema: return true
        default: return false
        }
    }
    public func isBytes() -> Bool {
        switch self {
        case .bytesSchema: return true
        default: return false
        }
    }
    
    public func isByte() -> Bool {
        switch self {
        case .bytesSchema, .fixedSchema: return true
        default: return false
        }
    }
    
    public func isDecimal() -> Bool {
        switch self {
        case .bytesSchema(let param):
            if let logic = param.logicalType, logic == .decimal {
                return true
            }
        case .fixedSchema(let param):
            if let logic = param.logicalType, logic == .decimal {
                return true
            }
        default: return false
        }
        return false
    }
    
    public func isString() -> Bool {
        switch self {
        case .stringSchema: return true
        default: return false
        }
    }
    public func isRecord() -> Bool {
        switch self {
        case .recordSchema: return true
        default: return false
        }
    }
    public func isArray() -> Bool {
        switch self {
        case .arraySchema: return true
        default: return false
        }
    }
    public func isMap() -> Bool {
        switch self {
        case .mapSchema: return true
        default: return false
        }
    }
    public func isEnum() -> Bool {
        switch self {
        case .enumSchema: return true
        default: return false
        }
    }
    public func isFixed() -> Bool {
        switch self {
        case .fixedSchema: return true
        default: return false
        }
    }
    public func isUnion() -> Bool {
        switch self {
        case .unionSchema: return true
        default: return false
        }
    }
    public func isField() -> Bool {
        switch self {
        case .fieldSchema: return true
        default: return false
        }
    }
    public func isContainer() -> Bool {
        switch self {
        case .arraySchema, .mapSchema, .recordSchema, .unionSchema, .fieldsSchema, .fieldSchema, .fixedSchema: return true
        default: return false
        }
    }
    public func isInvalid() -> Bool {
        switch self {
        case .invalidSchema: return true
        default: return false
        }
    }
}
/// serialized the schema
extension AvroSchema {
    func getSerializedSchema() -> [AvroSchema] {
        var serializedSchema: [AvroSchema] = []
        switch self {
        case .recordSchema(let rec):
            serializedSchema.append(.fieldsSchema(rec.fields))
        case .fieldsSchema(let fields):
            for field in fields {
                serializedSchema.append(.fieldSchema(field))
            }
        case .arraySchema(let array):
            serializedSchema.append(array.items)
        case .mapSchema(let map):
            serializedSchema.append(map.values)
        case .unionSchema:
            serializedSchema.append(self)
        case .fieldSchema(let field):
            serializedSchema.append(field.type)
        default: serializedSchema.append(self)
        }
        return serializedSchema
    }
}
/// get associate value of the schema
extension AvroSchema {
    func getEnumSymbols() -> [String] {
        switch self {
        case .enumSchema(let enums):
            return enums.symbols
        default:
            return []
        }
    }
    
    func getEnumIndex(_ value: String) -> Int? {
        switch self {
        case .enumSchema(let enums):
            return enums.symbols.firstIndex(of: value)
        default: return nil
        }
    }
    func getMapAttribute() -> MapSchema? {
        switch self {
        case .mapSchema(let attr):
            return attr
        default:
            return nil
        }
    }
    func getInt() -> IntSchema? {
        switch self {
        case .intSchema(let int):
            return int
        default:
            return nil
        }
    }
    func getLong() -> IntSchema? {
        switch self {
        case .longSchema(let long):
            return long
        default:
            return nil
        }
    }
    func getBytes() -> BytesSchema? {
        switch self {
        case .bytesSchema(let bytes):
            return bytes
        default:
            return nil
        }
    }
    func getFixedSize() -> Int? {
        switch self {
        case .fixedSchema(let fixed):
            return fixed.size
        default:
            return nil
        }
    }
    func getArrayItems() -> AvroSchema? {
        switch self {
        case .arraySchema(let array):
            return array.items
        default:
            return nil
        }
    }
    func getMapValues() -> AvroSchema? {
        switch self {
        case .mapSchema(let map):
            return map.values
        default:
            return nil
        }
    }
    func getUnionList() -> [AvroSchema] {
        switch self {
        case .unionSchema(let union):
            return union.branches
        default:
            return []
        }
    }
    func getField() -> FieldSchema? {
        switch self {
        case .fieldSchema(let field):
            return field
        default:
            return nil
        }
    }
    func getRecord() -> RecordSchema? {
        switch self {
        case .recordSchema(let record):
            return record
        default:
            return nil
        }
    }
    
    func getRecordInnerTypes() -> [AvroSchema] {
        var innerTypes: [AvroSchema] = []
        switch self {
        case .recordSchema(let record):
            for field in record.fields {
                innerTypes.append(field.type)
            }
            return innerTypes
        default:
            return []
        }
    }
}
