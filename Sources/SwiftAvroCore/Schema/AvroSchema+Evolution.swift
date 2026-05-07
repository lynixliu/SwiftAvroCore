import Foundation

extension AvroSchema {
    func resolveValue(_ value: Any?, writtenBy writer: AvroSchema) throws -> Any? {
        if exactlyMatches(writer) {
            return value
        }

        if case .unionSchema(let readerUnion) = self {
            return try firstMatch(in: readerUnion.branches) { try $0.resolveValue(value, writtenBy: writer) }
        }
        if case .unionSchema(let writerUnion) = writer {
            return try firstMatch(in: writerUnion.branches) { try self.resolveValue(value, writtenBy: $0) }
        }

        switch (self, writer) {
        case (.longSchema, .intSchema):
            return try int64Value(value)
        case (.floatSchema, .intSchema), (.floatSchema, .longSchema):
            return Float(try doubleValue(value))
        case (.doubleSchema, .intSchema), (.doubleSchema, .longSchema), (.doubleSchema, .floatSchema):
            return try doubleValue(value)
        case (.bytesSchema, .stringSchema):
            return Array((try stringValue(value)).utf8)
        case (.stringSchema, .bytesSchema):
            guard let bytes = value as? [UInt8],
                  let string = String(bytes: bytes, encoding: .utf8) else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return string
        case (.recordSchema(let reader), .recordSchema(let writer)):
            return try reader.resolveValue(value, writtenBy: writer)
        case (.errorSchema(let reader), .errorSchema(let writer)):
            return try reader.resolveValue(value, writtenBy: writer)
        case (.arraySchema(let reader), .arraySchema(let writer)):
            guard let values = value as? [Any] else { throw AvroSchemaResolutionError.SchemaMismatch }
            return try values.map { try reader.items.resolveValue($0, writtenBy: writer.items) as Any }
        case (.mapSchema(let reader), .mapSchema(let writer)):
            guard let values = value as? [String: Any] else { throw AvroSchemaResolutionError.SchemaMismatch }
            return try values.mapValues { try reader.values.resolveValue($0, writtenBy: writer.values) as Any }
        case (.enumSchema(let reader), .enumSchema(let writer)):
            guard let symbol = value as? String else { throw AvroSchemaResolutionError.SchemaMismatch }
            guard writer.symbols.contains(symbol) else { throw AvroSchemaResolutionError.SchemaMismatch }
            if reader.symbols.contains(symbol) { return symbol }
            if let defaultValue = reader.defaultValue, reader.symbols.contains(defaultValue) {
                return defaultValue
            }
            throw AvroSchemaResolutionError.SchemaMismatch
        case (.fixedSchema(let reader), .fixedSchema(let writer)):
            guard nameMatches(reader, writer), reader.size == writer.size else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return value
        case (.bytesSchema(let reader), .fixedSchema(let writer))
            where reader.logicalType == writer.logicalType
                && reader.precision == writer.precision
                && reader.scale == writer.scale:
            return value
        case (.fixedSchema(let reader), .bytesSchema(let writer))
            where reader.logicalType == writer.logicalType
                && reader.precision == writer.precision
                && reader.scale == writer.scale:
            return value
        default:
            throw AvroSchemaResolutionError.SchemaMismatch
        }
    }

    func defaultValue(from json: JSONValue) throws -> Any? {
        switch self {
        case .nullSchema:
            guard json == .null else { throw AvroSchemaResolutionError.SchemaMismatch }
            return nil
        case .booleanSchema:
            guard case .bool(let value) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            return value
        case .intSchema(let schema):
            let n = try jsonInt(json)
            switch schema.logicalType {
            case .date:       return LogicalTypeConverter.decodeDate(Int(n))
            case .timeMillis: return LogicalTypeConverter.decodeTimeMillis(Int32(n))
            default:          return Int32(n)
            }
        case .longSchema(let schema):
            let n = try jsonInt(json)
            switch schema.logicalType {
            case .timestampMillis:  return LogicalTypeConverter.decodeTimestampMillis(n)
            case .timestampMicros:  return LogicalTypeConverter.decodeTimestampMicros(n)
            case .timeMicros:       return LogicalTypeConverter.decodeTimeMicros(n)
            default:                return n
            }
        case .floatSchema:
            return Float(try jsonDouble(json))
        case .doubleSchema:
            return try jsonDouble(json)
        case .stringSchema:
            guard case .string(let value) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            return value
        case .bytesSchema:
            guard case .string(let value) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            return Array(value.utf8)
        case .fixedSchema(let schema):
            guard case .string(let value) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            let bytes = Array(value.utf8)
            guard bytes.count == schema.size else { throw AvroSchemaResolutionError.SchemaMismatch }
            return bytes
        case .enumSchema(let schema):
            guard case .string(let value) = json, schema.symbols.contains(value) else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return value
        case .arraySchema(let schema):
            guard case .array(let values) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            return try values.map { try schema.items.defaultValue(from: $0) as Any }
        case .mapSchema(let schema):
            guard case .object(let values) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            return try values.mapValues { try schema.values.defaultValue(from: $0) as Any }
        case .recordSchema(let schema), .errorSchema(let schema):
            guard case .object(let object) = json else { throw AvroSchemaResolutionError.SchemaMismatch }
            var result: [String: Any] = [:]
            for field in schema.fields {
                if let value = object[field.name] {
                    result[field.name] = try field.type.defaultValue(from: value)
                } else if let defaultValue = field.defaultValue {
                    result[field.name] = try field.type.defaultValue(from: defaultValue)
                } else {
                    throw AvroSchemaResolutionError.WriterFieldMissingWithoutDefaultValue
                }
            }
            return result
        case .unionSchema(let schema):
            guard let branch = schema.branches.first else { throw AvroSchemaResolutionError.SchemaMismatch }
            return try branch.defaultValue(from: json)
        default:
            throw AvroSchemaResolutionError.SchemaMismatch
        }
    }

    private func exactlyMatches(_ writer: AvroSchema) -> Bool {
        switch (self, writer) {
        case (.nullSchema, .nullSchema),
             (.booleanSchema, .booleanSchema),
             (.floatSchema, .floatSchema),
             (.doubleSchema, .doubleSchema):
            return true
        case (.intSchema(let r), .intSchema(let w)):
            return r == w
        case (.longSchema(let r), .longSchema(let w)):
            return r == w
        case (.bytesSchema(let r), .bytesSchema(let w)):
            return r == w
        case (.stringSchema(let r), .stringSchema(let w)):
            return r == w
        case (.fixedSchema(let r), .fixedSchema(let w)):
            return r == w
        default:
            return false
        }
    }
}

extension AvroSchema.RecordSchema {
    fileprivate func resolveValue(_ value: Any?, writtenBy writer: AvroSchema.RecordSchema) throws -> Any? {
        guard let values = value as? [String: Any] else { throw AvroSchemaResolutionError.SchemaMismatch }
        var result: [String: Any] = [:]
        for readerField in fields {
            if let writerField = writer.field(matching: readerField) {
                result[readerField.name] = try readerField.type.resolveValue(values[writerField.name], writtenBy: writerField.type)
            } else if let defaultValue = readerField.defaultValue {
                result[readerField.name] = try readerField.type.defaultValue(from: defaultValue)
            } else {
                throw AvroSchemaResolutionError.WriterFieldMissingWithoutDefaultValue
            }
        }
        return result
    }

    private func field(matching readerField: AvroSchema.FieldSchema) -> AvroSchema.FieldSchema? {
        if let field = fields.first(where: { $0.name == readerField.name }) {
            return field
        }
        return fields.first { writerField in
            readerField.aliases?.contains(writerField.name) == true
        }
    }
}

private func firstMatch(in branches: [AvroSchema], resolve: (AvroSchema) throws -> Any?) throws -> Any? {
    var lastError: Error = AvroSchemaResolutionError.SchemaMismatch
    for branch in branches {
        do { return try resolve(branch) } catch { lastError = error }
    }
    throw lastError
}

private func nameMatches<T: NameSchemaProtocol>(_ reader: T, _ writer: T) -> Bool {
    if reader.getFullname() == writer.getFullname() { return true }
    return reader.aliases?.contains(writer.getFullname()) == true
}

private func int64Value(_ value: Any?) throws -> Int64 {
    switch value {
    case let value as Int64: return value
    case let value as Int32: return Int64(value)
    case let value as Int: return Int64(value)
    case let value as Double: return Int64(value)
    case let value as Float: return Int64(value)
    default: throw AvroSchemaResolutionError.SchemaMismatch
    }
}

private func doubleValue(_ value: Any?) throws -> Double {
    switch value {
    case let value as Double: return value
    case let value as Float: return Double(value)
    case let value as Int64: return Double(value)
    case let value as Int32: return Double(value)
    case let value as Int: return Double(value)
    default: throw AvroSchemaResolutionError.SchemaMismatch
    }
}

private func stringValue(_ value: Any?) throws -> String {
    guard let value = value as? String else { throw AvroSchemaResolutionError.SchemaMismatch }
    return value
}

private func jsonInt(_ value: JSONValue) throws -> Int64 {
    switch value {
    case .int(let value): return value
    case .double(let value): return Int64(value)
    default: throw AvroSchemaResolutionError.SchemaMismatch
    }
}

private func jsonDouble(_ value: JSONValue) throws -> Double {
    switch value {
    case .int(let value): return Double(value)
    case .double(let value): return value
    default: throw AvroSchemaResolutionError.SchemaMismatch
    }
}
