//
//  AvroDatumReader.swift
//
//  Created by Kacper Kawecki on 24/03/2021.
//  Copyright © 2021 by Kacper Kawecki and the project authors.
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

internal class AvroDatumReader {
    let writerSchema: AvroSchema
    let readerSchema: AvroSchema?

    init(writerSchema: AvroSchema, readerSchema: AvroSchema? = nil) {
        self.writerSchema = writerSchema
        self.readerSchema = readerSchema
    }

    func read(decoder: AvroBinaryReader) throws -> AvroDatum {
        return try readData(
                writerSchema: writerSchema,
                readerSchema: readerSchema ?? writerSchema,
                decoder: decoder
        )
    }


    func readData(writerSchema: AvroSchema, readerSchema: AvroSchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        //TODO: match schemas

        if case AvroSchema.unionSchema(let readerUnionSchema) = readerSchema {
            if case AvroSchema.unionSchema(_) = writerSchema {
                // everything is fine
            } else {
                guard readerUnionSchema.branches.contains(writerSchema) else {
                    throw AvroSchemaResolutionError.SchemaMismatch
                }
            }
        }

        switch writerSchema {
        case .nullSchema :
            decoder.readNull()
            return .primitive(.null)
        case .booleanSchema:
            return .primitive(.boolean(try decoder.readBoolean()))
        case .stringSchema(let schema):
            return try packString(schema: schema, value: decoder.readString())
        case .intSchema(let schema):
            return try packInt(schema: schema, value: decoder.readInt())
        case .longSchema(let schema):
            return try packLong(schema: schema, value: decoder.readLong())
        case .floatSchema:
            return .primitive(.float(try decoder.readFloat()))
        case .doubleSchema:
            return .primitive(.double(try decoder.readDouble()))
        case .bytesSchema:
            return .primitive(.bytes(try decoder.readBytes()))
        case .fixedSchema(let schema):
            return try readFixed(schema: schema, decoder: decoder)
        case .unionSchema(let schema):
            guard case .unionSchema(let readerUnionSchema) = readerSchema else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return try readUnion(writerSchema: schema, readerSchema: readerUnionSchema, decoder: decoder)
        case .enumSchema(let schema):
            guard case .enumSchema(let readerEnumSchema) = readerSchema else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return try readEnum(writerSchema: schema, readerSchema: readerEnumSchema, decoder: decoder)
        case .arraySchema(let schema):
            guard case .arraySchema(let readerArraySchema) = readerSchema else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return try readArray(writerSchema: schema, readerSchema: readerArraySchema, decoder: decoder)
        case .mapSchema(let schema):
            guard case .mapSchema(let readerMapSchema) = readerSchema else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return try readMap(writerSchema: schema, readerSchema: readerMapSchema, decoder: decoder)
        case .recordSchema(let schema):
            guard case .recordSchema(let readerRecordSchema) = readerSchema else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
            return try readRecord(writerSchema: schema, readerSchema: readerRecordSchema, decoder: decoder)
        default:
            throw BinaryDecodingError.unknownType
        }

    }

    private func packString(schema: AvroSchema.StringSchema, value: String) throws -> AvroDatum {
        if let logicalType = schema.logicalType {
            switch logicalType {
            case .uuid:
                return .logical(.uuid(value))
            default:
                throw BinaryDecodingError.malformedAvro
            }
        } else {
            return .primitive(.string(value))
        }
    }

    private func packInt(schema: AvroSchema.IntSchema, value: Int64) throws -> AvroDatum {
        if let logicalType = schema.logicalType {
            switch logicalType {
            case .date:
                return .logical(.date(value))
            case .timeMillis:
                return .logical(.timeMillis(value))
            default:
                throw BinaryDecodingError.malformedAvro
            }
        } else {
            return .primitive(.int(value))
        }
    }

    private func packLong(schema: AvroSchema.IntSchema, value: Int64) throws -> AvroDatum {
        if let logicalType = schema.logicalType {
            switch logicalType {
            case .timeMicros:
                return .logical(.timeMicros(value))
            case .timestampMicros:
                return .logical(.timestampMicros(value))
            case .timestampMillis:
                return .logical(.timestampMillis(value))
            case .localTimestampMicros:
                return .logical(.localTimestampMicros(value))
            case .localTimestampMillis:
                return .logical(.localTimestampMillis(value))
            default:
                throw BinaryDecodingError.malformedAvro
            }
        } else {
            return .primitive(.long(value))
        }
    }

    private func readFixed(schema: AvroSchema.FixedSchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        let value = try decoder.read(schema.size)
        if let logicalType = schema.logicalType {
            switch logicalType {
            case .duration:
                return .logical(.duration(value))
            case .decimal:
                return .logical(.decimal(value, precision: schema.precision, scale: schema.size))
            default:
                throw BinaryDecodingError.malformedAvro
            }
        }
        return .primitive(.bytes(value))
    }

    private func readEnum(writerSchema: AvroSchema.EnumSchema, readerSchema: AvroSchema.EnumSchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        let indexOfSymbol = try decoder.readInt()
        var symbol = writerSchema.symbols[Int(indexOfSymbol)]

        if !readerSchema.symbols.contains(symbol) {
            if let defaultValue = readerSchema.defaultValue {
                symbol = defaultValue
            } else {
                throw AvroSchemaResolutionError.SchemaMismatch
            }
        }
        return .primitive(.string(symbol))
    }

    private func readArray(writerSchema: AvroSchema.ArraySchema, readerSchema: AvroSchema.ArraySchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        var readItems: [AvroDatum] = []
        var blockCount = try decoder.readLong()
        while blockCount != 0 {
            if blockCount < 0 {
                blockCount = -blockCount
                let _ = try decoder.readLong() // block size useful for skipping
            }
            for _ in 0..<blockCount {
                readItems.append(try self.readData(writerSchema: writerSchema.items, readerSchema: readerSchema.items, decoder: decoder))
            }
            blockCount = try decoder.readLong()
        }
        return .array(readItems)
    }

    private func readMap(writerSchema: AvroSchema.MapSchema, readerSchema: AvroSchema.MapSchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        var readItems: [String: AvroDatum] = [:]
        var blockCount = try decoder.readLong()
        while blockCount != 0 {
            if blockCount < 0 {
                blockCount = -blockCount
                let _ = try decoder.readLong() // block size useful for skipping
            }
            for _ in 0..<blockCount {
                let key = try decoder.readString()
                readItems[key] = try readData(writerSchema: writerSchema.values, readerSchema: readerSchema.values, decoder: decoder)
            }
            blockCount = try decoder.readLong()
        }
        return .keyed(readItems)
    }

    private func readUnion(writerSchema: AvroSchema.UnionSchema, readerSchema: AvroSchema.UnionSchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        let indexOfSchema = try Int(decoder.readLong())
        guard indexOfSchema >= 0 && indexOfSchema < writerSchema.branches.count else {
            throw BinaryDecodingError.indexOutOfRange
        }
        let selectedSchema = writerSchema.branches[indexOfSchema]

        return try readData(writerSchema: selectedSchema, readerSchema: .unionSchema(readerSchema), decoder: decoder)
    }

    private func readRecord(writerSchema: AvroSchema.RecordSchema, readerSchema: AvroSchema.RecordSchema, decoder: AvroBinaryReader) throws -> AvroDatum {
        let readersFields = readerSchema.fieldsMap()
        let readersAliases = readerSchema.fieldsAliasesMap()
        var readRecord: [String : AvroDatum] = [:]

        for field in writerSchema.fields {
            let fieldName = field.name
            if let readersField = readersFields[fieldName] {
                readRecord[fieldName] = try readData(writerSchema: field.type, readerSchema: readersField.type, decoder: decoder)
            } else if let readersField = readersAliases[fieldName] {
                readRecord[readersField.name] = try readData(writerSchema: field.type, readerSchema: readersField.type, decoder: decoder)
            } else {
                try skipData(schema: field.type, decoder: decoder)
            }
        }

        // Set default
        for field in readerSchema.fields {
            if readRecord[field.name] != nil {
                continue
            }
            guard let defaultValue = field.defaultValue else {
                throw BinaryDecodingError.missingDefaultValue
            }
            readRecord[field.name] = try readDefault(schema: field.type, value: defaultValue)
        }
        return .keyed(readRecord)
    }

    private func readDefault(schema: AvroSchema, value: String) throws -> AvroDatum {
        switch schema {
        case .nullSchema:
            return .primitive(.null)
        case .booleanSchema:
            return .primitive(.boolean(Bool(value)!))
        case .longSchema:
            return .primitive(.long(Int64(value)!))
        case .intSchema:
            return .primitive(.int(Int64(value)!))
        case .enumSchema, .stringSchema:
            return .primitive(.string(value))
        case .fixedSchema, .bytesSchema:
            if let data = value.data(using: .utf8) {
                return .primitive(.bytes([UInt8](data)))
            } else {
                throw BinaryDecodingError.failedReadingDefaultValue
            }
        default:
            throw BinaryDecodingError.failedReadingDefaultValue
        }
    }

    private func skipData(schema: AvroSchema, decoder: AvroBinaryReader) throws {
        switch schema {
        case .nullSchema:
            decoder.skipNull()
        case .booleanSchema:
            try decoder.skipBoolean()
        case .intSchema(_):
            try decoder.skipInt()
        case .longSchema(_):
            try decoder.skipLong()
        case .floatSchema:
            try decoder.skipFloat()
        case .doubleSchema:
            try decoder.skipDouble()
        case .bytesSchema(_):
            try decoder.skipBytes()
        case .stringSchema:
            try decoder.skipString()
        case .recordSchema(let schema):
            try skipRecord(schema: schema, decoder: decoder)
        case .enumSchema(_):
            try decoder.skipInt()
        case .arraySchema(let schema):
            try skipArray(schema: schema, decoder: decoder)
        case .mapSchema(let schema):
            try skipMap(schema: schema, decoder: decoder)
        case .unionSchema(let schema):
            try skipUnion(schema: schema, decoder: decoder)
        case .fixedSchema(let schema):
            try skipFixed(schema: schema, decoder: decoder)
        case .fieldsSchema(let schemas):
            for schema in schemas {
                try skipData(schema: schema.type, decoder: decoder)
            }
        case .fieldSchema(let schema):
            try skipData(schema: schema.type, decoder: decoder)
        default:
            throw BinaryDecodingError.unknownType
        }
    }

    private func skipFixed(schema: AvroSchema.FixedSchema, decoder: AvroBinaryReader) throws {
        try decoder.skip(schema.size)
    }

    private func skipUnion(schema: AvroSchema.UnionSchema, decoder: AvroBinaryReader) throws {
        let index = try Int(decoder.readLong())
        try skipData(schema: schema.branches[index], decoder: decoder)
    }

    private func skipArray(schema: AvroSchema.ArraySchema, decoder: AvroBinaryReader) throws {
        try skipBlocks(decoder: decoder) {
            try skipData(schema: schema.items, decoder: decoder)
        }
    }

    private func skipMap(schema: AvroSchema.MapSchema, decoder: AvroBinaryReader) throws {
        try skipBlocks(decoder: decoder) {
            try decoder.skipString()
            try skipData(schema: schema.values, decoder: decoder)
        }
    }

    private func skipRecord(schema: AvroSchema.RecordSchema, decoder: AvroBinaryReader) throws {
        for field in schema.fields {
            try skipData(schema: field.type, decoder: decoder)
        }
    }

    private func skipBlocks(decoder: AvroBinaryReader, skipItem: () throws -> Void) throws {
        var blockCount = try decoder.readLong()
        while blockCount != 0 {
            if blockCount < 0 {
                let blockSize = try decoder.readLong()
                try decoder.skip(blockSize)
            } else {
                for _ in 0..<blockCount {
                    try skipItem()
                }
            }
            blockCount = try decoder.readLong()
        }
    }

}
