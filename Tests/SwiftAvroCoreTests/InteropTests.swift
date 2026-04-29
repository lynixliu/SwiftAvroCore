//
//  InteropTests.swift
//  SwiftAvroCoreTests
//
//  Created by Yang Liu on 2024.
//  Copyright © 2024 柳洋 and the project authors.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

import Foundation
import SwiftAvroCore
import Testing

@Suite("Interop Schema Tests")
struct InteropTests {

    let interopSchemaJson: String

    init() throws {
        let avscPath = "/Users/yangliu/Workspace/avro/SwiftAvroCore/pyavro/interop.avsc"
        self.interopSchemaJson = try String(contentsOfFile: avscPath, encoding: .utf8)
    }

    @Test("Parse interop schema")
    func parseInteropSchema() throws {
        let schema = try AvroSchema(schemaJson: interopSchemaJson, decoder: JSONDecoder())
        // Verify it's a record with the expected fields
        if case .recordSchema(let record) = schema {
            #expect(record.name == "Interop")
            #expect(record.namespace == "org.apache.avro")
            #expect(record.fields.count == 14)
        } else {
            Issue.record("Expected record schema")
        }
    }

    @Test("UUID logical type in schema")
    func uuidLogicalTypeSchema() throws {
        let uuidSchemaJson = """
        {
            "type": "record",
            "name": "UuidRecord",
            "fields": [
                {"name": "id", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        """
        let schema = try AvroSchema(schemaJson: uuidSchemaJson, decoder: JSONDecoder())

        // Verify schema has uuid logical type
        if case .recordSchema(let record) = schema,
           case .stringSchema(let stringSchema) = record.fields[0].type {
            #expect(stringSchema.logicalType == .uuid)
        } else {
            Issue.record("Expected string schema with uuid logical type")
        }
    }

    @Test("Date logical type in schema")
    func dateLogicalTypeSchema() throws {
        let dateSchemaJson = """
        {
            "type": "record",
            "name": "DateRecord",
            "fields": [
                {"name": "day", "type": {"type": "int", "logicalType": "date"}}
            ]
        }
        """
        let schema = try AvroSchema(schemaJson: dateSchemaJson, decoder: JSONDecoder())

        // Verify schema has date logical type
        if case .recordSchema(let record) = schema,
           case .intSchema(let intSchema) = record.fields[0].type {
            #expect(intSchema.logicalType == .date)
        } else {
            Issue.record("Expected int schema with date logical type")
        }
    }

    @Test("Timestamp logical types in schema")
    func timestampLogicalTypesSchema() throws {
        let tsSchemaJson = """
        {
            "type": "record",
            "name": "TimestampRecord",
            "fields": [
                {"name": "millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "micros", "type": {"type": "long", "logicalType": "timestamp-micros"}}
            ]
        }
        """
        let schema = try AvroSchema(schemaJson: tsSchemaJson, decoder: JSONDecoder())

        // Verify schemas have correct logical types
        if case .recordSchema(let record) = schema {
            if case .longSchema(let ms) = record.fields[0].type {
                #expect(ms.logicalType == .timestampMillis)
            } else {
                Issue.record("Expected long schema with timestampMillis")
            }
            if case .longSchema(let us) = record.fields[1].type {
                #expect(us.logicalType == .timestampMicros)
            } else {
                Issue.record("Expected long schema with timestampMicros")
            }
        } else {
            Issue.record("Expected record schema")
        }
    }
}
