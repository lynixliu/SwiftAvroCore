//
//  AvroSchemaCodingTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Schema Coding")
struct AvroSchemaCodingTests {

    private func avro() -> SwiftAvroCore { SwiftAvroCore() }

    private func primitiveSchemas(_ type: String) -> [String] {
        [#"{"type":"\#(type)"}"#, #""\#(type)""#]
    }

    private func logicalSchema(type: String, logicalType: String) -> String {
        #"{"type":"\#(type)","logicalType":"\#(logicalType)"}"#
    }

    // MARK: - Primitives

    @Test("Null schema encodes and decodes")
    func null() throws {
        let samples = primitiveSchemas("null")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isNull()); #expect(schemaShort.isNull())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("Boolean schema encodes and decodes")
    func boolean() throws {
        let samples = primitiveSchemas("boolean")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isBoolean()); #expect(schemaShort.isBoolean())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("Int schema encodes and decodes")
    func int() throws {
        let samples = primitiveSchemas("int")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isInt()); #expect(schemaShort.isInt())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("Date logical type schema encodes and decodes")
    func date() throws {
        let sample  = logicalSchema(type: "int", logicalType: "date")
        let a       = avro()
        let schema  = try #require(a.decodeSchema(schema: sample))
        let encoded = try a.encodeSchema(schema: schema)
        #expect(schema.isInt())
        #expect(encoded == sample.data(using: .utf8)!)
    }

    @Test("time-millis logical type schema encodes and decodes")
    func millisecond() throws {
        let sample  = logicalSchema(type: "int", logicalType: "time-millis")
        let a       = avro()
        let schema  = try #require(a.decodeSchema(schema: sample))
        let encoded = try a.encodeSchema(schema: schema)
        #expect(schema.isInt())
        #expect(encoded == sample.data(using: .utf8)!)
    }

    @Test("Long schema encodes and decodes")
    func long() throws {
        let samples = primitiveSchemas("long")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isLong()); #expect(schemaShort.isLong())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("time-micros logical type schema encodes and decodes")
    func microsecond() throws {
        let sample  = logicalSchema(type: "long", logicalType: "time-micros")
        let a       = avro()
        let schema  = try #require(a.decodeSchema(schema: sample))
        let encoded = try a.encodeSchema(schema: schema)
        #expect(schema.isLong())
        #expect(encoded == sample.data(using: .utf8)!)
    }

    @Test("timestamp-millis logical type schema encodes and decodes")
    func timestampMilli() throws {
        let sample  = logicalSchema(type: "long", logicalType: "timestamp-millis")
        let a       = avro()
        let schema  = try #require(a.decodeSchema(schema: sample))
        let encoded = try a.encodeSchema(schema: schema)
        #expect(schema.isLong())
        #expect(encoded == sample.data(using: .utf8)!)
    }

    @Test("timestamp-micros logical type schema encodes and decodes")
    func timestampMicro() throws {
        let sample  = logicalSchema(type: "long", logicalType: "timestamp-micros")
        let a       = avro()
        let schema  = try #require(a.decodeSchema(schema: sample))
        let encoded = try a.encodeSchema(schema: schema)
        #expect(schema.isLong())
        #expect(encoded == sample.data(using: .utf8)!)
    }

    @Test("Float schema encodes and decodes")
    func float() throws {
        let samples = primitiveSchemas("float")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isFloat()); #expect(schemaShort.isFloat())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("Double schema encodes and decodes")
    func double() throws {
        let samples = primitiveSchemas("double")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isDouble()); #expect(schemaShort.isDouble())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("String schema encodes and decodes")
    func string() throws {
        let samples = primitiveSchemas("string")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isString()); #expect(schemaShort.isString())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("Bytes schema encodes and decodes")
    func bytes() throws {
        let samples = primitiveSchemas("bytes")
        let a = avro()
        let schema      = try #require(a.decodeSchema(schema: samples[0]))
        let schemaShort = try #require(a.decodeSchema(schema: samples[1]))
        let encoded     = try a.encodeSchema(schema: schema)
        #expect(schema.isBytes()); #expect(schemaShort.isBytes())
        #expect(encoded == samples[1].data(using: .utf8)!)
    }

    @Test("Decimal bytes logical type encodes and decodes")
    func logicDecimalBytes() throws {
        let sample    = #"{"scale":2,"precision":4,"type":"bytes","logicalType":"decimal"}"#
        let a         = avro()
        let schema    = try #require(a.decodeSchema(schema: sample))
        let encoded   = try a.encodeSchema(schema: schema)
        let newSchema = try #require(a.decodeSchema(schema: encoded))
        #expect(schema.isBytes()); #expect(newSchema.isBytes()); #expect(newSchema.isDecimal())
    }

    // MARK: - Complex

    @Test("Enum schema decodes correctly")
    func enumSchema() throws {
        let sample = #"{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS","CLUBS"]}"#
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: sample))
        #expect(schema.isEnum())
        guard case .enumSchema(let attr) = schema else { Issue.record("Not enum"); return }
        #expect(attr.name    == "Suit")
        #expect(attr.symbols == ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"])
    }

    @Test("Array schema decodes correctly")
    func arraySchema() throws {
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        #expect(schema.isArray())
        guard case .arraySchema(let attr) = schema else { Issue.record("Not array"); return }
        #expect(attr.items.isString())
    }

    @Test("Map schema decodes correctly")
    func mapSchema() throws {
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: #"{"type":"map","values":"long"}"#))
        #expect(schema.isMap())
        guard case .mapSchema(let attr) = schema else { Issue.record("Not map"); return }
        #expect(attr.values.isLong())
    }

    @Test("Fixed schema decodes correctly")
    func fixed() throws {
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: #"{"type":"fixed","size":16,"name":"md5"}"#))
        #expect(schema.isFixed())
        guard case .fixedSchema(let attr) = schema else { Issue.record("Not fixed"); return }
        #expect(attr.size == 16)
        #expect(attr.name == "md5")
    }

    @Test("Decimal fixed logical type encodes and decodes")
    func logicDecimalFixed() throws {
        let sample    = #"{"scale":2,"precision":4,"type":"fixed","logicalType":"decimal","size":3}"#
        let a         = avro()
        let schema    = try #require(a.decodeSchema(schema: sample))
        let encoded   = try a.encodeSchema(schema: schema)
        let newSchema = try #require(a.decodeSchema(schema: encoded))
        #expect(schema.isFixed()); #expect(newSchema.isFixed()); #expect(newSchema.isDecimal())
    }

    @Test("Duration schema decodes correctly")
    func durationSchema() throws {
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: #"{"type":"fixed","logicalType":"duration","size":12,"name":"mmddyy"}"#))
        #expect(schema.isFixed())
        guard case .fixedSchema(let attr) = schema else { Issue.record("Not fixed"); return }
        #expect(attr.size == 12)
        #expect(attr.name == "mmddyy")
        #expect(attr.logicalType?.rawValue == "duration")
    }

    @Test("Union schema decodes correctly")
    func union() throws {
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: #"["null",{"type":"fixed","size":16,"name":"md5"},"long"]"#))
        #expect(schema.isUnion())
    }

    @Test("Complex record schema decodes correctly with all field types")
    func record() throws {
        let sample = """
        {"type":"record","name":"Json","namespace":"org.apache.avro.data","fields":[
          {"name":"clientHash","type":{"type":"fixed","name":"MD5","size":16}},
          {"name":"clientProtocol","type":["null","string"]},
          {"name":"serverHash","type":"MD5"},
          {"name":"meta","type":["null",{"type":"map","values":"bytes"}]},
          {"name":"value","type":["long","double","string","boolean","null",
            {"name":"innerRecord","type":"record","fields":[
              {"name":"inner","type":["string"],"default":"default_value"},
              {"name":"serverHash","type":"MD5"}
            ]},
            {"name":"innerRecordRef","type":"innerRecord"},
            {"type":"array","items":"string"},
            {"type":"map","values":"long"},
            {"name":"Suit","type":"enum","symbols":["SPADES","HEARTS","DIAMONDS","CLUBS"]},
            ["null","string"]
          ]}
        ]}
        """
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: sample))
        #expect(schema.getName() == "Json")
        #expect(schema.getFullname() == "org.apache.avro.data.Json")

        let r = try #require(schema.getRecord())
        #expect(r.fields.count == 5)
        #expect(r.fields[0].name == "clientHash");  #expect(r.fields[0].type.isFixed())
        #expect(r.fields[1].name == "clientProtocol"); #expect(r.fields[1].type.isUnion())
        let union1 = r.fields[1].type.getUnionList()
        #expect(union1[0].isNull()); #expect(union1[1].isString())
        #expect(r.fields[2].name == "serverHash"); #expect(r.fields[2].type.isFixed())
        let metaList = r.fields[3].type.getUnionList()
        #expect(metaList[0].isNull()); #expect(metaList[1].isMap())
        let valueList = r.fields[4].type.getUnionList()
        #expect(valueList.count == 11)
        #expect(valueList[5].getName() == "innerRecord")
        #expect(valueList[5].getRecord()?.fields[0].defaultValue == "default_value")
        #expect(valueList[9].isEnum())
        #expect(valueList[9].getEnumSymbols() == ["SPADES","HEARTS","DIAMONDS","CLUBS"])
    }

    @Test("Nested record schema decodes correctly")
    func nestedRecord() throws {
        let sample = """
        {"name":"Rec","type":"record","fields":[
          {"name":"fel","type":{"name":"Fel","type":"record","fields":[
            {"name":"bea","type":"string"}
          ]}}
        ]}
        """
        let a      = avro()
        let schema = try #require(a.decodeSchema(schema: sample))
        #expect(schema.getName() == "Rec")
        let rec = try #require(schema.getRecord())
        #expect(rec.fields.count == 1)
        #expect(rec.fields[0].name == "fel")
        let fel = try #require(rec.fields[0].type.getRecord())
        #expect(fel.name == "Fel")
        #expect(fel.fields[0].name == "bea")
        #expect(fel.fields[0].type.isString())
    }
}
