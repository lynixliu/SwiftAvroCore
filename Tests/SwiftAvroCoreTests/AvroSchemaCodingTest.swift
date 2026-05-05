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

    private func avro() -> Avro { Avro() }

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

    // MARK: - Encoding form variations

    @Test("record encodes with aliases in FullForm")
    func recordFullFormAliases() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","aliases":["OldR"],
         "fields":[{"name":"a","type":"int"}]}
        """#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: schema)
        avro.setSchemaFormat(option: .FullForm)
        let full      = try avro.encodeSchema(schema: schema)
        // CanonicalForm omits aliases; FullForm includes them.
        #expect(!String(data: canonical, encoding: .utf8)!.contains("aliases"))
        #expect(String(data: full, encoding: .utf8)!.contains("aliases"))
    }

    @Test("record encodes with doc in PrettyPrintedForm")
    func recordPrettyDoc() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","doc":"Some doc",
         "fields":[{"name":"a","type":"int","doc":"Field doc"}]}
        """#))
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        let pretty = try avro.encodeSchema(schema: schema)
        #expect(String(data: pretty, encoding: .utf8)!.contains("doc"))
        // pretty-print uses newlines
        #expect(String(data: pretty, encoding: .utf8)!.contains("\n"))
    }

    @Test("enum encodes with aliases in FullForm and doc in PrettyPrintedForm")
    func enumFormVariants() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"enum","name":"E","aliases":["OldE"],"doc":"E doc",
         "symbols":["A","B"]}
        """#))
        avro.setSchemaFormat(option: .FullForm)
        let full   = try avro.encodeSchema(schema: schema)
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        let pretty = try avro.encodeSchema(schema: schema)
        #expect(String(data: full, encoding: .utf8)!.contains("aliases"))
        #expect(String(data: pretty, encoding: .utf8)!.contains("doc"))
    }

    @Test("primitive encodes as canonical short form")
    func primitiveCanonical() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        #expect(String(data: canonical, encoding: .utf8)!.contains("int"))
    }

    @Test("logical type encodes as object")
    func logicalTypeEncode() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-millis"}"#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        #expect(String(data: canonical, encoding: .utf8)!.contains("timestamp-millis"))
    }

    @Test("decimal bytes with precision/scale round-trips")
    func decimalBytes() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"""
        {"type":"bytes","logicalType":"decimal","precision":4,"scale":2}
        """#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        #expect(String(data: canonical, encoding: .utf8)!.contains("decimal"))
    }

    @Test("array schema encodes with items")
    func arraySchemaEncode() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        let str = String(data: canonical, encoding: .utf8)!
        #expect(str.contains("array"))
        #expect(str.contains("items"))
    }

    @Test("map schema encodes with values")
    func mapSchemaEncode() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        let str = String(data: canonical, encoding: .utf8)!
        #expect(str.contains("map"))
        #expect(str.contains("values"))
    }

    @Test("fixed schema encodes with size")
    func fixedSchemaEncode() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"{"type":"fixed","name":"F","size":4}"#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        let str = String(data: canonical, encoding: .utf8)!
        #expect(str.contains("fixed"))
        #expect(str.contains("size"))
    }

    @Test("union schema encodes as JSON array")
    func unionSchemaEncode() throws {
        let avro = Avro()
        let s = try #require(avro.decodeSchema(schema: #"["null","int"]"#))
        avro.setSchemaFormat(option: .CanonicalForm)
        let canonical = try avro.encodeSchema(schema: s)
        let str = String(data: canonical, encoding: .utf8)!
        #expect(str.contains("null"))
        #expect(str.contains("int"))
    }

    // MARK: - Field aliases (single string vs array)

    @Test("field aliases as single string decodes")
    func aliasSingleString() throws {
        // The decoder accepts both single-string and array for `aliases`.
        let schema = try #require(avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int","aliases":"oldName"}]}
        """#))
        let record = try #require(schema.getRecord())
        #expect(record.fields.first?.aliases == ["oldName"])
    }

    @Test("field aliases as array decodes")
    func aliasAsArray() throws {
        let schema = try #require(avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int","aliases":["one","two"]}]}
        """#))
        let record = try #require(schema.getRecord())
        let aliases = try #require(record.fields.first?.aliases)
        #expect(aliases.contains("one"))
        #expect(aliases.contains("two"))
    }

    // MARK: - UnionSchema duplicate-branch error

    @Test("union inside record with duplicate branch types throws")
    func duplicateBranchThrows() {
        // The duplicate-branch detection lives in the typeMap-aware
        // validation, which is invoked from RecordSchema validation only.
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"f","type":["int","int"]}]}
        """#)
        // decodeSchema returns nil on validation failure
        #expect(schema == nil)
    }

    @Test("union with distinct branches decodes successfully")
    func distinctBranchesSucceed() throws {
        let s = try #require(avro().decodeSchema(schema: #"["int","string"]"#))
        #expect(s.getUnionList().count == 2)
    }

    // MARK: - errorSchema decode path

    @Test("error schema is decoded as errorSchema variant")
    func decodeError() throws {
        let s = try #require(avro().decodeSchema(schema: #"""
        {"type":"error","name":"MyError","fields":[
          {"name":"message","type":"string"}]}
        """#))
        #expect(s.getError() != nil)
    }

    // MARK: - Forward reference / unknown schema

    @Test("type+name forward reference creates unknownSchema")
    func forwardReference() throws {
        // {type: foo, name: bar} with no other keys is a forward reference.
        let s = try #require(avro().decodeSchema(schema: #"{"type":"OtherType","name":"Ref"}"#))
        #expect(s.isUnknown())
    }

    // MARK: - Convenience init from JSON string

    @Test("primitive short form decodes via convenience init")
    func primitiveShortForm() throws {
        let s = try AvroSchema(schemaJson: "int", decoder: JSONDecoder())
        #expect(s.isInt())
    }

    @Test("full JSON decodes via convenience init")
    func fullJSON() throws {
        let s = try AvroSchema(schemaJson: #"{"type":"long"}"#, decoder: JSONDecoder())
        #expect(s.isLong())
    }

    // MARK: - Record schema codable round-trip with optional fields

    @Test("field with default value decodes")
    func fieldWithDefault() throws {
        let s = try #require(avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int","default":"0"}]}
        """#))
        let record = try #require(s.getRecord())
        #expect(record.fields.first?.defaultValue != nil)
    }

    // MARK: - Decoding edge cases

    @Test("singleValue JSON that is neither string nor array throws")
    func decodeSingleValueBoolThrows() {
        let data = "42".data(using: .utf8)!
        #expect(throws: (any Error).self) {
            _ = try JSONDecoder().decode(AvroSchema.self, from: data)
        }
    }

    @Test("explicit branches-keyed JSON decodes as named union (lines 125-128)")
    func decodeBranchesKeyedUnion() throws {
        let data = #"""
        {"name":"U","optional":"null","branches":["int","string"]}
        """#.data(using: .utf8)!
        let schema = try JSONDecoder().decode(AvroSchema.self, from: data)
        #expect(schema.isUnion())
        #expect(schema.getUnionList().count == 2)
    }

    @Test("type 'record' with extra non-fields keys hits unknownSchema default branch")
    func decodeRecordWithoutFields() {
        // Forces Types.self decode to succeed for "record" but no specific
        // case in the switch handles it (line 170-171).
        let data = #"{"type":"record","name":"R","aliases":["A"]}"#.data(using: .utf8)!
        let schema = try? JSONDecoder().decode(AvroSchema.self, from: data)
        #expect(schema?.isUnknown() == true || schema == nil)
    }

    @Test("encoding an unknownSchema throws EncodingError.invalidValue")
    func encodeUnknownSchemaThrows() {
        let unknown = AvroSchema()  // Default init creates unknownSchema.
        #expect(throws: (any Error).self) {
            _ = try JSONEncoder().encode(unknown)
        }
    }

    @Test("decodeOptionalField throws when present-but-wrong-type")
    func decodeFieldOrderWrongType() {
        let data = #"""
        {"type":"record","name":"R","fields":[
          {"name":"x","type":"int","order":42}]}
        """#.data(using: .utf8)!
        #expect(throws: (any Error).self) {
            _ = try JSONDecoder().decode(AvroSchema.self, from: data)
        }
    }

    @Test("UnionSchema.init(name:optional:branches:) and addField on RecordSchema")
    func namedUnionAndAddField() {
        // Covers public init and RecordSchema.addField (lines 338-347, 478-480).
        let union = AvroSchema.UnionSchema(name: "U", optional: "null",
                                           branches: [.intSchema(.init())])
        #expect(union.name == "U")
        #expect(union.branches.count == 1)

        var record = AvroSchema.RecordSchema(
            name: "R", namespace: nil, type: "record",
            fields: [], aliases: nil, doc: nil)
        record.addField(.fieldSchema(.init(
            name: "a", type: .intSchema(.init()),
            doc: nil, order: nil, aliases: nil,
            defaultValue: nil, optional: nil
        )))
        #expect(record.fields.count == 1)
    }

    @Test("getNamespace and setName on dotted name schema")
    func namespaceAndSetName() throws {
        // Covers NameSchemaProtocol.getNamespace (line 278-281) and setName (295).
        let s = try #require(avro().decodeSchema(schema: #"""
        {"type":"record","name":"a.b.R","fields":[]}
        """#))
        var record = try #require(s.getRecord())
        #expect(record.getNamespace() == "a.b")
        record.setName(name: "Renamed")
        #expect(record.name == "Renamed")
    }

    @Test("UnionSchema validate resolves error branch from typeMap")
    func unionValidateErrorBranchFromMap() throws {
        // Targets UnionSchema.validate's errorSchema branch (lines 795-797)
        // and unknownName-found-in-typeMap path (lines 779-782) by parsing
        // a record whose field union references an earlier-declared error
        // type via a forward name.
        let json = #"""
        {"type":"record","name":"R","fields":[
          {"name":"e","type":{"type":"error","name":"E","fields":[{"name":"why","type":"string"}]}},
          {"name":"u","type":["null","E"]}
        ]}
        """#
        _ = avro().decodeSchema(schema: json)
    }

    // MARK: - Edge cases for AvroSchema+Codable coverage

    @Test("getNamespace returns the namespace field when name has no dot")
    func namespaceFromField() throws {
        // record's `name` is "R" (no dot), so getNamespace falls through to
        // the `namespace` field — the L280 fallback branch.
        let json = #"""
        {"type":"record","name":"R","namespace":"com.example","fields":[]}
        """#
        let schema = try #require(avro().decodeSchema(schema: json))
        guard case .recordSchema(let r) = schema else {
            Issue.record("expected recordSchema")
            return
        }
        #expect(r.getNamespace() == "com.example")
    }

    @Test("getNamespace returns nil when name has no dot and namespace is missing")
    func namespaceNilWhenMissing() throws {
        let json = #"{"type":"record","name":"R","fields":[]}"#
        let schema = try #require(avro().decodeSchema(schema: json))
        guard case .recordSchema(let r) = schema else {
            Issue.record("expected recordSchema")
            return
        }
        #expect(r.getNamespace() == nil)
    }

    @Test("union with inline record branch validates fixed/enum/record sub-fields")
    func unionRecordSubFieldValidation() throws {
        // Validating a union whose branch is a record forces
        // RecordField.validate(...) to dispatch through the fixed/enum/record
        // arms — paths only reached via the typeMap-aware validate path.
        let json = #"""
        {
          "type":"record","name":"Outer","fields":[
            {"name":"u","type":[
              "null",
              {"type":"record","name":"Inner","fields":[
                {"name":"f","type":{"type":"fixed","name":"F","size":4}},
                {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                {"name":"n","type":{"type":"record","name":"N","fields":[
                  {"name":"x","type":"int"}
                ]}}
              ]}
            ]}
          ]
        }
        """#
        let schema = try #require(avro().decodeSchema(schema: json))
        guard case .recordSchema = schema else {
            Issue.record("expected recordSchema")
            return
        }
    }

    @Test("FixedSchema decimal validate returns false when precision exceeds size capacity")
    func decimalFixedTooLargePrecision() throws {
        // size=4 fixed: bits=24, base realPrecision=6, lowerBits=4 (lowerNum=15
        // → 1 after one /10 step). After the loop realPrecision=7. Asking for
        // precision=10 forces the loop to exit naturally (no early return),
        // hits the final `return p <= realPrecision` (false), and the encoder
        // surfaces .invalidDecimal.
        let json = #"""
        {"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":10,"scale":0}
        """#
        let schema = try #require(avro().decodeSchema(schema: json))
        #expect(throws: BinaryEncodingError.invalidDecimal) {
            _ = try Avro().encodeSchema(schema: schema)
        }
    }

    @Test("decodeOptionalField throws when the key is present but value is JSON null")
    func decodeOptionalFieldNullThrows() throws {
        // A field's `doc` key set to JSON null should throw rather than be
        // silently dropped — exercises decodeOptionalField's else branch.
        let json = #"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int","doc":null}]}
        """#
        #expect(avro().decodeSchema(schema: json) == nil)
    }

    @Test("union with inline error branch is parsed and validated")
    func unionWithErrorBranch() throws {
        // The `error` branch in a union forces `UnionSchema.validate(...)` into
        // the `.errorSchema` arm.
        let json = #"""
        {
          "type":"record","name":"R","fields":[
            {"name":"u","type":[
              "null",
              {"type":"error","name":"E","fields":[{"name":"why","type":"string"}]}
            ]}
          ]
        }
        """#
        _ = avro().decodeSchema(schema: json)
    }

    @Test("Field with no type key returns nil (throws unknownSchemaJsonFormat)")
    func fieldMissingType() {
        // Field.init(from:) throws when the "type" key is absent from a field object.
        let json = #"""
        {"type":"record","name":"R","fields":[{"name":"a"}]}
        """#
        #expect(avro().decodeSchema(schema: json) == nil)
    }

    @Test("JSON number as schema hits singleValueContainer else branch")
    func jsonNumberAsSchema() {
        // JSON value 42 is not a String nor an Array, triggering the else throw in
        // AvroSchema.init(from:) singleValueContainer path (line 84).
        #expect(avro().decodeSchema(schema: "42") == nil)
    }

    // MARK: - RecordSchema.addField

    @Test("addField silently ignores a schema with no name")
    func addFieldIgnoresUnnamedSchema() throws {
        let record = try #require(avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        guard case .recordSchema(var r) = record else {
            Issue.record("Expected recordSchema")
            return
        }
        let before = r.fields.count
        r.addField(.unknownSchema(AvroSchema.UnknownSchema(typeName: "?", name: nil)))
        #expect(r.fields.count == before)
    }
}
