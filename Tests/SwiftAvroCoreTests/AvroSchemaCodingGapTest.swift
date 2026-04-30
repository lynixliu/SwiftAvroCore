import Foundation
import Testing
@testable import SwiftAvroCore

private func parse(_ json: String) throws -> AvroSchema {
    let avro = Avro()
    return try #require(avro.decodeSchema(schema: json))
}

private func encode(_ schema: AvroSchema, option: AvroSchemaEncodingOption) throws -> String {
    let avro = Avro()
    avro.setSchemaFormat(option: option)
    let data = try avro.encodeSchema(schema: schema)
    return String(decoding: data, as: UTF8.self)
}

// MARK: - Encoding form variations

@Suite("AvroSchema encoding – FullForm / PrettyPrintedForm")
struct SchemaEncodingFormTests {

    @Test("record encodes with aliases in FullForm")
    func recordFullFormAliases() throws {
        let schema = try parse(#"""
        {"type":"record","name":"R","aliases":["OldR"],
         "fields":[{"name":"a","type":"int"}]}
        """#)
        let canonical = try encode(schema, option: .CanonicalForm)
        let full      = try encode(schema, option: .FullForm)
        // CanonicalForm omits aliases; FullForm includes them.
        #expect(!canonical.contains("aliases"))
        #expect(full.contains("aliases"))
    }

    @Test("record encodes with doc in PrettyPrintedForm")
    func recordPrettyDoc() throws {
        let schema = try parse(#"""
        {"type":"record","name":"R","doc":"Some doc",
         "fields":[{"name":"a","type":"int","doc":"Field doc"}]}
        """#)
        let pretty = try encode(schema, option: .PrettyPrintedForm)
        #expect(pretty.contains("doc"))
        // pretty-print uses newlines
        #expect(pretty.contains("\n"))
    }

    @Test("enum encodes with aliases in FullForm and doc in PrettyPrintedForm")
    func enumFormVariants() throws {
        let schema = try parse(#"""
        {"type":"enum","name":"E","aliases":["OldE"],"doc":"E doc",
         "symbols":["A","B"]}
        """#)
        let full   = try encode(schema, option: .FullForm)
        let pretty = try encode(schema, option: .PrettyPrintedForm)
        #expect(full.contains("aliases"))
        #expect(pretty.contains("doc"))
    }

    @Test("primitive encodes as canonical short form")
    func primitiveCanonical() throws {
        let s = try parse(#"{"type":"int"}"#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("int"))
    }

    @Test("logical type encodes as object")
    func logicalTypeEncode() throws {
        let s = try parse(#"{"type":"long","logicalType":"timestamp-millis"}"#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("timestamp-millis"))
    }

    @Test("decimal bytes with precision/scale round-trips")
    func decimalBytes() throws {
        let s = try parse(#"""
        {"type":"bytes","logicalType":"decimal","precision":4,"scale":2}
        """#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("decimal"))
    }

    @Test("array schema encodes with items")
    func arraySchemaEncode() throws {
        let s = try parse(#"{"type":"array","items":"int"}"#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("array"))
        #expect(canonical.contains("items"))
    }

    @Test("map schema encodes with values")
    func mapSchemaEncode() throws {
        let s = try parse(#"{"type":"map","values":"int"}"#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("map"))
        #expect(canonical.contains("values"))
    }

    @Test("fixed schema encodes with size")
    func fixedSchemaEncode() throws {
        let s = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("fixed"))
        #expect(canonical.contains("size"))
    }

    @Test("union schema encodes as JSON array")
    func unionSchemaEncode() throws {
        let s = try parse(#"["null","int"]"#)
        let canonical = try encode(s, option: .CanonicalForm)
        #expect(canonical.contains("null"))
        #expect(canonical.contains("int"))
    }
}

// MARK: - Field aliases (single string vs array)

@Suite("FieldSchema aliases decoding")
struct FieldSchemaAliasTests {

    @Test("field aliases as single string decodes")
    func aliasSingleString() throws {
        // The decoder accepts both single-string and array for `aliases`.
        let schema = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int","aliases":"oldName"}]}
        """#)
        let record = try #require(schema.getRecord())
        #expect(record.fields.first?.aliases == ["oldName"])
    }

    @Test("field aliases as array decodes")
    func aliasAsArray() throws {
        let schema = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int","aliases":["one","two"]}]}
        """#)
        let record = try #require(schema.getRecord())
        let aliases = try #require(record.fields.first?.aliases)
        #expect(aliases.contains("one"))
        #expect(aliases.contains("two"))
    }
}

// MARK: - UnionSchema duplicate-branch error

@Suite("UnionSchema validation")
struct UnionSchemaValidationTests {

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
        let s = try parse(#"["int","string"]"#)
        #expect(s.getUnionList().count == 2)
    }
}

// MARK: - errorSchema decode path

@Suite("ErrorSchema decoding")
struct ErrorSchemaDecodeTests {

    @Test("error schema is decoded as errorSchema variant")
    func decodeError() throws {
        let s = try parse(#"""
        {"type":"error","name":"MyError","fields":[
          {"name":"message","type":"string"}]}
        """#)
        #expect(s.getError() != nil)
    }
}

// MARK: - Forward reference / unknown schema

@Suite("UnknownSchema decoding (forward reference)")
struct UnknownSchemaDecodeTests {

    @Test("type+name forward reference creates unknownSchema")
    func forwardReference() throws {
        // {type: foo, name: bar} with no other keys is a forward reference.
        let s = try parse(#"{"type":"OtherType","name":"Ref"}"#)
        #expect(s.isUnknown())
    }
}

// MARK: - Convenience init from JSON string

@Suite("AvroSchema.init(schemaJson:decoder:) convenience")
struct ConvenienceInitTests {

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
}

// MARK: - Record schema codable round-trip with optional fields

@Suite("Record schema with optional & nullable fields")
struct RecordSchemaRoundTripTests {

    @Test("field with default value decodes")
    func fieldWithDefault() throws {
        let s = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int","default":"0"}]}
        """#)
        let record = try #require(s.getRecord())
        #expect(record.fields.first?.defaultValue != nil)
    }
}
