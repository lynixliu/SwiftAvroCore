import Foundation
import Testing
import SwiftAvroCore

@Suite("Avro JSON Encoder Basic")
struct AvroJSONEncoderBasicTests {

    @Test("encode simple string")
    func encodeString() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: "\"string\""))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let data = try avro.encode("hello")
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("hello"))
    }

    struct PrimitiveRecord: Encodable {
        let intField: Int
        let boolField: Bool
    }

    @Test("encode record with struct")
    func encodeRecord() throws {
        let avro = Avro()
        let schemaJson = #"""
        {
            "type": "record",
            "name": "PrimitiveRecord",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "boolField", "type": "boolean"}
            ]
        }
        """#
        let schema = try #require(avro.decodeSchema(schema: schemaJson))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let record = PrimitiveRecord(intField: 42, boolField: true)
        let data = try avro.encode(record)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"intField\":42"))
        #expect(json.contains("\"boolField\":true"))
    }

    @Test("encode enum value")
    func encodeEnum() throws {
        let avro = Avro()
        let schemaJson = #"""
        {"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}
        """#
        let schema = try #require(avro.decodeSchema(schema: schemaJson))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let data = try avro.encode("GREEN")
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("GREEN"))
    }

    @Test("encode union with string")
    func encodeUnionString() throws {
        let avro = Avro()
        let schemaJson = #"""
        ["null", "string"]
        """#
        let schema = try #require(avro.decodeSchema(schema: schemaJson))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let data = try avro.encode("hello")
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"string\":\"hello\""))
    }

    @Test("encode uuid logical type")
    func encodeUUID() throws {
        let avro = Avro()
        let schemaJson = #"""
        {"type":"string","logicalType":"uuid"}
        """#
        let schema = try #require(avro.decodeSchema(schema: schemaJson))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let uuid = "550e8400-e29b-41d4-a716-446655440000"
        let data = try avro.encode(uuid)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains(uuid))
    }
}
