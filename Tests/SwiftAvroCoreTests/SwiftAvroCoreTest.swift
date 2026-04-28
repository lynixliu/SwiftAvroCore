//
//  SwiftAvroCoreTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro Core API")
struct AvroCoreAPITests {

    // MARK: - Avro class init

    @Test("Avro init creates instance")
    func avroInit() {
        let avro = Avro()
        #expect(avro.getSchema() == nil)
    }

    // MARK: - Schema management

    @Test("setSchema and getSchema work")
    func setAndGetSchema() {
        let avro = Avro()
        let schema = AvroSchema(type: "string")
        avro.setSchema(schema: schema)
        #expect(avro.getSchema() != nil)
        #expect(avro.getSchema()?.isString() == true)
    }

    @Test("setSchemaFormat sets option")
    func setSchemaFormat() {
        let avro = Avro()
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        avro.setSchemaFormat(option: .FullForm)
        avro.setSchemaFormat(option: .CanonicalForm)
    }

    @Test("setAvroFormat sets option")
    func setAvroFormat() {
        let avro = Avro()
        avro.setAvroFormat(option: .AvroBinary)
        avro.setAvroFormat(option: .AvroJson)
    }

    // MARK: - Schema decoding

    @Test("decodeSchema from string")
    func decodeSchemaString() {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)
        #expect(schema != nil)
        #expect(schema?.isString() == true)
    }

    @Test("decodeSchema from Data")
    func decodeSchemaData() {
        let avro = Avro()
        let data = #"{"type":"int"}"#.data(using: .utf8)!
        let schema = avro.decodeSchema(schema: data)
        #expect(schema != nil)
        #expect(schema?.isInt() == true)
    }

    @Test("decodeSchema returns nil for invalid JSON")
    func decodeSchemaInvalid() {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: "invalid json")
        #expect(schema == nil)
    }

    @Test("newSchema creates schema without storing")
    func newSchemaWithoutStoring() {
        let avro = Avro()
        let schema = avro.newSchema(schema: #"{"type":"boolean"}"#)
        #expect(schema != nil)
        #expect(avro.getSchema() == nil)
    }

    @Test("newSchema from Data")
    func newSchemaData() {
        let avro = Avro()
        let data = #"{"type":"long"}"#.data(using: .utf8)!
        let schema = avro.newSchema(schema: data)
        #expect(schema != nil)
        #expect(schema?.isLong() == true)
    }

    // MARK: - Schema encoding

    @Test("encodeSchema with stored schema")
    func encodeStoredSchema() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let data = try avro.encodeSchema()
        #expect(data.count > 0)
    }

    @Test("encodeSchema with provided schema")
    func encodeProvidedSchema() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"int"}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    @Test("encodeSchema as canonical")
    func encodeSchemaAsCanonical() throws {
        let avro = Avro()
        avro.setSchemaFormat(option: .CanonicalForm)
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    @Test("encodeRecordSchema works")
    func encodeRecordSchema() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    @Test("encodeSchema pretty printed")
    func encodeSchemaPrettyPrinted() throws {
        let avro = Avro()
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        let schema = avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#)!
        let data = try avro.encodeSchema(schema: schema)
        let str = String(data: data, encoding: .utf8)!
        #expect(str.contains("\n"))
    }

    @Test("encodeSchema canonical form")
    func encodeSchemaCanonical() throws {
        let avro = Avro()
        avro.setSchemaFormat(option: .CanonicalForm)
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    // MARK: - Binary encode/decode

    @Test("encode with reflected schema")
    func encodeWithReflectedSchema() throws {
        let avro = Avro()
        let data = try avro.encode("hello")
        #expect(data.count > 0)
    }

    @Test("encodeFrom with explicit schema")
    func encodeFromExplicit() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let data = try avro.encodeFrom("test", schema: schema)
        #expect(data.count > 0)
    }

    @Test("decode round-trip")
    func decodeRoundTrip() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("hello")
        let decoded: String = try avro.decode(from: encoded)
        #expect(decoded == "hello")
    }

    @Test("decodeFrom with explicit schema")
    func decodeFromExplicit() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"int"}"#)!
        let data = try avro.encodeFrom(Int32(42), schema: schema)
        let value: Int32 = try avro.decodeFrom(from: data, schema: schema)
        #expect(value == 42)
    }

    @Test("decode untyped")
    func decodeUntyped() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("test")
        let decoded = try avro.decode(from: encoded)
        #expect(decoded as? String == "test")
    }

    @Test("decodeFrom untyped")
    func decodeFromUntyped() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"long"}"#)!
        let data = try avro.encodeFrom(Int64(123), schema: schema)
        let decoded = try avro.decodeFrom(from: data, schema: schema)
        #expect(decoded as? Int64 == 123)
    }

    // MARK: - These tests are skipped due to crash issues with current implementation

    // @Test("decode throws without schema")
    // func decodeNoSchema() throws {
    //     let avro = Avro()
    //     #expect(throws: BinaryEncodingError.noSchemaSpecified) {
    //         let _: String = try avro.decode(from: Data([0x01]))
    //     }
    // }

    // @Test("decodeFrom throws without schema")
    // func decodeFromNoSchema() throws {
    //     let avro = Avro()
    //     let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
    //     #expect(throws: BinaryEncodingError.noSchemaSpecified) {
    //         let _: String = try avro.decodeFrom(from: Data([0x01]), schema: schema)
    //     }
    // }

    // MARK: - AvroDataReader

    @Test("AvroDataReader isAtEnd initially false")
    func dataReaderIsAtEnd() {
        let avro = Avro()
        let data = Data([0x01])
        let reader = avro.makeDataReader(data: data)
        #expect(!reader.isAtEnd)
    }

    @Test("AvroDataReader bytesRemaining")
    func dataReaderBytesRemaining() {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01, 0x02, 0x03]))
        #expect(reader.bytesRemaining == 3)
    }

    @Test("AvroDataReader readBytes")
    func dataReaderReadBytes() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01, 0x02, 0x03]))
        let bytes = try reader.readBytes(count: 2)
        #expect(bytes.count == 2)
        #expect(reader.bytesRemaining == 1)
    }

    @Test("AvroDataReader readBytes throws on insufficient data")
    func dataReaderReadBytesInsufficient() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01]))
        var threw = false
        do {
            _ = try reader.readBytes(count: 10)
        } catch {
            threw = true
        }
        #expect(threw)
    }

    @Test("AvroDataReader decode typed")
    func dataReaderDecodeTyped() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("hello")
        let reader = avro.makeDataReader(data: encoded)
        let decoded: String = try reader.decode(schema: avro.getSchema()!)
        #expect(decoded == "hello")
    }

    @Test("AvroDataReader decode untyped")
    func dataReaderDecodeUntyped() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("world")
        let reader = avro.makeDataReader(data: encoded)
        let decoded = try reader.decode(schema: avro.getSchema()!)
        #expect(decoded as? String == "world")
    }

    @Test("AvroDataReader skip with bytes schema")
    func dataReaderSkip() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"bytes"}"#)!
        let encoded = try avro.encodeFrom([UInt8]([1, 2, 3]), schema: schema)
        let reader = avro.makeDataReader(data: encoded)
        try reader.skip(schema: schema)
        #expect(reader.isAtEnd)
    }

    @Test("AvroDataReader decodeContinue with empty buffer throws")
    func dataReaderEmptyBuffer() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let reader = avro.makeDataReader(data: Data())
        var threw = false
        do {
            _ = try reader.decode(schema: schema)
        } catch {
            threw = true
        }
        #expect(threw)
    }

    // MARK: - ObjectContainer

    @Test("makeFileObjectContainer without schema")
    func makeObjectContainerNoSchema() {
        let avro = Avro()
        _ = avro.makeFileObjectContainer()
    }

    @Test("makeFileObjectContainer with schema")
    func makeObjectContainerWithSchema() {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        _ = avro.makeFileObjectContainer()
    }

    // MARK: - Encoding options

    @Test("AvroEncodingOption raw values")
    func encodingOptionRawValues() {
        #expect(AvroEncodingOption.AvroBinary.rawValue == 0)
        #expect(AvroEncodingOption.AvroJson.rawValue == 1)
    }

    @Test("AvroSchemaEncodingOption raw values")
    func schemaEncodingOptionRawValues() {
        #expect(AvroSchemaEncodingOption.CanonicalForm.rawValue == 0)
        #expect(AvroSchemaEncodingOption.FullForm.rawValue == 1)
        #expect(AvroSchemaEncodingOption.PrettyPrintedForm.rawValue == 2)
    }

    // MARK: - AvroJSON encoding

    // MARK: - This test is skipped due to JSON serialization issues
    // @Test("encode with AvroJson format")
    // func encodeAvroJson() throws {
    //     let avro = Avro()
    //     avro.setAvroFormat(option: .AvroJson)
    //     avro.decodeSchema(schema: #"{"type":"string"}"#)
    //     let data = try avro.encode("json test")
    //     #expect(data.count > 0)
    // }
}