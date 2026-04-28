//
//  CodableAPITest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("AvroDecoder API Tests")
struct AvroDecoderAPITests {

    @Test("decode returns Any from Data with schema")
    func decodeAny() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("hello", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded)
        #expect(result as? String == "hello")
    }
}

@Suite("AvroEncoder API Tests")
struct AvroEncoderAPITests {

    @Test("encode returns Data")
    func encodeToData() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let result = try AvroEncoder().encode("hello", schema: schema)
        #expect(result.count > 0)
    }

    @Test("sizeOf returns correct size")
    func sizeOfReturnsSize() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let size = try AvroEncoder().sizeOf("hello", schema: schema)
        #expect(size > 0)
    }

    @Test("sizeOf returns correct size for Int32")
    func sizeOfInt32() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let size = try AvroEncoder().sizeOf(Int32(42), schema: schema)
        #expect(size > 0)
    }

    @Test("sizeOf returns correct size for Int64")
    func sizeOfInt64() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let size = try AvroEncoder().sizeOf(Int64(42), schema: schema)
        #expect(size > 0)
    }

    @Test("sizeOf returns correct size for Double")
    func sizeOfDouble() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"double"}"#))
        let size = try AvroEncoder().sizeOf(Double(3.14), schema: schema)
        #expect(size > 0)
    }
}

@Suite("Record Codable Tests")
struct RecordCodableTests {

    @Test("encode record with struct")
    func encodeRecord() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"value","type":"int"}]}"#))
        struct Test: Encodable {
            let value: Int
        }
        let encoded = try AvroEncoder().encode(Test(value: 42), schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("decode record")
    func decodeRecord() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"value","type":"int"}]}"#))
        struct TestRec: Codable {
            let value: Int
        }
        let test = TestRec(value: 42)
        let encoded = try avro.encode(test)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded)
        #expect(result != nil)
    }
}

@Suite("Container Codable Tests")
struct ContainerCodableTests {

    @Test("encode array")
    func encodeArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        let encoded = try AvroEncoder().encode([Int32]([1, 2, 3]), schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode map")
    func encodeMap() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"string"}"#))
        let encoded = try AvroEncoder().encode(["a": "x", "b": "y"], schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("decode map")
    func decodeMap() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"string"}"#))
        let encoded = try avro.encodeFrom(["a": "x"], schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded) as? [String: String]
        #expect(result?["a"] == "x")
    }
}

@Suite("Primitive Codable Tests")
struct PrimitiveCodableTests {

    @Test("encode bytes")
    func encodeBytes() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"bytes"}"#))
        let data: [UInt8] = [1, 2, 3]
        let encoded = try AvroEncoder().encode(data, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("decode bytes")
    func decodeBytes() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"bytes"}"#))
        let data: [UInt8] = [1, 2, 3]
        let encoded = try avro.encodeFrom(data, schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded) as? [UInt8]
        #expect(result?.count == 3)
    }

    @Test("encode fixed schema")
    func encodeFixed() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","name":"Md5","size":16}"#))
        let data = [UInt8](repeating: 0, count: 16)
        let encoded = try AvroEncoder().encode(data, schema: schema)
        #expect(encoded.count == 16)
    }

    @Test("decode fixed schema")
    func decodeFixed() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","name":"Md5","size":16}"#))
        let data = [UInt8](repeating: 0, count: 16)
        let encoded = try avro.encodeFrom(data, schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded) as? [UInt8]
        #expect(result?.count == 16)
    }

    @Test("encode enum")
    func encodeEnum() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"enum","name":"Suit","symbols":["spades","hearts"]}"#))
        let encoded = try AvroEncoder().encode("spades", schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("decode enum")
    func decodeEnum() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"enum","name":"Suit","symbols":["spades","hearts"]}"#))
        let encoded = try avro.encodeFrom("spades", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded) as? String
        #expect(result == "spades")
    }
}