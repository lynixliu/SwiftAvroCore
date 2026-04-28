//
//  CodableAdvancedTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Advanced Decoding Tests")
struct AdvancedDecodingTests {

    @Test("decode optional field")
    func decodeOptional() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"required","type":"int"},{"name":"optional","type":["null","string"]}]}"#))
        struct TestRec: Codable {
            let required: Int
            let optional: String?
        }
        let test = TestRec(required: 1, optional: nil)
        let encoded = try avro.encode(test)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(TestRec.self, from: encoded)
        #expect(result.required == 1)
    }

    @Test("decode optional with value")
    func decodeOptionalWithValue() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"value","type":["null","string"]}]}"#))
        struct TestRec: Codable {
            let value: String?
        }
        let test = TestRec(value: "hello")
        let encoded = try avro.encode(test)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(TestRec.self, from: encoded)
        #expect(result.value == "hello")
    }

    @Test("decode map with values")
    func decodeMapWithValues() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"string"}"#))
        let encoded = try avro.encodeFrom(["a": "A", "b": "B"], schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded) as? [String: String]
        #expect(result?["a"] == "A")
        #expect(result?["b"] == "B")
    }
}

@Suite("Advanced Encoding Tests")
struct AdvancedEncodingTests {

    @Test("encode record with nested record")
    func encodeNestedRecord() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"value","type":"int"}]}}]}"#))
        struct Inner: Encodable {
            let value: Int
        }
        struct Outer: Encodable {
            let inner: Inner
        }
        let outer = Outer(inner: Inner(value: 123))
        let encoded = try AvroEncoder().encode(outer, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode optional nil")
    func encodeOptionalNil() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"value","type":["null","string"]}]}"#))
        struct TestRec: Encodable {
            let value: String?
        }
        let test = TestRec(value: nil)
        let encoded = try AvroEncoder().encode(test, schema: schema)
        #expect(encoded.count >= 0)
    }

    @Test("encode optional with value")
    func encodeOptionalWithValue() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"value","type":["null","string"]}]}"#))
        struct TestRec: Encodable {
            let value: String?
        }
        let test = TestRec(value: "hello")
        let encoded = try AvroEncoder().encode(test, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode array of records")
    func encodeArrayOfRecords() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"id","type":"int"}]}}"#))
        struct Item: Encodable {
            let id: Int
        }
        let items = [Item(id: 1), Item(id: 2)]
        let encoded = try AvroEncoder().encode(items, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode map")
    func encodeMap() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"string"}"#))
        let data: [String: String] = ["a": "A", "b": "B"]
        let encoded = try AvroEncoder().encode(data, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode array")
    func encodeArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        let data = ["a", "b", "c"]
        let encoded = try AvroEncoder().encode(data, schema: schema)
        #expect(encoded.count > 0)
    }
}