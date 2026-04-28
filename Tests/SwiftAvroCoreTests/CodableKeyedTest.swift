//
//  CodableKeyedTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Keyed Container Decoding Tests")
struct KeyedContainerDecodingTests {

    @Test("decode bytes for key")
    func decodeBytesForKey() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"data","type":"bytes"}]}"#))
        struct TestRecord: Codable {
            let data: [UInt8]
        }
        let data: [UInt8] = [1, 2, 3]
        let test = TestRecord(data: data)
        let encoded = try avro.encode(test)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(TestRecord.self, from: encoded)
        #expect(result.data.count == 3)
    }

    @Test("decode string for key")
    func decodeStringForKey() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"name","type":"string"}]}"#))
        struct TestRecord: Codable {
            let name: String
        }
        let test = TestRecord(name: "hello")
        let encoded = try avro.encode(test)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(TestRecord.self, from: encoded)
        #expect(result.name == "hello")
    }

    @Test("decode UInt8 for key")
    func decodeUInt8ForKey() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"value","type":"bytes"}]}"#))
        struct TestRecord: Codable {
            let value: [UInt8]
        }
        let test = TestRecord(value: [UInt8]([42]))
        let encoded = try avro.encode(test)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(TestRecord.self, from: encoded)
        #expect(result.value.count > 0)
    }
}

@Suite("Keyed Container Encoding Tests")
struct KeyedContainerEncodingTests {

    @Test("encode via keyed container")
    func encodeKeyedContainer() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Test","fields":[{"name":"x","type":"int"}]}"#))
        struct Test: Encodable {
            let x: Int
        }
        let test = Test(x: 42)
        let encoded = try AvroEncoder().encode(test, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode nested via keyed container")
    func encodeNestedKeyedContainer() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"int"}]}}]}"#))
        struct Inner: Encodable {
            let val: Int
        }
        struct Outer: Encodable {
            let inner: Inner
        }
        let outer = Outer(inner: Inner(val: 42))
        let encoded = try AvroEncoder().encode(outer, schema: schema)
        #expect(encoded.count > 0)
    }
}

@Suite("Unkeyed Container Tests")
struct UnkeyedContainerTests {

    @Test("decode array via unkeyed container")
    func decodeUnkeyed() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        let arr = ["a", "b", "c"]
        let encoded = try avro.encode(arr)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode([String].self, from: encoded)
        #expect(result.count == 3)
    }

    @Test("decode empty array")
    func decodeEmptyArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        let arr: [Int] = []
        let encoded = try avro.encode(arr)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode([Int].self, from: encoded)
        #expect(result.count == 0)
    }
}

@Suite("Single Value Container Tests")
struct SingleValueContainerTests {

    @Test("decode single value string")
    func decodeSingleString() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded)
        #expect((result as? String) == "test")
    }

    @Test("decode single value enum")
    func decodeSingleEnum() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"enum","name":"Suit","symbols":["spades"]}"#))
        let encoded = try avro.encodeFrom("spades", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(from: encoded)
        #expect((result as? String) == "spades")
    }
}