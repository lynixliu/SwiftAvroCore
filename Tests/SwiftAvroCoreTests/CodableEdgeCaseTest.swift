//
//  CodableEdgeCaseTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Edge Case Tests")
struct CodableEdgeCaseTests {

    @Test("decode array with block size")
    func decodeArrayWithBlock() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"long"}"#))
        let arr: [Int64] = [1, 2, 3, 4, 5]
        let encoded = try avro.encode(arr)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode([Int64].self, from: encoded)
        #expect(result.count == 5)
    }

    @Test("decode map with block size")
    func decodeMapWithBlock() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"long"}"#))
        let mp: [String: Int64] = ["a": 1, "b": 2, "c": 3]
        let encoded = try avro.encode(mp)
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode([String: Int64].self, from: encoded)
        #expect(result.count == 3)
    }

    @Test("decode empty record")
    func decodeEmptyRecord() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"Empty","fields":[]}"#))
        struct Empty: Codable {}
        let encoded = try avro.encode(Empty())
        let decoder = AvroDecoder(schema: schema)
        let result = try decoder.decode(Empty.self, from: encoded)
        #expect(result != nil)
    }

    @Test("encode string array")
    func encodeStringArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        let arr = ["a", "b", "c"]
        let encoded = try AvroEncoder().encode(arr, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode Int64 array")
    func encodeInt64Array() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"long"}"#))
        let arr: [Int64] = [1, 2, 3]
        let encoded = try AvroEncoder().encode(arr, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode empty array")
    func encodeEmptyArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        let arr: [Int32] = []
        let encoded = try AvroEncoder().encode(arr, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode int map")
    func encodeIntMap() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        let mp: [String: Int32] = ["x": 1]
        let encoded = try AvroEncoder().encode(mp, schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("encode empty map")
    func encodeEmptyMap() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        let mp: [String: Int32] = [:]
        let encoded = try AvroEncoder().encode(mp, schema: schema)
        #expect(encoded.count > 0)
    }
}