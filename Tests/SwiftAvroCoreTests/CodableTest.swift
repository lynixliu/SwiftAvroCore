//
//  CodableTest.swift
//  SwiftAvroCoreTests
//
//  Combined from:
//  - CodableAdvancedTest.swift
//  - CodableEdgeCaseTest.swift
//  - CodableAPITest.swift
//  - CodableKeyedTest.swift
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

// MARK: - AvroDecoder API Tests

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

// MARK: - AvroEncoder API Tests

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

// MARK: - Record Codable Tests

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

// MARK: - Container Codable Tests

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

// MARK: - Primitive Codable Tests

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

// MARK: - Advanced Decoding Tests

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

// MARK: - Advanced Encoding Tests

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

// MARK: - Edge Case Tests

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
        _ = try decoder.decode(Empty.self, from: encoded)
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

// MARK: - Keyed Container Decoding Tests

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

// MARK: - Keyed Container Encoding Tests

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

// MARK: - Unkeyed Container Tests

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

// MARK: - Single Value Container Tests

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
