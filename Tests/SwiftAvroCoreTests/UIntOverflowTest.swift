//
//  UIntOverflowTest.swift
//  SwiftAvroCoreTests
//
//  Tests for UInt overflow handling in Avro encoding.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("UInt Overflow Tests")
struct UIntOverflowTests {

    // MARK: - Binary Encoder Overflow Tests

    @Test("UInt overflow throws uintOverflow error")
    func testUIntOverflow() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let encoder = AvroEncoder()

        // UInt at boundary - should succeed
        let maxValue = UInt(Int64.max)
        _ = try encoder.encode(maxValue, schema: schema)

        // UInt exceeding boundary - should throw
        let overflowValue = UInt(Int64.max) + 1
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder.encode(overflowValue, schema: schema)
        }
    }

    @Test("UInt64 overflow throws uintOverflow error")
    func testUInt64Overflow() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let encoder = AvroEncoder()

        // UInt64 at boundary - should succeed
        let maxValue = UInt64(Int64.max)
        _ = try encoder.encode(maxValue, schema: schema)

        // UInt64 exceeding boundary - should throw
        let overflowValue = UInt64(Int64.max) + 1
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder.encode(overflowValue, schema: schema)
        }
    }

    @Test("UInt max value encodes and decodes correctly")
    func testUIntMaxValueRoundTrip() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        let maxValue = UInt(Int64.max)
        let encoded = try encoder.encode(maxValue, schema: schema)
        let decoded = try decoder.decode(UInt.self, from: encoded)
        #expect(decoded == maxValue)
    }

    @Test("UInt64 max value encodes and decodes correctly")
    func testUInt64MaxValueRoundTrip() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        let maxValue = UInt64(Int64.max)
        let encoded = try encoder.encode(maxValue, schema: schema)
        let decoded = try decoder.decode(UInt64.self, from: encoded)
        #expect(decoded == maxValue)
    }

    @Test("Zero and small UInt values encode correctly")
    func testSmallUIntValues() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        for value: UInt in [0, 1, 127, 255, 32767, 65535] {
            let encoded = try encoder.encode(value, schema: schema)
            let decoded = try decoder.decode(UInt.self, from: encoded)
            #expect(decoded == value)
        }
    }

    // MARK: - Primitive Sizer Tests

    @Test("Primitive sizer UInt size calculation")
    func testPrimitiveSizerUIntSize() throws {
        // Test various UInt values and their expected varint sizes (after ZigZag encoding)
        // ZigZag: 0→0, 1→2, -1→1, n→2*|n| (if positive), so 127→254 needs 2 bytes
        var sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(0))
        #expect(sizer.size == 1)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(63))  // ZigZag → 126, fits in 1 byte
        #expect(sizer.size == 1)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(127))  // ZigZag → 254, needs 2 bytes
        #expect(sizer.size == 2)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(8191))  // ZigZag → 16382, fits in 2 bytes
        #expect(sizer.size == 2)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(8192))  // ZigZag → 16384, needs 3 bytes
        #expect(sizer.size == 3)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(Int64.max))
        #expect(sizer.size == 10) // Max long value, ZigZag encoded needs 10 bytes
    }

    @Test("Primitive sizer UInt64 size calculation")
    func testPrimitiveSizerUInt64Size() throws {
        var sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt64(0))
        #expect(sizer.size == 1)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt64(63))
        #expect(sizer.size == 1)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt64(127))
        #expect(sizer.size == 2)

        sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt64(Int64.max))
        #expect(sizer.size == 10)
    }

    @Test("Primitive sizer UInt encode succeeds")
    func testPrimitiveSizerUIntOverflow() throws {
        let sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt(Int64.max) + 1)
        #expect(sizer.size > 0)
    }

    @Test("Primitive sizer UInt64 encode succeeds")
    func testPrimitiveSizerUInt64Overflow() throws {
        let sizer = AvroPrimitiveSizer()
        try sizer.encode(UInt64(Int64.max) + 1)
        #expect(sizer.size > 0)
    }

    @Test("Primitive sizer size matches actual encoding")
    func testSizerMatchesEncoder() throws {
        let testValues: [UInt] = [0, 1, 127, 16383, 2097151, 268435455, UInt(Int64.max)]

        for value in testValues {
            let sizer = AvroPrimitiveSizer()
            let encoder = AvroPrimitiveEncoder()

            try sizer.encode(value)
            try encoder.encode(value)

            #expect(sizer.size == encoder.size, "Size mismatch for value \\(value): sizer=\\(sizer.size), encoder=\\(encoder.size)")
        }
    }

    // MARK: - Type Mismatch Tests

    @Test("UInt with wrong schema throws typeMismatch")
    func testUIntTypeMismatch() throws {
        let avro = Avro()
        let intSchema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let encoder = AvroEncoder()

        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt) {
            try encoder.encode(UInt(42), schema: intSchema)
        }
    }

    @Test("UInt64 with wrong schema throws typeMismatch")
    func testUInt64TypeMismatch() throws {
        let avro = Avro()
        let intSchema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let encoder = AvroEncoder()

        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt64) {
            try encoder.encode(UInt64(42), schema: intSchema)
        }
    }

    // MARK: - Record with UInt field tests

    @Test("Record with UInt field encodes and decodes")
    func testRecordWithUIntField() throws {
        struct Model: Codable {
            let id: UInt
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"id","type":"long"}]}"#))
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        let model = Model(id: 12345)
        let encoded = try encoder.encode(model, schema: schema)
        let decoded = try decoder.decode(Model.self, from: encoded)
        #expect(decoded.id == 12345)
    }

    @Test("Record with UInt64 field encodes and decodes")
    func testRecordWithUInt64Field() throws {
        struct Model: Codable {
            let id: UInt64
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"id","type":"long"}]}"#))
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        let model = Model(id: 9_223_372_036_854_775_807) // Int64.max
        let encoded = try encoder.encode(model, schema: schema)
        let decoded = try decoder.decode(Model.self, from: encoded)
        #expect(decoded.id == 9_223_372_036_854_775_807)
    }

    @Test("Record with UInt overflow throws")
    func testRecordWithUIntOverflow() throws {
        struct Model: Codable {
            let id: UInt
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"id","type":"long"}]}"#))
        let encoder = AvroEncoder()

        let overflowModel = Model(id: UInt(Int64.max) + 1)
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder.encode(overflowModel, schema: schema)
        }
    }

    @Test("Record with UInt64 overflow throws")
    func testRecordWithUInt64Overflow() throws {
        struct Model: Codable {
            let id: UInt64
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"id","type":"long"}]}"#))
        let encoder = AvroEncoder()

        let overflowModel = Model(id: UInt64(Int64.max) + 1)
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder.encode(overflowModel, schema: schema)
        }
    }

    // MARK: - JSON Encoder Tests

    @Test("JSON encoder UInt overflow throws uintOverflow error")
    func testJsonUIntOverflow() throws {
        struct Model: Codable {
            let value: UInt
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"value","type":"long"}]}"#))

        // UInt at boundary - should succeed
        let model = Model(value: UInt(Int64.max))
        let encoder1 = AvroJSONEncoder(schema: schema)
        try encoder1.encode(model)
        _ = try encoder1.getData()

        // UInt exceeding boundary - should throw
        let overflowModel = Model(value: UInt(Int64.max) + 1)
        let encoder2 = AvroJSONEncoder(schema: schema)
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder2.encode(overflowModel)
        }
    }

    @Test("JSON encoder UInt64 overflow throws uintOverflow error")
    func testJsonUInt64Overflow() throws {
        struct Model: Codable {
            let value: UInt64
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"value","type":"long"}]}"#))

        // UInt64 at boundary - should succeed
        let model = Model(value: UInt64(Int64.max))
        let encoder1 = AvroJSONEncoder(schema: schema)
        try encoder1.encode(model)
        _ = try encoder1.getData()

        // UInt64 exceeding boundary - should throw
        let overflowModel = Model(value: UInt64(Int64.max) + 1)
        let encoder2 = AvroJSONEncoder(schema: schema)
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder2.encode(overflowModel)
        }
    }

    @Test("JSON encoder UInt encodes correctly")
    func testJsonUIntEncode() throws {
        struct Model: Codable, Equatable {
            let value: UInt
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"value","type":"long"}]}"#))

        let model = Model(value: 42)
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(model)
        let data = try encoder.getData()
        let jsonString = String(decoding: data, as: UTF8.self)
        #expect(jsonString.contains("\"value\":42"))
    }

    @Test("JSON encoder UInt64 encodes correctly")
    func testJsonUInt64Encode() throws {
        struct Model: Codable, Equatable {
            let value: UInt64
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"M","fields":[{"name":"value","type":"long"}]}"#))

        let model = Model(value: 1_000_000_000)
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(model)
        let data = try encoder.getData()
        let jsonString = String(decoding: data, as: UTF8.self)
        #expect(jsonString.contains("\"value\":1000000000"))
    }
}
