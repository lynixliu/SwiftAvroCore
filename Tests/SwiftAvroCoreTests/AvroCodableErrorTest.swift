//
//  AvroCodableErrorTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro Decoding Error Cases")
struct AvroDecodableErrorTests {

    // MARK: - Schema type mismatch errors

    @Test("decode Bool throws for non-boolean schema")
    func decodeBoolMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaBool) {
            let _: Bool = try decoder.decode(Bool.self, from: encoded)
        }
    }

    @Test("decode Int throws for non-int schema")
    func decodeIntMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaInt) {
            let _: Int = try decoder.decode(Int.self, from: encoded)
        }
    }

    @Test("decode Int8 throws for non-integer schema")
    func decodeInt8Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaInt8) {
            let _: Int8 = try decoder.decode(Int8.self, from: encoded)
        }
    }

    @Test("decode Int16 throws for non-integer schema")
    func decodeInt16Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaInt16) {
            let _: Int16 = try decoder.decode(Int16.self, from: encoded)
        }
    }

    @Test("decode Int32 throws for non-int schema")
    func decodeInt32Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaInt32) {
            let _: Int32 = try decoder.decode(Int32.self, from: encoded)
        }
    }

    @Test("decode Int64 throws for non-long schema")
    func decodeInt64Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaInt64) {
            let _: Int64 = try decoder.decode(Int64.self, from: encoded)
        }
    }

    @Test("decode UInt throws for non-integer schema")
    func decodeUIntMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaUInt) {
            let _: UInt = try decoder.decode(UInt.self, from: encoded)
        }
    }

    @Test("decode UInt8 throws for non-bytes schema")
    func decodeUInt8Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let encoded = try avro.encodeFrom(Int32(42), schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaUInt8) {
            let _: UInt8 = try decoder.decode(UInt8.self, from: encoded)
        }
    }

    @Test("decode UInt16 throws for non-integer schema")
    func decodeUInt16Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaUInt16) {
            let _: UInt16 = try decoder.decode(UInt16.self, from: encoded)
        }
    }

    @Test("decode UInt32 throws for non-fixed schema")
    func decodeUInt32Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let encoded = try avro.encodeFrom(Int32(42), schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaUInt32) {
            let _: UInt32 = try decoder.decode(UInt32.self, from: encoded)
        }
    }

    @Test("decode UInt64 throws for non-long schema")
    func decodeUInt64Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaUInt64) {
            let _: UInt64 = try decoder.decode(UInt64.self, from: encoded)
        }
    }

    @Test("decode Float throws for non-float schema")
    func decodeFloatMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaFloat) {
            let _: Float = try decoder.decode(Float.self, from: encoded)
        }
    }

    @Test("decode Double throws for non-double schema")
    func decodeDoubleMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let encoded = try avro.encodeFrom("test", schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaDouble) {
            let _: Double = try decoder.decode(Double.self, from: encoded)
        }
    }

    @Test("decode String throws for non-string schema")
    func decodeStringMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let encoded = try avro.encodeFrom(Int32(42), schema: schema)
        let decoder = AvroDecoder(schema: schema)
        #expect(throws: BinaryDecodingError.typeMismatchWithSchemaString) {
            let _: String = try decoder.decode(String.self, from: encoded)
        }
    }
}

@Suite("Avro Encoding Error Cases")
struct AvroEncodableErrorTests {

    // MARK: - Schema type mismatch errors

    @Test("encode Bool throws for non-boolean schema")
    func encodeBoolMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaBool) {
            try AvroEncoder().encode(true, schema: schema)
        }
    }

    @Test("encode String throws for non-string schema")
    func encodeStringMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaString) {
            try AvroEncoder().encode("test", schema: schema)
        }
    }

    @Test("encode Double throws for non-double schema")
    func encodeDoubleMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaDouble) {
            try AvroEncoder().encode(Double(3.14), schema: schema)
        }
    }

    @Test("encode Float throws for non-float schema")
    func encodeFloatMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaFloat) {
            try AvroEncoder().encode(Float(3.14), schema: schema)
        }
    }

    @Test("encode Int throws for non-int schema")
    func encodeIntMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt) {
            try AvroEncoder().encode(Int(42), schema: schema)
        }
    }

    @Test("encode Int8 throws for non-int schema")
    func encodeInt8Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt8) {
            try AvroEncoder().encode(Int8(42), schema: schema)
        }
    }

    @Test("encode Int16 throws for non-int schema")
    func encodeInt16Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt16) {
            try AvroEncoder().encode(Int16(42), schema: schema)
        }
    }

    @Test("encode Int32 throws for non-int schema")
    func encodeInt32Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt32) {
            try AvroEncoder().encode(Int32(42), schema: schema)
        }
    }

    @Test("encode Int64 throws for non-long schema")
    func encodeInt64Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt64) {
            try AvroEncoder().encode(Int64(42), schema: schema)
        }
    }

    @Test("encode UInt throws for non-long schema")
    func encodeUIntMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt) {
            try AvroEncoder().encode(UInt(42), schema: schema)
        }
    }

    @Test("encode UInt8 throws for non-fixed schema")
    func encodeUInt8Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt8) {
            try AvroEncoder().encode(UInt8(42), schema: schema)
        }
    }

    @Test("encode UInt16 throws for non-int schema")
    func encodeUInt16Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt16) {
            try AvroEncoder().encode(UInt16(42), schema: schema)
        }
    }

    @Test("encode UInt32 throws for non-fixed schema")
    func encodeUInt32Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt32) {
            try AvroEncoder().encode(UInt32(42), schema: schema)
        }
    }

    @Test("encode UInt64 throws for non-long schema")
    func encodeUInt64Mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt64) {
            try AvroEncoder().encode(UInt64(42), schema: schema)
        }
    }

    @Test("encode [UInt8] throws for non-bytes schema")
    func encodeBytesMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt8) {
            try AvroEncoder().encode([UInt8]([1, 2, 3]), schema: schema)
        }
    }

    @Test("encode fixed throws for non-fixed schema")
    func encodeFixedMismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let fixed_bytes: [UInt8] = [1, 2, 3, 4]
        #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt8) {
            try AvroEncoder().encode(fixed_bytes, schema: schema)
        }
    }
}