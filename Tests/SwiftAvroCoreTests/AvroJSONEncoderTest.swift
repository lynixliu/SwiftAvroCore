import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro JSON Encoder")
struct AvroJSONEncoderTests {

    @Test("encode Int8")
    func encodeInt8() throws {
        let schema = AvroSchema(type: "int")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Int8(42))
    }

    @Test("encode Int16")
    func encodeInt16() throws {
        let schema = AvroSchema(type: "int")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Int16(1000))
    }

    @Test("encode Int32")
    func encodeInt32() throws {
        let schema = AvroSchema(type: "int")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Int32(42))
    }

    @Test("encode Int64")
    func encodeInt64() throws {
        let schema = AvroSchema(type: "long")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Int64(12345678901))
    }

    @Test("encode Int")
    func encodeInt() throws {
        let schema = AvroSchema(type: "long")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Int(12345678901))
    }

    @Test("encode boolean")
    func encodeBoolean() throws {
        let schema = AvroSchema(type: "boolean")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(true)
    }

    @Test("encode Float")
    func encodeFloat() throws {
        let schema = AvroSchema(type: "float")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Float(3.14))
    }

    @Test("encode Double")
    func encodeDouble() throws {
        let schema = AvroSchema(type: "double")
        let encoder = AvroJSONEncoder(schema: schema)
        try encoder.encode(Double(2.71828))
    }

    @Test("encode type mismatch throws")
    func encodeTypeMismatch() throws {
        let schema = AvroSchema(type: "string")
        let encoder = AvroJSONEncoder(schema: schema)
        #expect(throws: BinaryEncodingError.typeMismatchWithSchema) {
            try encoder.encode(Int32(42))
        }
    }

    @Test("encode wrong boolean schema throws")
    func encodeBoolWrongSchema() throws {
        let schema = AvroSchema(type: "string")
        let encoder = AvroJSONEncoder(schema: schema)
        #expect(throws: BinaryEncodingError.typeMismatchWithSchema) {
            try encoder.encode(false)
        }
    }

    @Test("encode double for int schema throws")
    func encodeDoubleWrongSchema() throws {
        let schema = AvroSchema(type: "int")
        let encoder = AvroJSONEncoder(schema: schema)
        #expect(throws: BinaryEncodingError.typeMismatchWithSchema) {
            try encoder.encode(Double(1.5))
        }
    }

    @Test("encode UInt overflow throws")
    func encodeUIntOverflow() throws {
        let schema = AvroSchema(type: "long")
        let encoder = AvroJSONEncoder(schema: schema)
        let overflowValue = UInt(Int64.max) + 1
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder.encode(overflowValue)
        }
    }

    @Test("encode UInt64 overflow throws")
    func encodeUInt64Overflow() throws {
        let schema = AvroSchema(type: "long")
        let encoder = AvroJSONEncoder(schema: schema)
        let overflowValue = UInt64(Int64.max) + 1
        #expect(throws: BinaryEncodingError.uintOverflow) {
            try encoder.encode(overflowValue)
        }
    }
}