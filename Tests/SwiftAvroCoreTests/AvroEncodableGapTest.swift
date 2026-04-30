import Foundation
import Testing
@testable import SwiftAvroCore

// MARK: - Logical-type Double encoding (EncodingHelper.encode(Double))

@Suite("AvroEncoder – Double via logical types")
struct DoubleLogicalTypeEncode {

    // The single-value path uses EncodingHelper.encode(Double), which has the
    // logical-type switch.

    @Test("encode top-level Double against int+date schema")
    func date() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"int","logicalType":"date"}
        """#))
        // Encode the underlying Double — EncodingHelper handles the conversion.
        let data = try AvroEncoder().encode(Double(0), schema: schema)
        #expect(data.count > 0)
    }

    @Test("encode top-level Double against int+time-millis schema")
    func timeMillis() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"int","logicalType":"time-millis"}
        """#))
        let data = try AvroEncoder().encode(Double(123), schema: schema)
        #expect(data.count > 0)
    }

    @Test("encode top-level Double against long+time-micros schema")
    func timeMicros() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"long","logicalType":"time-micros"}
        """#))
        let data = try AvroEncoder().encode(Double(1_000_000), schema: schema)
        #expect(data.count > 0)
    }

    @Test("encode top-level Double against long+timestamp-millis schema")
    func timestampMillis() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"long","logicalType":"timestamp-millis"}
        """#))
        let data = try AvroEncoder().encode(Double(5_000), schema: schema)
        #expect(data.count > 0)
    }

    @Test("encode top-level Double against long+timestamp-micros schema")
    func timestampMicros() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"long","logicalType":"timestamp-micros"}
        """#))
        let data = try AvroEncoder().encode(Double(2_000_000), schema: schema)
        #expect(data.count > 0)
    }

    @Test("encode top-level Double against int (no logical type) throws")
    func mismatch() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Double(1), schema: schema)
        }
    }
}

// MARK: - String encoding via EncodingHelper

@Suite("AvroEncoder – String via UUID/enum/union")
struct StringEncodeVariantsTest {

    @Test("uuid logical type accepts valid uuid")
    func uuidValid() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"string","logicalType":"uuid"}
        """#))
        let data = try AvroEncoder().encode("550e8400-e29b-41d4-a716-446655440000",
                                            schema: schema)
        #expect(data.count > 0)
    }

    @Test("uuid logical type rejects malformed uuid")
    func uuidInvalid() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"string","logicalType":"uuid"}
        """#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode("not-a-uuid", schema: schema)
        }
    }

    @Test("enum schema accepts valid symbol")
    func enumValid() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"enum","name":"E","symbols":["A","B","C"]}
        """#))
        let data = try AvroEncoder().encode("B", schema: schema)
        #expect(data == Data([0x02]))
    }

    @Test("enum schema rejects unknown symbol")
    func enumInvalid() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"enum","name":"E","symbols":["A","B"]}
        """#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode("Z", schema: schema)
        }
    }
}

// MARK: - Primitive mismatch errors via EncodingHelper

@Suite("AvroEncoder – primitive guard mismatch errors")
struct PrimitiveMismatchTest {

    private func intSchema() throws -> AvroSchema {
        try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
    }
    private func longSchema() throws -> AvroSchema {
        try #require(Avro().decodeSchema(schema: #"{"type":"long"}"#))
    }
    private func stringSchema() throws -> AvroSchema {
        try #require(Avro().decodeSchema(schema: #"{"type":"string"}"#))
    }

    @Test("Bool against int schema throws")
    func boolMismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(true, schema: try intSchema())
        }
    }

    @Test("Int against string schema throws")
    func intMismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Int(1), schema: try stringSchema())
        }
    }

    @Test("Int8 against string schema throws")
    func int8Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Int8(1), schema: try stringSchema())
        }
    }

    @Test("Int16 against string schema throws")
    func int16Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Int16(1), schema: try stringSchema())
        }
    }

    @Test("Int32 against string schema throws")
    func int32Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Int32(1), schema: try stringSchema())
        }
    }

    @Test("Int64 against int schema throws")
    func int64Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Int64(1), schema: try intSchema())
        }
    }

    @Test("UInt against int schema throws")
    func uintMismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(UInt(1), schema: try intSchema())
        }
    }

    @Test("UInt8 against int schema throws")
    func uint8Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(UInt8(1), schema: try intSchema())
        }
    }

    @Test("UInt16 against long schema throws")
    func uint16Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(UInt16(1), schema: try longSchema())
        }
    }

    @Test("UInt32 against int schema throws")
    func uint32Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(UInt32(1), schema: try intSchema())
        }
    }

    @Test("UInt64 against int schema throws")
    func uint64Mismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(UInt64(1), schema: try intSchema())
        }
    }

    @Test("Float against int schema throws")
    func floatMismatch() throws {
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode(Float(1.0), schema: try intSchema())
        }
    }
}

// MARK: - Keyed container nested-container creation paths

@Suite("AvroEncoder – keyed nested containers")
struct KeyedNestedContainerTest {

    @Test("record-of-record drives keyed nestedContainer + nestedUnkeyedContainer")
    func recordOfRecord() throws {
        struct Inner: Codable, Equatable { let a: Int32 }
        struct Outer: Codable, Equatable { let inner: Inner; let arr: [Int32] }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"Outer","fields":[
          {"name":"inner","type":{"type":"record","name":"Inner","fields":[
              {"name":"a","type":"int"}]}},
          {"name":"arr","type":{"type":"array","items":"int"}}
        ]}
        """#))
        let value = Outer(inner: Inner(a: 7), arr: [1, 2, 3])
        let data = try AvroEncoder().encode(value, schema: schema)
        let back = try AvroDecoder(schema: schema).decode(Outer.self, from: data)
        #expect(back == value)
    }

    @Test("record with [UInt32] duration field")
    func uint32Duration() throws {
        struct R: Codable, Equatable { let dur: [UInt32] }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"dur","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}}
        ]}
        """#))
        let value = R(dur: [1, 2, 3])
        let data = try AvroEncoder().encode(value, schema: schema)
        // Round-trip via AvroDecoder Any? since Decoder logic for UInt32 fixed
        // uses different paths.
        let back: Any? = try AvroDecoder(schema: schema).decode(from: data)
        let dict = try #require(back as? [String: Any])
        let dur = try #require(dict["dur"] as? [UInt32])
        #expect(dur == [1, 2, 3])
    }
}

// MARK: - Union-pinning init path

@Suite("AvroEncoder – union pinning")
struct UnionPinningTest {

    @Test("nested encoder pins to first non-null branch of union")
    func unionPinning() throws {
        struct Item: Codable, Equatable { let v: Int32 }
        struct R: Codable, Equatable { let item: Item? }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"item","type":["null",
            {"type":"record","name":"Item","fields":[{"name":"v","type":"int"}]}]}
        ]}
        """#))
        let value = R(item: Item(v: 42))
        let data = try AvroEncoder().encode(value, schema: schema)
        let back = try AvroDecoder(schema: schema).decode(R.self, from: data)
        #expect(back == value)
    }
}
