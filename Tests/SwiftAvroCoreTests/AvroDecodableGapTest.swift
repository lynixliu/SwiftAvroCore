import Foundation
import Testing
@testable import SwiftAvroCore

// MARK: - Helpers

private func decode<T: Decodable>(
    _ type: T.Type,
    schema schemaJSON: String,
    bytes: [UInt8]
) throws -> T {
    let avro = Avro()
    let schema = try #require(avro.decodeSchema(schema: schemaJSON))
    return try AvroDecoder(schema: schema).decode(type, from: Data(bytes))
}

private func decodeAny(
    schema schemaJSON: String,
    bytes: [UInt8]
) throws -> Any? {
    let avro = Avro()
    let schema = try #require(avro.decodeSchema(schema: schemaJSON))
    return try AvroDecoder(schema: schema).decode(from: Data(bytes))
}

// MARK: - Logical-type Double decoding (DecodingHelper.decode(Double))

@Suite("AvroDecoder – Double via logical types")
struct DoubleLogicalTypeDecode {

    // Encode the underlying integer using the integer schema, then decode at
    // top level as Double — that path goes through DecodingHelper's
    // logical-type branches.

    @Test("date logical type decodes into Double")
    func date() throws {
        let avro = Avro()
        let intSchema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let logSchema = try #require(avro.decodeSchema(schema: #"""
        {"type":"int","logicalType":"date"}
        """#))
        let data = try AvroEncoder().encode(Int32(0), schema: intSchema)
        let back = try AvroDecoder(schema: logSchema).decode(Double.self, from: data)
        // 1970-2001 offset is non-zero
        #expect(back.isFinite)
    }

    @Test("time-millis logical type decodes into Double")
    func timeMillis() throws {
        let avro = Avro()
        let intSchema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let logSchema = try #require(avro.decodeSchema(schema: #"""
        {"type":"int","logicalType":"time-millis"}
        """#))
        let data = try AvroEncoder().encode(Int32(12_345), schema: intSchema)
        let back = try AvroDecoder(schema: logSchema).decode(Double.self, from: data)
        #expect(back == 12_345.0)
    }

    @Test("time-micros logical type decodes into Double")
    func timeMicros() throws {
        let avro = Avro()
        let longSchema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let logSchema = try #require(avro.decodeSchema(schema: #"""
        {"type":"long","logicalType":"time-micros"}
        """#))
        let data = try AvroEncoder().encode(Int64(1_000_000), schema: longSchema)
        let back = try AvroDecoder(schema: logSchema).decode(Double.self, from: data)
        #expect(back == 1.0)
    }

    @Test("timestamp-millis logical type decodes into Double")
    func timestampMillis() throws {
        let avro = Avro()
        let longSchema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let logSchema = try #require(avro.decodeSchema(schema: #"""
        {"type":"long","logicalType":"timestamp-millis"}
        """#))
        let data = try AvroEncoder().encode(Int64(5_000), schema: longSchema)
        let back = try AvroDecoder(schema: logSchema).decode(Double.self, from: data)
        #expect(back == 5.0)
    }

    @Test("timestamp-micros logical type decodes into Double")
    func timestampMicros() throws {
        let avro = Avro()
        let longSchema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let logSchema = try #require(avro.decodeSchema(schema: #"""
        {"type":"long","logicalType":"timestamp-micros"}
        """#))
        let data = try AvroEncoder().encode(Int64(2_000_000), schema: longSchema)
        let back = try AvroDecoder(schema: logSchema).decode(Double.self, from: data)
        #expect(back == 2.0)
    }

    @Test("non-logical-type long decode as Double throws typeMismatch")
    func mismatch() throws {
        let avro = Avro()
        let longSchema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let data = try AvroEncoder().encode(Int64(1), schema: longSchema)
        // long without logical type doesn't have a Double mapping
        #expect(throws: (any Error).self) {
            _ = try AvroDecoder(schema: longSchema).decode(Double.self, from: data)
        }
    }
}

// MARK: - String UUID & Enum via DecodingHelper

@Suite("AvroDecoder – String logical types and enum")
struct StringDecodeVariantsTest {

    private struct R: Codable, Equatable { let v: String }

    @Test("UUID logical-type decodes valid uuid")
    func uuidValid() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"v","type":{"type":"string","logicalType":"uuid"}}]}
        """#))
        let original = R(v: "550e8400-e29b-41d4-a716-446655440000")
        let data = try AvroEncoder().encode(original, schema: schema)
        let back = try AvroDecoder(schema: schema).decode(R.self, from: data)
        #expect(back == original)
    }

    @Test("UUID logical-type accepts valid uuid in top-level array")
    func uuidValidArray() throws {
        let schemaJSON = #"""
        {"type":"array","items":{"type":"string","logicalType":"uuid"}}
        """#
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: schemaJSON))
        let original = ["550e8400-e29b-41d4-a716-446655440000"]
        let data = try AvroEncoder().encode(original, schema: schema)
        let back = try AvroDecoder(schema: schema).decode([String].self, from: data)
        #expect(back == original)
    }

    @Test("enum field decodes via DecodingHelper.decode(String)")
    func enumViaHelper() throws {
        struct E: Codable, Equatable { let color: String }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"E","fields":[
          {"name":"color","type":{"type":"enum","name":"C","symbols":["RED","GREEN","BLUE"]}}]}
        """#))
        // index 1 = GREEN → zigzag 0x02
        let data = Data([0x02])
        let back = try AvroDecoder(schema: schema).decode(E.self, from: data)
        #expect(back.color == "GREEN")
    }
}

// MARK: - Map decoding via Dictionary AvroDecodable

@Suite("AvroDecoder – Dictionary AvroDecodable adapter")
struct DictionaryDecodingTest {

    @Test("decode top-level [String:Int32] via Codable")
    func topLevelMap() throws {
        let schemaJSON = #"{"type":"map","values":"int"}"#
        let original: [String: Int32] = ["a": 1, "b": 2, "c": 3]
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: schemaJSON))
        let data = try AvroEncoder().encode(original, schema: schema)
        let decoded: [String: Int32] = try AvroDecoder(schema: schema).decode(
            [String: Int32].self, from: data
        )
        #expect(decoded == original)
    }

    @Test("decode empty [String:Int32]")
    func emptyMap() throws {
        let schemaJSON = #"{"type":"map","values":"int"}"#
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: schemaJSON))
        // Empty map: just a 0 block count (zigzag 0x00)
        let data = Data([0x00])
        let decoded: [String: Int32] = try AvroDecoder(schema: schema).decode(
            [String: Int32].self, from: data
        )
        #expect(decoded.isEmpty)
    }
}

// MARK: - decode(from:) Any? – array/map/enum/union/error/fixed-duration

@Suite("AvroDecoder – decode(from:) Any? coverage")
struct DecodeAnyVariantsTest {

    @Test("null schema decodes to nil")
    func nullDecode() throws {
        let result = try decodeAny(schema: #"{"type":"null"}"#, bytes: [])
        #expect(result == nil)
    }

    @Test("enum out-of-range index throws indexOutofBoundary")
    func enumOOR() throws {
        let schema = #"{"type":"enum","name":"E","symbols":["A","B"]}"#
        // Index 5 → zigzag 0x0a
        #expect(throws: (any Error).self) {
            _ = try decodeAny(schema: schema, bytes: [0x0a])
        }
    }

    @Test("union out-of-range index throws indexOutofBoundary")
    func unionOOR() throws {
        let schema = #"["null","string"]"#
        // Index 7 → zigzag 0x0e
        #expect(throws: (any Error).self) {
            _ = try decodeAny(schema: schema, bytes: [0x0e])
        }
    }

    @Test("fixed schema with duration logical type decodes as [UInt32]")
    func fixedDuration() throws {
        let schema = #"{"type":"fixed","name":"D","size":12,"logicalType":"duration"}"#
        // 12 bytes representing 3 little-endian UInt32 values
        let bytes: [UInt8] = [
            0x01, 0x00, 0x00, 0x00,  // 1
            0x02, 0x00, 0x00, 0x00,  // 2
            0x03, 0x00, 0x00, 0x00,  // 3
        ]
        let result = try decodeAny(schema: schema, bytes: bytes)
        let arr = try #require(result as? [UInt32])
        #expect(arr == [1, 2, 3])
    }
}

// MARK: - Single-value container with union

@Suite("AvroDecoder – SingleValueDecodingContainer union")
struct SingleValueUnionTest {

    @Test("union out-of-range index throws indexOutofBoundary")
    func unionOORThrows() throws {
        // A struct that decodes via singleValueContainer using the parent
        // unionSchema directly.
        struct Wrapper: Decodable {
            init(from decoder: Decoder) throws {
                let c = try decoder.singleValueContainer()
                _ = try c.decode(Int32.self)
            }
        }
        let schemaJSON = #"["null","int"]"#
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: schemaJSON))
        // index = 5 (zigzag 0x0a) – out of range for a 2-branch union
        #expect(throws: (any Error).self) {
            _ = try AvroDecoder(schema: schema).decode(Wrapper.self, from: Data([0x0a]))
        }
    }
}

// MARK: - decodeNil with union

@Suite("AvroDecoder – decodeNil(forKey:) on union")
struct DecodeNilUnionTest {

    @Test("decodeNil on union with index out of range throws")
    func decodeNilUnionOOR() throws {
        struct R: Decodable {
            let v: String?
            init(from decoder: Decoder) throws {
                let c = try decoder.container(keyedBy: AnyKey.self)
                let key = AnyKey(stringValue: "v")!
                if try c.decodeNil(forKey: key) {
                    self.v = nil
                } else {
                    self.v = try c.decode(String.self, forKey: key)
                }
            }
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"v","type":["null","string"]}]}
        """#))
        // index 7 (zigzag 0x0e) – out of range
        #expect(throws: (any Error).self) {
            _ = try AvroDecoder(schema: schema).decode(R.self, from: Data([0x0e]))
        }
    }

    @Test("decodeNil on non-null non-union returns false")
    func decodeNilNonUnion() throws {
        struct R: Decodable {
            let isNil: Bool
            init(from decoder: Decoder) throws {
                let c = try decoder.container(keyedBy: AnyKey.self)
                self.isNil = try c.decodeNil(forKey: AnyKey(stringValue: "v")!)
                _ = try c.decode(Int32.self, forKey: AnyKey(stringValue: "v")!)
            }
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"v","type":"int"}]}
        """#))
        // int 1 (zigzag 0x02)
        let r = try AvroDecoder(schema: schema).decode(R.self, from: Data([0x02]))
        #expect(r.isNil == false)
    }
}

// MARK: - Record decode helpers (allKeys, superDecoder, contains)

@Suite("AvroDecoder – KeyedDecodingContainer protocol surface")
struct KeyedDecodingProtocolTest {

    @Test("contains, allKeys, superDecoder are exercised")
    func protocolMethods() throws {
        struct R: Decodable {
            init(from decoder: Decoder) throws {
                let c = try decoder.container(keyedBy: AnyKey.self)
                #expect(c.contains(AnyKey(stringValue: "v")!))
                let _ = c.allKeys
                let _ = try c.superDecoder()
                let _ = try c.superDecoder(forKey: AnyKey(stringValue: "v")!)
                _ = try c.decode(Int32.self, forKey: AnyKey(stringValue: "v")!)
            }
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"v","type":"int"}]}
        """#))
        _ = try AvroDecoder(schema: schema).decode(R.self, from: Data([0x02]))
    }

    @Test("decode([MK:T] forKey:) for missing key throws")
    func decodeMapMissingKey() throws {
        struct R: Decodable {
            init(from decoder: Decoder) throws {
                let c = try decoder.container(keyedBy: AnyKey.self)
                _ = try c.decode([String: Int32].self, forKey: AnyKey(stringValue: "missing")!)
            }
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"m","type":{"type":"map","values":"int"}}]}
        """#))
        #expect(throws: (any Error).self) {
            _ = try AvroDecoder(schema: schema).decode(R.self, from: Data([0x00]))
        }
    }
}

// MARK: - decode<T> with [K:T] explicit map decoder API

@Suite("AvroDecoder – decode<K,T>(_:from:) explicit API")
struct ExplicitMapDecodeAPI {

    @Test("[String:Int32] via Avro public API")
    func mapAPI() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        let original: [String: Int32] = ["x": 9, "y": 10]
        let data = try AvroEncoder().encode(original, schema: schema)
        let decoded = try AvroDecoder(schema: schema).decode([String: Int32].self, from: data)
        #expect(decoded == original)
    }
}

// MARK: - Helper

private struct AnyKey: CodingKey {
    var stringValue: String
    var intValue: Int?
    init?(stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
    init?(intValue: Int) { self.stringValue = "\(intValue)"; self.intValue = intValue }
}
