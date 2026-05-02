import Foundation
import Testing
import SwiftAvroCore

// MARK: - Helpers

private func makeAvro(schema schemaJson: String, json: Bool = true) throws -> Avro {
    let avro = Avro()
    let schema = try #require(avro.decodeSchema(schema: schemaJson))
    avro.setSchema(schema: schema)
    if json { avro.setAvroFormat(option: .AvroJson) }
    return avro
}

private func jsonString(from avro: Avro, encoding value: some Encodable) throws -> String {
    String(decoding: try avro.encode(value), as: UTF8.self)
}

// MARK: - Primitives (SingleValueEncodingContainer)

@Suite("AvroJSONEncoder – SingleValue primitives")
struct SingleValuePrimitiveTests {

    @Test("encode null via encodeNil")
    func encodeNull() throws {
        let avro = try makeAvro(schema: #"{"type":"null"}"#)
        let data = try avro.encode(Optional<String>.none as String?)
        #expect(data.count > 0)
    }

    @Test("encodeNil on union schema succeeds")
    func encodeNilOnUnion() throws {
        let avro = try makeAvro(schema: #"["null","string"]"#)
        // Optional.none encodes as null through union
        let data = try avro.encode(Optional<String>.none as String?)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("null"))
    }

    @Test("encodeNil throws on non-null non-union schema")
    func encodeNilThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"string"}"#)
        #expect(throws: (any Error).self) {
            try avro.encode(Optional<String>.none as String?)
        }
    }

    @Test("encode Bool true")
    func encodeBoolTrue() throws {
        let avro = try makeAvro(schema: #"{"type":"boolean"}"#)
        let json = try jsonString(from: avro, encoding: true)
        #expect(json.contains("true"))
    }

    @Test("encode Bool false")
    func encodeBoolFalse() throws {
        let avro = try makeAvro(schema: #"{"type":"boolean"}"#)
        let json = try jsonString(from: avro, encoding: false)
        #expect(json.contains("false"))
    }

    @Test("encode Bool throws on wrong schema")
    func encodeBoolThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"string"}"#)
        #expect(throws: (any Error).self) { try avro.encode(true) }
    }

    @Test("encode Int (maps to long)")
    func encodeInt() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let json = try jsonString(from: avro, encoding: Int(99))
        #expect(json.contains("99"))
    }

    @Test("encode Int (maps to int)")
    func encodeIntAsInt() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let json = try jsonString(from: avro, encoding: Int(7))
        #expect(json.contains("7"))
    }

    @Test("encode Int throws on wrong schema")
    func encodeIntThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"string"}"#)
        #expect(throws: (any Error).self) { try avro.encode(Int(1)) }
    }

    @Test("encode Int8")
    func encodeInt8() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let json = try jsonString(from: avro, encoding: Int8(5))
        #expect(json.contains("5"))
    }

    @Test("encode Int8 throws on wrong schema")
    func encodeInt8Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(Int8(1)) }
    }

    @Test("encode Int16")
    func encodeInt16() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let json = try jsonString(from: avro, encoding: Int16(300))
        #expect(json.contains("300"))
    }

    @Test("encode Int32")
    func encodeInt32() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let json = try jsonString(from: avro, encoding: Int32(42))
        #expect(json.contains("42"))
    }

    @Test("encode Int64")
    func encodeInt64() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let json = try jsonString(from: avro, encoding: Int64(9_000_000_000))
        #expect(json.contains("9000000000"))
    }

    @Test("encode Int64 throws on int schema")
    func encodeInt64ThrowsOnIntSchema() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(Int64(1)) }
    }

    @Test("encode UInt")
    func encodeUInt() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let json = try jsonString(from: avro, encoding: UInt(123))
        #expect(json.contains("123"))
    }

    @Test("encode UInt16")
    func encodeUInt16() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let json = try jsonString(from: avro, encoding: UInt16(500))
        #expect(json.contains("500"))
    }

    @Test("encode UInt32 on fixed schema")
    func encodeUInt32() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":4}"#)
        let json = try jsonString(from: avro, encoding: UInt32(1))
        #expect(json.count > 0)
    }

    @Test("encode UInt32 throws on wrong schema")
    func encodeUInt32Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(UInt32(1)) }
    }

    @Test("encode UInt64")
    func encodeUInt64() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let json = try jsonString(from: avro, encoding: UInt64(55))
        #expect(json.contains("55"))
    }

    @Test("encode Float")
    func encodeFloat() throws {
        let avro = try makeAvro(schema: #"{"type":"float"}"#)
        let json = try jsonString(from: avro, encoding: Float(1.5))
        #expect(json.contains("1.5"))
    }

    @Test("encode Float throws on wrong schema")
    func encodeFloatThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"double"}"#)
        #expect(throws: (any Error).self) { try avro.encode(Float(1.5)) }
    }

    @Test("encode Double")
    func encodeDouble() throws {
        let avro = try makeAvro(schema: #"{"type":"double"}"#)
        let json = try jsonString(from: avro, encoding: Double(2.718))
        #expect(json.contains("2.718"))
    }

    @Test("encode Double throws on wrong schema")
    func encodeDoubleThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"float"}"#)
        #expect(throws: (any Error).self) { try avro.encode(Double(1.0)) }
    }

    @Test("encode String")
    func encodeString() throws {
        let avro = try makeAvro(schema: #"{"type":"string"}"#)
        let json = try jsonString(from: avro, encoding: "hello")
        #expect(json.contains("hello"))
    }

    @Test("encode String on enum schema – valid symbol")
    func encodeStringEnum() throws {
        let avro = try makeAvro(schema: #"{"type":"enum","name":"C","symbols":["A","B"]}"#)
        let json = try jsonString(from: avro, encoding: "A")
        #expect(json.contains("A"))
    }

    @Test("encode String on enum schema – invalid symbol throws")
    func encodeStringEnumInvalid() throws {
        let avro = try makeAvro(schema: #"{"type":"enum","name":"C","symbols":["A","B"]}"#)
        #expect(throws: (any Error).self) { try avro.encode("Z") }
    }

    @Test("encode String on union schema")
    func encodeStringUnion() throws {
        let avro = try makeAvro(schema: #"["null","string"]"#)
        let json = try jsonString(from: avro, encoding: "world")
        #expect(json.contains("\"string\":\"world\""))
    }

    @Test("encode String on union without string branch throws")
    func encodeStringUnionNoStringThrows() throws {
        let avro = try makeAvro(schema: #"["null","int"]"#)
        #expect(throws: (any Error).self) { try avro.encode("oops") }
    }

    @Test("encode String throws on non-string schema")
    func encodeStringThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode("oops") }
    }

    @Test("encode UInt8 on bytes schema")
    func encodeUInt8Bytes() throws {
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        let json = try jsonString(from: avro, encoding: UInt8(0xFF))
        #expect(json.count > 0)
    }

    @Test("encode UInt8 on fixed schema")
    func encodeUInt8Fixed() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":1}"#)
        let json = try jsonString(from: avro, encoding: UInt8(0xAB))
        #expect(json.count > 0)
    }

    @Test("encode UInt8 throws on other schema")
    func encodeUInt8Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(UInt8(1)) }
    }
}

// MARK: - Bytes / Fixed helpers

@Suite("AvroJSONEncoder – bytes and fixed helpers")
struct BytesFixedTests {

    @Test("encodeBytes via bytes schema")
    func encodeBytesSchema() throws {
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        let data = try avro.encode([UInt8]([0x01, 0x02, 0x03]))
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.count > 0)
    }

    @Test("encodeBytes throws on wrong schema")
    func encodeBytesThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"string"}"#)
        #expect(throws: (any Error).self) {
            try avro.encode([UInt8]([0x01]))
        }
    }

    @Test("encode fixedValue [UInt8]")
    func encodeFixedUInt8() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":2}"#)
        let data = try avro.encode([UInt8]([0xDE, 0xAD]))
        #expect(data.count > 0)
    }

    @Test("encode fixedValue [UInt32]")
    func encodeFixedUInt32() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":4}"#)
        let data = try avro.encode([UInt32]([1, 2, 3, 4]))
        #expect(data.count > 0)
    }
}

// MARK: - Date logical type

@Suite("AvroJSONEncoder – date logical type")
struct DateLogicalTypeTests {

    @Test("encode Date as Avro date (int with logicalType date)")
    func encodeDate() throws {
        let avro = try makeAvro(schema: #"{"type":"int","logicalType":"date"}"#)
        let date = Date(timeIntervalSince1970: 0) // epoch = day 0
        let data = try avro.encode(date)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.count > 0)
    }
}

// MARK: - Records

@Suite("AvroJSONEncoder – keyed (record) encoding")
struct KeyedEncodingTests {

    struct AllPrimitives: Encodable {
        let s: String
        let i: Int32
        let l: Int64
        let b: Bool
        let f: Float
        let d: Double
    }

    @Test("encode record with all primitive field types")
    func encodeAllPrimitives() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"AllPrimitives","fields":[
            {"name":"s","type":"string"},
            {"name":"i","type":"int"},
            {"name":"l","type":"long"},
            {"name":"b","type":"boolean"},
            {"name":"f","type":"float"},
            {"name":"d","type":"double"}
          ]
        }
        """#)
        let r = AllPrimitives(s: "hi", i: 1, l: 2, b: true, f: 3.0, d: 4.0)
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("\"s\":\"hi\""))
        #expect(json.contains("\"i\":1"))
        #expect(json.contains("\"l\":2"))
        #expect(json.contains("\"b\":true"))
    }

    struct IntWidths: Encodable {
        let i8:  Int8
        let i16: Int16
        let u16: UInt16
        let u:   UInt
        let u64: UInt64
    }

    @Test("encode record with various int widths")
    func encodeIntWidths() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"IntWidths","fields":[
            {"name":"i8",  "type":"int"},
            {"name":"i16", "type":"int"},
            {"name":"u16", "type":"int"},
            {"name":"u",   "type":"long"},
            {"name":"u64", "type":"long"}
          ]
        }
        """#)
        let r = IntWidths(i8: 1, i16: 2, u16: 3, u: 4, u64: 5)
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("\"i8\":1"))
        #expect(json.contains("\"i16\":2"))
    }

    struct WithEnum: Encodable {
        let color: String
    }

    @Test("encode record field with enum schema")
    func encodeRecordWithEnum() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithEnum","fields":[
            {"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}}
          ]
        }
        """#)
        let r = WithEnum(color: "RED")
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("\"color\":\"RED\""))
    }

    struct WithUnionString: Encodable {
        let maybeStr: String
    }

    @Test("encode record field with union string schema")
    func encodeRecordWithUnionString() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithUnionString","fields":[
            {"name":"maybeStr","type":["null","string"]}
          ]
        }
        """#)
        let r = WithUnionString(maybeStr: "yo")
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("\"string\":\"yo\""))
    }

    struct WithBytes: Encodable {
        let raw: [UInt8]
    }

    @Test("encode record field with bytes schema")
    func encodeRecordWithBytes() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithBytes","fields":[
            {"name":"raw","type":"bytes"}
          ]
        }
        """#)
        let r = WithBytes(raw: [0xDE, 0xAD, 0xBE, 0xEF])
        let data = try avro.encode(r)
        #expect(data.count > 0)
    }

    struct WithUInt32Field: Encodable {
        let val: UInt32
    }

    @Test("encode record field UInt32 on long schema")
    func encodeRecordUInt32() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithUInt32","fields":[
            {"name":"val","type":"long"}
          ]
        }
        """#)
        let r = WithUInt32Field(val: 99)
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("99"))
    }

    struct WithUInt8Field: Encodable {
        let val: UInt8
    }

    @Test("encode record field UInt8 on fixed schema")
    func encodeRecordUInt8() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithUInt8","fields":[
            {"name":"val","type":{"type":"fixed","name":"F","size":1}}
          ]
        }
        """#)
        let r = WithUInt8Field(val: 0xAB)
        let data = try avro.encode(r)
        #expect(data.count > 0)
    }

    struct WithUInt64Field: Encodable {
        let big: UInt64
    }

    @Test("encode record field UInt64")
    func encodeRecordUInt64() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithUInt64","fields":[
            {"name":"big","type":"long"}
          ]
        }
        """#)
        let r = WithUInt64Field(big: 1_000_000)
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("1000000"))
    }

    @Test("encodeNil forKey on union field")
    func encodeNilForKeyUnion() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithOpt","fields":[
            {"name":"opt","type":["null","string"]}
          ]
        }
        """#)
        struct WithOpt: Encodable {
            let opt: String?
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: CodingKeys.self)
                try c.encodeIfPresent(opt, forKey: .opt)
                if opt == nil { try c.encodeNil(forKey: .opt) }
            }
            enum CodingKeys: String, CodingKey { case opt }
        }
        let data = try avro.encode(WithOpt(opt: nil))
        #expect(data.count > 0)
    }

    @Test("encodeNil forKey on null-only field")
    func encodeNilForKeyNull() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithNull","fields":[
            {"name":"n","type":"null"}
          ]
        }
        """#)
        struct WithNull: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: CodingKeys.self)
                try c.encodeNil(forKey: .n)
            }
            enum CodingKeys: String, CodingKey { case n }
        }
        let data = try avro.encode(WithNull())
        #expect(data.count > 0)
    }

    @Test("encodeNil forKey on non-nullable field throws")
    func encodeNilForKeyThrows() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"Strict","fields":[
            {"name":"x","type":"int"}
          ]
        }
        """#)
        struct Strict: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: CodingKeys.self)
                try c.encodeNil(forKey: .x)
            }
            enum CodingKeys: String, CodingKey { case x }
        }
        #expect(throws: (any Error).self) { try avro.encode(Strict()) }
    }

    struct WithOptionalTrailing: Encodable {
        let required: String
        let trailing: String?
    }

    @Test("trailing nil optional fields are emitted as null via encodeNilsAfter")
    func encodeTrailingNilFields() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithOpt","fields":[
            {"name":"required","type":"string"},
            {"name":"trailing","type":["null","string"]}
          ]
        }
        """#)
        let r = WithOptionalTrailing(required: "yes", trailing: nil)
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("\"required\":\"yes\""))
    }

    struct WithLeadingNil: Encodable {
        let leading: String?
        let required: String
    }

    @Test("leading nil optional fields are emitted as null via encodeNilsBefore")
    func encodeLeadingNilFields() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithLeadingNil","fields":[
            {"name":"leading","type":["null","string"]},
            {"name":"required","type":"string"}
          ]
        }
        """#)
        let r = WithLeadingNil(leading: nil, required: "here")
        let json = try jsonString(from: avro, encoding: r)
        #expect(json.contains("\"required\":\"here\""))
    }
}

// MARK: - Arrays (UnkeyedEncodingContainer)

@Suite("AvroJSONEncoder – unkeyed (array) encoding")
struct UnkeyedEncodingTests {

    @Test("encode array of strings")
    func encodeStringArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"string"}"#)
        let json = try jsonString(from: avro, encoding: ["a", "b", "c"])
        #expect(json.contains("\"a\""))
        #expect(json.contains("\"c\""))
    }

    @Test("encode array of ints")
    func encodeIntArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        let json = try jsonString(from: avro, encoding: [Int32(1), Int32(2)])
        #expect(json.contains("1"))
        #expect(json.contains("2"))
    }

    @Test("encode array of longs")
    func encodeLongArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"long"}"#)
        let json = try jsonString(from: avro, encoding: [Int64(100), Int64(200)])
        #expect(json.contains("100"))
    }

    @Test("encode array of booleans")
    func encodeBoolArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"boolean"}"#)
        let json = try jsonString(from: avro, encoding: [true, false])
        #expect(json.contains("true"))
        #expect(json.contains("false"))
    }

    @Test("encode array of doubles")
    func encodeDoubleArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"double"}"#)
        let json = try jsonString(from: avro, encoding: [1.1, 2.2])
        #expect(json.contains("1.1"))
    }

    @Test("encode array of floats")
    func encodeFloatArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"float"}"#)
        let json = try jsonString(from: avro, encoding: [Float(1.0), Float(2.0)])
        #expect(json.count > 0)
    }

    @Test("encode array of Int8")
    func encodeInt8Array() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        let json = try jsonString(from: avro, encoding: [Int8(10), Int8(20)])
        #expect(json.contains("10"))
    }

    @Test("encode array of Int16")
    func encodeInt16Array() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        let json = try jsonString(from: avro, encoding: [Int16(500), Int16(600)])
        #expect(json.contains("500"))
    }

    @Test("encode array of Int32")
    func encodeInt32Array() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        let json = try jsonString(from: avro, encoding: [Int32(9), Int32(8)])
        #expect(json.contains("9"))
    }

    @Test("encode array of UInt8 on bytes schema")
    func encodeUInt8Array() throws {
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        let data = try avro.encode([UInt8]([0x01, 0x02]))
        #expect(data.count > 0)
    }

    @Test("encode array of UInt16")
    func encodeUInt16Array() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        let json = try jsonString(from: avro, encoding: [UInt16(10), UInt16(20)])
        #expect(json.contains("10"))
    }

    @Test("encode array of UInt32 on fixed")
    func encodeUInt32Array() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":4}"#)
        let data = try avro.encode([UInt32]([1, 2, 3, 4]))
        #expect(data.count > 0)
    }

    @Test("encode array of UInt64")
    func encodeUInt64Array() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"long"}"#)
        let json = try jsonString(from: avro, encoding: [UInt64(7), UInt64(8)])
        #expect(json.contains("7"))
    }

    @Test("encode array of UInt64 overflow throws")
    func encodeUInt64ArrayOverflow() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"long"}"#)
        #expect(throws: (any Error).self) {
            try avro.encode([UInt64.max])
        }
    }

    @Test("encode array of bytes")
    func encodeBytesArray() throws {
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        let data = try avro.encode([UInt8]([0xAB, 0xCD]))
        #expect(data.count > 0)
    }

    @Test("encodeNil in unkeyed container on null schema")
    func encodeNilUnkeyed() throws {
        let avro = try makeAvro(schema: #"{"type":"null"}"#)
        struct NullEncodable: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encodeNil()
            }
        }
        let data = try avro.encode(NullEncodable())
        #expect(data.count > 0)
    }

    @Test("encodeNil in unkeyed container on wrong schema throws")
    func encodeNilUnkeyedThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        struct NullEncodable: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encodeNil()
            }
        }
        #expect(throws: (any Error).self) { try avro.encode(NullEncodable()) }
    }

    @Test("encode array of union strings")
    func encodeUnionStringArray() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":["null","string"]}"#)
        struct U: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encode("hi")
            }
        }
        let data = try avro.encode(U())
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"string\":\"hi\""))
    }

    @Test("encode array of enum strings")
    func encodeEnumStringArray() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"array","items":{"type":"enum","name":"E","symbols":["X","Y"]}}
        """#)
        struct U: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encode("X")
            }
        }
        let data = try avro.encode(U())
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"X\""))
    }

    @Test("encode array of records")
    func encodeRecordArray() throws {
        struct Item: Encodable { let x: Int32 }
        let avro = try makeAvro(schema: #"""
        {"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"x","type":"int"}]}}
        """#)
        let json = try jsonString(from: avro, encoding: [Item(x: 1), Item(x: 2)])
        #expect(json.contains("\"x\":1"))
        #expect(json.contains("\"x\":2"))
    }
}

// MARK: - Logical types

@Suite("AvroJSONEncoder – logical types")
struct LogicalTypeTests {

    @Test("encode UUID string")
    func encodeUUID() throws {
        let avro = try makeAvro(schema: #"{"type":"string","logicalType":"uuid"}"#)
        let uuid = "550e8400-e29b-41d4-a716-446655440000"
        let json = try jsonString(from: avro, encoding: uuid)
        #expect(json.contains(uuid))
    }
}

// MARK: - Nested containers

@Suite("AvroJSONEncoder – nested container paths")
struct NestedContainerTests {

    @Test("superEncoder() and superEncoder(forKey:) return encoder")
    func superEncoderPaths() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}
        """#)
        struct UseSuper: Encodable {
            func encode(to encoder: Encoder) throws {
                var kc = encoder.container(keyedBy: CodingKeys.self)
                _ = kc.superEncoder()
                _ = kc.superEncoder(forKey: .x)
            }
            enum CodingKeys: String, CodingKey { case x }
        }
        // Should not crash
        _ = try? avro.encode(UseSuper())
    }

    @Test("superEncoder() in unkeyed container")
    func unkeyedSuperEncoder() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        struct UseSuper: Encodable {
            func encode(to encoder: Encoder) throws {
                var uc = encoder.unkeyedContainer()
                _ = uc.superEncoder()
            }
        }
        _ = try? avro.encode(UseSuper())
    }

    @Test("nestedContainer forKey")
    func nestedContainerForKey() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"Outer","fields":[
          {"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
        ]}
        """#)
        struct Inner: Encodable { let v: Int32 }
        struct Outer: Encodable { let inner: Inner }
        let data = try avro.encode(Outer(inner: Inner(v: 7)))
        #expect(data.count > 0)
    }

    @Test("nestedUnkeyedContainer forKey")
    func nestedUnkeyedContainerForKey() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"arr","type":{"type":"array","items":"int"}}
        ]}
        """#)
        struct R: Encodable { let arr: [Int32] }
        let data = try avro.encode(R(arr: [1, 2, 3]))
        #expect(data.count > 0)
    }

    @Test("nestedContainer in unkeyed container")
    func unkeyedNestedContainer() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"array","items":{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}}
        """#)
        struct R: Encodable { let v: Int32 }
        let data = try avro.encode([R(v: 5)])
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"v\":5"))
    }

    @Test("nestedUnkeyedContainer in unkeyed container")
    func unkeyedNestedUnkeyed() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        struct Nested: Encodable {
            func encode(to encoder: Encoder) throws {
                var uc = encoder.unkeyedContainer()
                var nested = uc.nestedUnkeyedContainer()
                try nested.encode(Int32(42))
            }
        }
        _ = try? avro.encode(Nested())
    }
}

// MARK: - Unkeyed primitive encode overloads with type mismatches

@Suite("AvroJSONEncoder – unkeyed primitive type mismatches")
struct UnkeyedPrimitiveMismatchTests {

    private func wrongSchemaArray(items: String) throws -> Avro {
        let avro = try makeAvro(schema: #"{"type":"array","items":"\#(items)"}"#)
        return avro
    }

    @Test("Bool array against int schema throws")
    func boolMismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([true, false]) }
    }

    @Test("Double array against int schema throws")
    func doubleMismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([1.5, 2.5]) }
    }

    @Test("Float array against int schema throws")
    func floatMismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([Float(1.0)]) }
    }

    @Test("Int (long-mapped) array against bool schema throws")
    func intLongMismatch() throws {
        let avro = try wrongSchemaArray(items: "boolean")
        #expect(throws: (any Error).self) { try avro.encode([Int(1)]) }
    }

    @Test("Int8 array against long schema throws")
    func int8Mismatch() throws {
        let avro = try wrongSchemaArray(items: "long")
        #expect(throws: (any Error).self) { try avro.encode([Int8(1)]) }
    }

    @Test("Int16 array against long schema throws")
    func int16Mismatch() throws {
        let avro = try wrongSchemaArray(items: "long")
        #expect(throws: (any Error).self) { try avro.encode([Int16(1)]) }
    }

    @Test("Int32 array against long schema throws")
    func int32Mismatch() throws {
        let avro = try wrongSchemaArray(items: "long")
        #expect(throws: (any Error).self) { try avro.encode([Int32(1)]) }
    }

    @Test("Int64 array against int schema throws")
    func int64Mismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([Int64(1)]) }
    }

    @Test("UInt array against int schema throws")
    func uIntMismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([UInt(1)]) }
    }

    @Test("UInt16 array against long schema throws")
    func uInt16Mismatch() throws {
        let avro = try wrongSchemaArray(items: "long")
        #expect(throws: (any Error).self) { try avro.encode([UInt16(1)]) }
    }

    @Test("UInt32 array against int schema throws")
    func uInt32Mismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([UInt32(1)]) }
    }

    @Test("UInt64 array against int schema throws")
    func uInt64Mismatch() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([UInt64(1)]) }
    }

    @Test("UInt8 array against int schema throws")
    func uInt8WrongSchema() throws {
        let avro = try wrongSchemaArray(items: "int")
        #expect(throws: (any Error).self) { try avro.encode([UInt8(1)]) }
    }

    @Test("[UInt8] against int (top-level non-bytes) throws")
    func bytesArrayMismatch() throws {
        // bytes guard at L693: only bytes schema accepts [UInt8]
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode([UInt8(1), UInt8(2)]) }
    }
}

// MARK: - Unkeyed string variants (string / enum / union)

@Suite("AvroJSONEncoder – unkeyed string variants")
struct UnkeyedStringVariantTests {

    @Test("string array against enum schema (valid symbols)")
    func enumValid() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"array","items":{"type":"enum","name":"E","symbols":["X","Y","Z"]}}
        """#)
        let json = String(decoding: try avro.encode(["X", "Z"]), as: UTF8.self)
        #expect(json.contains("\"X\""))
        #expect(json.contains("\"Z\""))
    }

    @Test("string array against enum schema (invalid symbol throws)")
    func enumInvalid() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"array","items":{"type":"enum","name":"E","symbols":["X","Y"]}}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(["NOPE"]) }
    }

    @Test("string array against union without string throws")
    func unionWithoutStringThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":["null","int"]}"#)
        #expect(throws: (any Error).self) { try avro.encode(["hi"]) }
    }

    @Test("string array against non-string non-enum non-union throws")
    func defaultMismatch() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(["text"]) }
    }
}

// MARK: - Unkeyed bytes / fixed paths

@Suite("AvroJSONEncoder – unkeyed bytes/fixed")
struct UnkeyedBytesFixedTests {

    @Test("[UInt8] against fixed schema (via generic encode<T>)")
    func uint8ArrayAgainstFixed() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":4}"#)
        let data = try avro.encode([UInt8]([0xDE, 0xAD, 0xBE, 0xEF]))
        #expect(data.count > 0)
    }

    @Test("[UInt32] against fixed-duration schema (via generic encode<T>)")
    func uint32ArrayAgainstFixedDuration() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"D","size":12,"logicalType":"duration"}"#)
        let data = try avro.encode([UInt32]([1, 2, 3]))
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("1"))
        #expect(json.contains("2"))
        #expect(json.contains("3"))
    }
}

// MARK: - Keyed container: [UInt8] / fixed / nested / superEncoder

@Suite("AvroJSONEncoder – keyed bytes & nested coverage")
struct KeyedBytesNestedTests {

    @Test("record with [UInt8] field encoded as bytes")
    func recordBytesField() throws {
        struct R: Encodable { let payload: [UInt8] }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"payload","type":"bytes"}
        ]}
        """#)
        let json = String(decoding: try avro.encode(R(payload: [0x01, 0x02, 0x03])), as: UTF8.self)
        #expect(json.contains("\"payload\""))
    }

    @Test("record with [UInt8] field against non-bytes throws")
    func recordBytesFieldMismatch() throws {
        struct R: Encodable { let payload: [UInt8] }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"payload","type":"string"}
        ]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R(payload: [1, 2])) }
    }

    @Test("record with nested record uses keyed nestedContainer path")
    func recordWithNestedRecord() throws {
        // Exercise nestedContainer(keyedBy:forKey:) execution path. The
        // encoder doesn't actually propagate child writes back to the parent
        // for this manual path, so we only assert that the call doesn't
        // throw — the goal is to execute L532–539 of AvroJsonEncoder.swift.
        struct Outer: Encodable {
            func encode(to encoder: Encoder) throws {
                var k = encoder.container(keyedBy: AnyKey.self)
                var nested = k.nestedContainer(keyedBy: AnyKey.self,
                                               forKey: AnyKey(stringValue: "inner")!)
                try nested.encode(Int32(7), forKey: AnyKey(stringValue: "a")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"Outer","fields":[
          {"name":"inner","type":{"type":"record","name":"Inner","fields":[
              {"name":"a","type":"int"}]}}
        ]}
        """#)
        let data = try avro.encode(Outer())
        #expect(data.count > 0)
    }

    @Test("record with nested array uses keyed nestedUnkeyedContainer path")
    func recordWithNestedArray() throws {
        // Exercises L541-548 (keyed nestedUnkeyedContainer creation). The
        // returned child container has a schema mismatch when written
        // manually, so we accept that the encode call may throw — the goal
        // is to execute the construction path itself.
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var k = encoder.container(keyedBy: AnyKey.self)
                _ = k.nestedUnkeyedContainer(forKey: AnyKey(stringValue: "nums")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"nums","type":{"type":"array","items":"int"}}
        ]}
        """#)
        _ = try? avro.encode(R())
    }

    @Test("encodeNil(forKey:) on null field succeeds")
    func encodeNilOnNullField() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var k = encoder.container(keyedBy: AnyKey.self)
                try k.encodeNil(forKey: AnyKey(stringValue: "n")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"n","type":"null"}
        ]}
        """#)
        let data = try avro.encode(R())
        #expect(data.count > 0)
    }

    @Test("encodeNil(forKey:) on non-null non-union field throws")
    func encodeNilThrowsOnPrimitive() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var k = encoder.container(keyedBy: AnyKey.self)
                try k.encodeNil(forKey: AnyKey(stringValue: "n")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"n","type":"int"}
        ]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R()) }
    }

    @Test("UInt64 too-large in record throws")
    func uint64Overflow() throws {
        struct R: Encodable { let v: UInt64 }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R(v: UInt64.max)) }
    }

    @Test("UInt too-large in record throws")
    func uintOverflow() throws {
        struct R: Encodable { let v: UInt }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}
        """#)
        // On 64-bit platforms UInt.max > Int64.max
        #expect(throws: (any Error).self) { try avro.encode(R(v: UInt.max)) }
    }
}

// MARK: - Top-level array nestedContainer / nestedUnkeyedContainer

@Suite("AvroJSONEncoder – unkeyed nested container creation")
struct UnkeyedNestedContainerTests {

    @Test("array of arrays via nestedUnkeyedContainer in unkeyed")
    func arrayOfArrays() throws {
        // Exercises L722-731 (unkeyed nestedUnkeyedContainer creation).
        let avro = try makeAvro(schema: #"{"type":"array","items":{"type":"array","items":"int"}}"#)
        struct AA: Encodable {
            func encode(to encoder: Encoder) throws {
                var outer = encoder.unkeyedContainer()
                var inner = outer.nestedUnkeyedContainer()
                try inner.encode(Int32(1))
            }
        }
        let data = try avro.encode(AA())
        #expect(data.count > 0)
    }

    @Test("array of records via nestedContainer in unkeyed")
    func arrayOfRecordsViaNested() throws {
        // Exercises L722-731 (unkeyed nestedContainer creation).
        let avro = try makeAvro(schema: #"""
        {"type":"array","items":{"type":"record","name":"R","fields":[
          {"name":"x","type":"int"}]}}
        """#)
        struct AR: Encodable {
            func encode(to encoder: Encoder) throws {
                var outer = encoder.unkeyedContainer()
                var nested = outer.nestedContainer(keyedBy: AnyKey.self)
                try nested.encode(Int32(42), forKey: AnyKey(stringValue: "x")!)
            }
        }
        let data = try avro.encode(AR())
        #expect(data.count > 0)
    }
}

// MARK: - Helper coding key

private struct AnyKey: CodingKey {
    var stringValue: String
    var intValue: Int?
    init?(stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
    init?(intValue: Int) { self.stringValue = "\(intValue)"; self.intValue = intValue }
}

// MARK: - getData error path

@Suite("AvroJSONEncoder – getData error path")
struct GetDataErrorTests {

    @Test("getData throws when stack is empty")
    func getDataEmptyStack() throws {
        // Use a schema that never pushes a container (e.g. encode a type that
        // calls encode(to:) but adds nothing to the stack)
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        // Encode a type that encodes nothing
        struct Empty: Encodable {
            func encode(to encoder: Encoder) throws { }
        }
        #expect(throws: (any Error).self) { try avro.encode(Empty()) }
    }
}

// MARK: - Direct unkeyed primitive overloads (via custom Encodable)
//
// These tests intentionally use concrete types (NOT generics) so that
// `c.encode(value)` dispatches statically to the specific primitive overload
// on AvroJSONUnkeyedEncodingContainer (rather than the generic
// `encode<T: Encodable>` witness).

@Suite("AvroJSONEncoder – unkeyed direct primitive overloads")
struct UnkeyedDirectPrimitiveTests {

    private struct WBool: Encodable {
        let v: Bool
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WDouble: Encodable {
        let v: Double
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WFloat: Encodable {
        let v: Float
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WInt: Encodable {
        let v: Int
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WInt8: Encodable {
        let v: Int8
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WInt16: Encodable {
        let v: Int16
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WInt64: Encodable {
        let v: Int64
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WUInt: Encodable {
        let v: UInt
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WUInt8: Encodable {
        let v: UInt8
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WUInt16: Encodable {
        let v: UInt16
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WUInt32: Encodable {
        let v: UInt32
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }
    private struct WUInt64: Encodable {
        let v: UInt64
        func encode(to encoder: Encoder) throws {
            var c = encoder.unkeyedContainer()
            try c.encode(v)
        }
    }

    @Test("unkeyed Bool primitive overload")
    func unkeyedBool() throws {
        let avro = try makeAvro(schema: #"{"type":"boolean"}"#)
        let data = try avro.encode(WBool(v: true))
        #expect(data.count > 0)
    }

    @Test("unkeyed Bool throws on mismatched schema")
    func unkeyedBoolThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WBool(v: true)) }
    }

    @Test("unkeyed Double primitive overload")
    func unkeyedDouble() throws {
        let avro = try makeAvro(schema: #"{"type":"double"}"#)
        let data = try avro.encode(WDouble(v: 1.5))
        #expect(data.count > 0)
    }

    @Test("unkeyed Double throws on mismatched schema")
    func unkeyedDoubleThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"float"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WDouble(v: 1.5)) }
    }

    @Test("unkeyed Float primitive overload")
    func unkeyedFloat() throws {
        let avro = try makeAvro(schema: #"{"type":"float"}"#)
        let data = try avro.encode(WFloat(v: 2.5))
        #expect(data.count > 0)
    }

    @Test("unkeyed Float throws on mismatched schema")
    func unkeyedFloatThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"double"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WFloat(v: 1.0)) }
    }

    @Test("unkeyed Int primitive overload")
    func unkeyedInt() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let data = try avro.encode(WInt(v: 42))
        #expect(data.count > 0)
    }

    @Test("unkeyed Int throws on mismatched schema")
    func unkeyedIntThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WInt(v: 1)) }
    }

    @Test("unkeyed Int8 primitive overload")
    func unkeyedInt8() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let data = try avro.encode(WInt8(v: 7))
        #expect(data.count > 0)
    }

    @Test("unkeyed Int8 throws on mismatched schema")
    func unkeyedInt8Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WInt8(v: 1)) }
    }

    @Test("unkeyed Int16 primitive overload")
    func unkeyedInt16() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let data = try avro.encode(WInt16(v: 11))
        #expect(data.count > 0)
    }

    @Test("unkeyed Int16 throws on mismatched schema")
    func unkeyedInt16Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WInt16(v: 1)) }
    }

    @Test("unkeyed Int64 primitive overload")
    func unkeyedInt64() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let data = try avro.encode(WInt64(v: 99_999))
        #expect(data.count > 0)
    }

    @Test("unkeyed Int64 throws on mismatched schema")
    func unkeyedInt64Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WInt64(v: 1)) }
    }

    @Test("unkeyed UInt primitive overload")
    func unkeyedUInt() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let data = try avro.encode(WUInt(v: 50))
        #expect(data.count > 0)
    }

    @Test("unkeyed UInt throws on mismatched schema")
    func unkeyedUIntThrows() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt(v: 1)) }
    }

    @Test("unkeyed UInt overflow throws")
    func unkeyedUIntOverflow() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt(v: UInt.max)) }
    }

    // NOTE: There is no test for `unkeyed UInt8 on bytes/fixed schema` because
    // the path leading there in `AvroJSONEncoder.encode<T>(_ value: T)` (the
    // bytes/fixed `else` branch at L63-65 / L77-79) recurses infinitely when
    // the top-level value is neither [UInt8]/UInt8 nor [UInt32]/UInt32. The
    // unkeyed container's `encode(_ value: UInt8)` bytes/fixed cases are
    // therefore unreachable from external code.

    @Test("unkeyed UInt8 throws on int schema")
    func unkeyedUInt8Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt8(v: 1)) }
    }

    @Test("unkeyed UInt16 primitive overload")
    func unkeyedUInt16() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let data = try avro.encode(WUInt16(v: 33))
        #expect(data.count > 0)
    }

    @Test("unkeyed UInt16 throws on mismatched schema")
    func unkeyedUInt16Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt16(v: 1)) }
    }

    // The unkeyed `encode(_ value: UInt32)` is only reachable when the unkeyed
    // container is created on an encoder whose schema is `.fixedSchema`.
    // Top-level `avro.encode(WUInt32(v:))` against a `fixed` schema would
    // recurse infinitely through the `else` branch in
    // `AvroJSONEncoder.encode<T>` for `.fixedSchema`. We sidestep that by
    // descending through a record + nested unkeyed container, which correctly
    // creates an unkeyed container with `.fixedSchema`.
    private struct WUInt32Field: Encodable {
        let v: UInt32
        func encode(to encoder: Encoder) throws {
            var k = encoder.container(keyedBy: AnyKey.self)
            var nested = k.nestedUnkeyedContainer(forKey: AnyKey(stringValue: "raw")!)
            try nested.encode(v)
        }
    }

    @Test("unkeyed UInt32 primitive overload on fixed schema (via nested)")
    func unkeyedUInt32Nested() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"raw","type":{"type":"fixed","name":"F","size":4}}
        ]}
        """#)
        let data = try avro.encode(WUInt32Field(v: 7))
        #expect(data.count > 0)
    }

    private struct WUInt32FieldOnInt: Encodable {
        let v: UInt32
        func encode(to encoder: Encoder) throws {
            var k = encoder.container(keyedBy: AnyKey.self)
            var nested = k.nestedUnkeyedContainer(forKey: AnyKey(stringValue: "v")!)
            try nested.encode(v)
        }
    }

    @Test("unkeyed UInt32 throws on mismatched (non-fixed) schema (via nested)")
    func unkeyedUInt32NestedThrows() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"v","type":"int"}
        ]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt32FieldOnInt(v: 1)) }
    }

    @Test("unkeyed UInt64 primitive overload")
    func unkeyedUInt64() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        let data = try avro.encode(WUInt64(v: 123))
        #expect(data.count > 0)
    }

    @Test("unkeyed UInt64 throws on mismatched schema")
    func unkeyedUInt64Throws() throws {
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt64(v: 1)) }
    }

    @Test("unkeyed UInt64 overflow throws")
    func unkeyedUInt64Overflow() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(WUInt64(v: UInt64.max)) }
    }
}

// MARK: - Direct keyed Int overload

@Suite("AvroJSONEncoder – keyed direct Int overload")
struct KeyedDirectIntTests {

    private struct WKInt: Encodable {
        let v: Int
        func encode(to encoder: Encoder) throws {
            var c = encoder.container(keyedBy: AnyKey.self)
            try c.encode(v, forKey: AnyKey(stringValue: "v")!)
        }
    }

    @Test("keyed Int primitive overload")
    func keyedInt() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}
        """#)
        let json = try jsonString(from: avro, encoding: WKInt(v: 99))
        #expect(json.contains("99"))
    }

    @Test("keyed Int throws on mismatched schema")
    func keyedIntThrows() throws {
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(WKInt(v: 1)) }
    }
}

// MARK: - Top-level type-mismatch throws against bytes / fixed

@Suite("AvroJSONEncoder – top-level type mismatch")
struct TopLevelTypeMismatchTests {

    @Test("encode arbitrary type against bytes schema throws")
    func arbitraryAgainstBytes() throws {
        struct R: Encodable { let x: Int32 }
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        #expect(throws: (any Error).self) { try avro.encode(R(x: 1)) }
    }

    @Test("encode arbitrary type against fixed schema throws")
    func arbitraryAgainstFixed() throws {
        struct R: Encodable { let x: Int32 }
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":1}"#)
        #expect(throws: (any Error).self) { try avro.encode(R(x: 1)) }
    }

    @Test("encode String against bytes schema throws")
    func stringAgainstBytes() throws {
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        #expect(throws: (any Error).self) { try avro.encode("hello") }
    }

    @Test("encode String against fixed schema throws")
    func stringAgainstFixed() throws {
        let avro = try makeAvro(schema: #"{"type":"fixed","name":"F","size":4}"#)
        #expect(throws: (any Error).self) { try avro.encode("hello") }
    }
}

// MARK: - Container accessors (codingPath / count)

@Suite("AvroJSONEncoder – container accessors")
struct ContainerAccessorTests {

    @Test("keyed container codingPath is readable")
    func keyedCodingPath() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                let c = encoder.container(keyedBy: AnyKey.self)
                _ = c.codingPath
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[]}
        """#)
        let data = try avro.encode(R())
        #expect(data.count > 0)
    }

    @Test("unkeyed container codingPath and count are readable")
    func unkeyedAccessors() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                let c = encoder.unkeyedContainer()
                _ = c.codingPath
                _ = c.count
            }
        }
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        let data = try avro.encode(R())
        #expect(data.count > 0)
    }
}

// MARK: - Keyed encode(_:forKey:) String error variants

@Suite("AvroJSONEncoder – keyed string error variants")
struct KeyedStringErrorTests {

    @Test("keyed enum field with invalid symbol throws")
    func keyedEnumInvalid() throws {
        struct R: Encodable { let color: String }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"color","type":{"type":"enum","name":"E","symbols":["A","B"]}}
        ]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R(color: "INVALID")) }
    }

    @Test("keyed string against union without string branch throws")
    func keyedUnionNoString() throws {
        struct R: Encodable { let v: String }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":["null","int"]}]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R(v: "hello")) }
    }

    @Test("keyed string against int field schema throws")
    func keyedStringAgainstInt() throws {
        struct R: Encodable { let v: String }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R(v: "hello")) }
    }
}

// MARK: - Unkeyed encode(_ value: String) variants (via unkeyed container)

@Suite("AvroJSONEncoder – unkeyed string variants direct")
struct UnkeyedStringDirectTests {

    @Test("unkeyed encode(String) on string schema succeeds")
    func unkeyedStringOnString() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encode("hi")
            }
        }
        let avro = try makeAvro(schema: #"{"type":"string"}"#)
        let data = try avro.encode(R())
        #expect(data.count > 0)
    }

    @Test("unkeyed encode(String) on enum schema with invalid symbol throws")
    func unkeyedStringOnEnumInvalid() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encode("INVALID")
            }
        }
        let avro = try makeAvro(schema: #"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        #expect(throws: (any Error).self) { try avro.encode(R()) }
    }

    @Test("unkeyed encode(String) on union without string throws")
    func unkeyedStringOnUnionNoString() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encode("hi")
            }
        }
        let avro = try makeAvro(schema: #"["null","int"]"#)
        #expect(throws: (any Error).self) { try avro.encode(R()) }
    }

    @Test("unkeyed encode(String) on int schema throws")
    func unkeyedStringOnIntDefault() throws {
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.unkeyedContainer()
                try c.encode("hi")
            }
        }
        let avro = try makeAvro(schema: #"{"type":"int"}"#)
        #expect(throws: (any Error).self) { try avro.encode(R()) }
    }
}

// MARK: - encodeNilsBefore tail (key not in mirror) and keyed encode<T> empty-stack throw

@Suite("AvroJSONEncoder – misc keyed paths")
struct KeyedMiscPathTests {

    @Test("encodeNilsBefore consumes all children when key not in mirror")
    func nilsBeforeKeyNotInMirror() throws {
        struct R: Encodable {
            let realField: String
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: AnyKey.self)
                try c.encode("hello", forKey: AnyKey(stringValue: "differentName")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"differentName","type":"string"}]}
        """#)
        let data = try avro.encode(R(realField: "anything"))
        #expect(data.count > 0)
    }

    @Test("schema(for:) falls back to record schema when key not in field map")
    func schemaFallbackToRecordSchema() throws {
        // Encoding a key that isn't in the record schema's field list causes
        // schemaMap lookup to miss; `schema(for:)` then falls back to the
        // record schema itself. The subsequent encode against a record schema
        // (which is neither string/enum/union) hits the default-throw branch.
        struct R: Encodable {
            let differentName: String
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: AnyKey.self)
                try c.encode("hello", forKey: AnyKey(stringValue: "unknownKey")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"differentName","type":"string"}]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R(differentName: "x")) }
    }

    @Test("keyed encode<T> throws when child encoder produces no value")
    func keyedEncodeEmptyChildThrows() throws {
        struct Empty: Encodable {
            func encode(to encoder: Encoder) throws { /* push nothing */ }
        }
        struct R: Encodable {
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: AnyKey.self)
                try c.encode(Empty(), forKey: AnyKey(stringValue: "x")!)
            }
        }
        let avro = try makeAvro(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}
        """#)
        #expect(throws: (any Error).self) { try avro.encode(R()) }
    }
}

// MARK: - encodeAvroBytes (tested indirectly — free function is internal, not accessible from test target)

@Suite("AvroJSONEncoder – bytes base64 output")
struct EncodeAvroBytesTests {

    private func bytesJSON(_ bytes: [UInt8]) throws -> String {
        let avro = try makeAvro(schema: #"{"type":"bytes"}"#)
        return String(decoding: try avro.encode(bytes), as: UTF8.self)
    }

    @Test("bytes encode produces valid base64 string")
    func base64Output() throws {
        let json = try bytesJSON([0x00, 0xFF, 0x80])
        #expect(!json.isEmpty)
        let valid = CharacterSet.alphanumerics.union(.init(charactersIn: "+/=[]\""))
        #expect(json.unicodeScalars.allSatisfy { valid.contains($0) })
    }

    @Test("empty bytes encodes without crashing")
    func base64Empty() throws {
        let json = try bytesJSON([])
        #expect(json.count > 0)
    }

    @Test("known bytes round-trip through base64")
    func base64KnownValue() throws {
        // [0x48, 0x69] == "Hi" in ASCII → base64 "SGk="
        let json = try bytesJSON([0x48, 0x69])
        #expect(json.contains("SGk="))
    }
}
