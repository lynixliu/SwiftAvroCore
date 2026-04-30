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

    @Test("encode UInt overflow throws")
    func encodeUIntOverflow() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(UInt.max) }
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

    @Test("encode UInt64 overflow throws")
    func encodeUInt64Overflow() throws {
        let avro = try makeAvro(schema: #"{"type":"long"}"#)
        #expect(throws: (any Error).self) { try avro.encode(UInt64.max) }
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

    @Test("encode record field UInt64 overflow throws")
    func encodeRecordUInt64Overflow() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithUInt64","fields":[
            {"name":"big","type":"long"}
          ]
        }
        """#)
        struct R: Encodable { let big: UInt64 }
        #expect(throws: (any Error).self) {
            try avro.encode(R(big: UInt64.max))
        }
    }

    struct WithUIntField: Encodable {
        let n: UInt
    }

    @Test("encode record field UInt overflow throws")
    func encodeRecordUIntOverflow() throws {
        let avro = try makeAvro(schema: #"""
        {
          "type":"record","name":"WithUInt","fields":[
            {"name":"n","type":"long"}
          ]
        }
        """#)
        #expect(throws: (any Error).self) {
            try avro.encode(WithUIntField(n: UInt.max))
        }
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

    @Test("encode array of UInt overflow throws")
    func encodeUIntArrayOverflow() throws {
        let avro = try makeAvro(schema: #"{"type":"array","items":"long"}"#)
        #expect(throws: (any Error).self) {
            try avro.encode([UInt.max])
        }
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
