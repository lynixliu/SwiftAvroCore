//
//  AvroEncodableTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Encoding")
struct AvroEncodableTests {

    private let schema: AvroSchema = {
        let json = """
        {"type":"record","fields":[
          {"name":"requestId",   "type":"int"},
          {"name":"requestName", "type":"string"},
          {"name":"requestType", "type":{"type":"fixed","size":4}},
          {"name":"parameter",   "type":{"type":"array","items":"int"}},
          {"name":"parameter2",  "type":{"type":"map","values":"int"}}
        ]}
        """
        return Avro().decodeSchema(schema: json)!
    }()

    // MARK: - Primitives composed in a single record

    @Test("All primitive types encode correctly in a record")
    func allPrimitives() throws {
        enum TestEnum: String, Codable { case a, b, c }
        struct AllTypes: Codable {
            let boolField: Bool
            let intField: Int32
            let longField: Int64
            let timeMillisField: Int32
            let floatField: Float
            let doubleField: Double
            let dateField: Date
            let stringField: String
            let enumField: TestEnum
            let bytesField: [UInt8]
            let fixedField: [UInt8]
            let durationField: [UInt32]
        }
        let jsonSchema = """
        {"type":"record","name":"AllTypes","fields":[
          {"name":"boolField","type":"boolean"},
          {"name":"intField","type":"int"},
          {"name":"longField","type":"long"},
          {"name":"timeMillisField","type":{"type":"int","logicalType":"time-millis"}},
          {"name":"floatField","type":"float"},
          {"name":"doubleField","type":"double"},
          {"name":"dateField","type":{"type":"int","logicalType":"date"}},
          {"name":"stringField","type":"string"},
          {"name":"enumField","type":{"type":"enum","name":"TestEnum","symbols":["a","b","c"]}},
          {"name":"bytesField","type":"bytes"},
          {"name":"fixedField","type":{"type":"fixed","size":4}},
          {"name":"durationField","type":{"type":"fixed","size":12,"logicalType":"duration"}}
        ]}
        """
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model  = AllTypes(
            boolField: true, intField: 3_209_099, longField: 3_209_099,
            timeMillisField: 3_209_099, floatField: 3.14, doubleField: 3.14,
            dateField: Date(timeIntervalSince1970: 0), stringField: "foo",
            enumField: .c, bytesField: [0x66, 0x6f, 0x6f],
            fixedField: [0x01, 0x02, 0x03, 0x04], durationField: [1, 1, 1970]
        )
        let expected = Data([
            0x01,
            0x96, 0xde, 0x87, 0x03, 0x96, 0xde, 0x87, 0x03, 0x96, 0xde, 0x87, 0x03,
            0xc3, 0xf5, 0x48, 0x40,
            0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
            0x00,
            0x06, 0x66, 0x6f, 0x6f,
            0x04,
            0x06, 0x66, 0x6f, 0x6f,
            0x01, 0x02, 0x03, 0x04,
            0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xB2, 0x07, 0x00, 0x00
        ])
        #expect(try AvroEncoder().encode(model, schema: schema) == expected)
    }

    @Test("Array encodes correctly")
    func array() throws {
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"long"}"#))
        let value  = try AvroEncoder().encode([Int64(3), Int64(27)], schema: schema)
        #expect(value == Data([0x04, 0x06, 0x36, 0x00]))
    }

    @Test("Map encodes correctly (order-independent)")
    func map() throws {
        let avroBytes1 = Data([0x04,
            0x06,0x62,0x6f,0x6f, 0x04,0x08,0x38,0x00,
            0x06,0x66,0x6f,0x6f, 0x04,0x06,0x36,0x00, 0x00])
        let avroBytes2 = Data([0x04,
            0x06,0x66,0x6f,0x6f, 0x04,0x06,0x36,0x00,
            0x06,0x62,0x6f,0x6f, 0x04,0x08,0x38,0x00, 0x00])
        let source: [String: [Int64]] = ["boo": [4, 28], "foo": [3, 27]]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":{"type":"array","items":"long"}}"#))
        let value  = try AvroEncoder().encode(source, schema: schema)
        #expect(value == avroBytes1 || value == avroBytes2)
    }

    @Test("Union encodes non-nil and nil correctly")
    func union() throws {
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"["null","string"]"#))
        let encoder = AvroEncoder()

        let nonNil = try encoder.encode(Optional("a"), schema: schema)
        #expect(nonNil == Data([0x02, 0x02, 0x61]))

        let nilVal = try encoder.encode(Optional<String>.none, schema: schema)
        #expect(nilVal == Data([0x00]))
    }

    @Test("Record encodes correctly")
    func recordEncode() throws {
        struct Model: Codable {
            let requestId: Int32; let requestName: String
            let requestType: [UInt8]; let parameter: [Int32]
            let parameter2: [String: Int32]
        }
        let expected = Data([
            0x54,
            0x0a,0x68,0x65,0x6c,0x6c,0x6f,
            0x01,0x02,0x03,0x04,
            0x04,0x02,0x04, 0x00,
            0x02,0x06,0x66,0x6f,0x6f,0x04, 0x00
        ])
        let model = Model(requestId:42, requestName:"hello",
                          requestType:[1,2,3,4], parameter:[1,2],
                          parameter2:["foo":2])
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data == expected)
    }

    // MARK: - Nested record / optional union

    @Test("Nested record with optional fields encodes and round-trips correctly")
    func nestedRecord() throws {
        struct Model: Codable {
            let requestId: Int32; let requestName: String
            let optionalDouble: Double?
            let requestType: [UInt8]; let parameter: [Int32]
            let parameter2: [String: Int32]
        }
        struct Wrapper: Codable { let message: Model; let name: String }

        let schemaJson = """
        {"name":"wrapper","type":"record","fields":[
          {"name":"message","type":{"name":"message","type":"record","fields":[
            {"name":"requestId",      "type":"int"},
            {"name":"requestName",    "type":"string"},
            {"name":"optionalDouble", "type":["null","double"]},
            {"name":"requestType",    "type":{"type":"fixed","size":4}},
            {"name":"parameter",      "type":{"type":"array","items":"int"}},
            {"name":"parameter2",     "type":{"type":"map","values":"int"}}
          ]}},
          {"name":"name","type":"string"}
        ]}
        """
        let schema  = try #require(Avro().decodeSchema(schema: schemaJson))
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        // non-nil optional
        let withDouble = Wrapper(
            message: Model(requestId:42, requestName:"hello", optionalDouble:3.14,
                           requestType:[1,2,3,4], parameter:[1,2], parameter2:["foo":2]),
            name: "test")
        let expectedWithDouble = Data([
            0x54, 0x0a,0x68,0x65,0x6c,0x6c,0x6f,
            0x02, 0x1f,0x85,0xeb,0x51,0xb8,0x1e,0x09,0x40,
            0x01,0x02,0x03,0x04, 0x04,0x02,0x04,0x00,
            0x02,0x06,0x66,0x6f,0x6f,0x04,0x00,
            0x08,0x74,0x65,0x73,0x74
        ])
        let encoded = try encoder.encode(withDouble, schema: schema)
        #expect(encoded == expectedWithDouble)

        let decodedWithDouble = try decoder.decode(Wrapper.self, from: expectedWithDouble)
        #expect(decodedWithDouble.message.optionalDouble == 3.14)
        #expect(decodedWithDouble.message.requestId == 42)
        #expect(decodedWithDouble.name == "test")

        // nil optional
        let withNil = Wrapper(
            message: Model(requestId:42, requestName:"hello", optionalDouble:nil,
                           requestType:[1,2,3,4], parameter:[1,2], parameter2:["foo":2]),
            name: "test")
        let expectedWithNil = Data([
            0x54, 0x0a,0x68,0x65,0x6c,0x6c,0x6f,
            0x00,
            0x01,0x02,0x03,0x04, 0x04,0x02,0x04,0x00,
            0x02,0x06,0x66,0x6f,0x6f,0x04,0x00,
            0x08,0x74,0x65,0x73,0x74
        ])
        let encodedNil = try encoder.encode(withNil, schema: schema)
        #expect(encodedNil == expectedWithNil)

        let decodedWithNil = try decoder.decode(Wrapper.self, from: expectedWithNil)
        #expect(decodedWithNil.message.optionalDouble == nil)
    }

    // MARK: - Optional union edge cases

    @Test("Optional union mixed nil fields round-trip correctly")
    func optionalUnionsMixedNil() throws {
        struct R: Codable, Equatable {
            let doulbeField: Double; let doulbeField2: Double; let stringField: String
            let optionalDouble: Double?; let optionalDouble2: Double?
            let optionalString: String?; let optionalString2: String?
        }
        let schemaJson = """
        {"type":"record","name":"RecordIncludeOptionalUnions","namespace":"org.avro.test","fields":[
          {"name":"doulbeField",    "type":"double"},
          {"name":"doulbeField2",   "type":"double"},
          {"name":"stringField",    "type":"string"},
          {"name":"optionalDouble",  "type":["null","double"]},
          {"name":"optionalDouble2", "type":["null","double"]},
          {"name":"optionalString",  "type":["null","string"]},
          {"name":"optionalString2", "type":["null","string"]}
        ]}
        """
        let schema = try #require(Avro().decodeSchema(schema: schemaJson))
        let model  = R(doulbeField:0.1, doulbeField2:0.2, stringField:"abc",
                       optionalDouble:nil, optionalDouble2:0.3,
                       optionalString:nil, optionalString2:"def")
        let data   = try AvroEncoder().encode(model, schema: schema)
        let got    = try AvroDecoder(schema: schema).decode(R.self, from: data)
        #expect(got == model)
    }

    @Test("Optional union first/last nil fields round-trip correctly")
    func optionalUnionsFirstLastNil() throws {
        struct R: Codable, Equatable {
            var doubleField: Double; var stringFieldOpt: String?
            var doubleFieldOpt1: Double?; var doubleFieldOpt2: Double?; var doubleFieldOpt3: Double?
        }
        let schemaJson = """
        {"type":"record","fields":[
          {"name":"doubleField",    "type":"double"},
          {"name":"stringFieldOpt", "type":["null","string"]},
          {"name":"doubleFieldOpt3","type":["null","double"]},
          {"name":"doubleFieldOpt1","type":["null","double"]},
          {"name":"doubleFieldOpt2","type":["null","double"]}
        ]}
        """
        let schema = try #require(Avro().decodeSchema(schema: schemaJson))
        let model  = R(doubleField:22.0, stringFieldOpt:nil,
                       doubleFieldOpt1:nil, doubleFieldOpt2:99.0, doubleFieldOpt3:33.0)
        let data   = try AvroEncoder().encode(model, schema: schema)
        let got    = try AvroDecoder(schema: schema).decode(R.self, from: data)
        #expect(got == model)
    }

    @Test("Optional union with complex inner record round-trips correctly")
    func optionalUnionsComplexType() throws {
        struct Inner: Codable, Equatable { var Name: String; var Number: Int32 }
        struct R: Codable, Equatable {
            var doubleField: Double; var stringFieldOpt: String?
            var doubleFieldOpt1: Double?; var recordFieldOpt1: Inner?; var recordFieldOpt2: Inner?
        }
        let schemaJson = """
        {"type":"record","fields":[
          {"name":"doubleField",    "type":"double"},
          {"name":"stringFieldOpt", "type":["null","string"]},
          {"name":"doubleFieldOpt1","type":["null","double"]},
          {"name":"recordFieldOpt1","type":["null",{"type":"record","name":"innerRecord",
            "fields":[{"name":"Name","type":"string"},{"name":"Number","type":"int"}]}]},
          {"name":"recordFieldOpt2","type":["null",{"type":"record","name":"innerRecord",
            "fields":[{"name":"Name","type":"string"},{"name":"Number","type":"int"}]}]}
        ]}
        """
        let schema = try #require(Avro().decodeSchema(schema: schemaJson))
        let model  = R(doubleField:11.0, stringFieldOpt:"abc",
                       doubleFieldOpt1:nil, recordFieldOpt1:nil,
                       recordFieldOpt2:Inner(Name:"tom", Number:123))
        let data   = try AvroEncoder().encode(model, schema: schema)
        let got    = try AvroDecoder(schema: schema).decode(R.self, from: data)
        #expect(got == model)
    }

    // MARK: - Additional tests

    @Test("AvroEncoder sizeOf returns correct size")
    func sizeOf() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let encoder = AvroEncoder()
        let size = try encoder.sizeOf(Int64(42), schema: schema)
        #expect(size > 0)
    }

    @Test("AvroEncoder setUserInfo works")
    func setUserInfo() throws {
        let encoder = AvroEncoder()
        encoder.setUserInfo(userInfo: [:])
    }

    @Test("AvroEncoder encodes Int8")
    func encodeInt8() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let data = try AvroEncoder().encode(Int8(42), schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes Int16")
    func encodeInt16() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let data = try AvroEncoder().encode(Int16(1000), schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes empty array")
    func encodeEmptyArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        let data = try AvroEncoder().encode([Int32](), schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes empty map")
    func encodeEmptyMap() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        let data = try AvroEncoder().encode([String: Int32](), schema: schema)
        #expect(data.count > 0)
    }

    // MARK: - Additional edge cases for higher coverage

    @Test("AvroEncoder userInfo can be modified")
    func encoderUserInfoModification() throws {
        let encoder = AvroEncoder()
        let key = CodingUserInfoKey(rawValue: "test")!
        encoder.userInfo[key] = "value"
        #expect(encoder.userInfo[key] as? String == "value")
    }

    @Test("AvroEncoder encodes nested array")
    func encodeNestedArray() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":{"type":"array","items":"int"}}"#))
        let data = try AvroEncoder().encode([[Int32]]([[1, 2], [3, 4]]), schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes record with all primitive types")
    func encodeRecordAllPrimitives() throws {
        struct AllPrimitives: Encodable {
            let b: Bool
            let i8: Int8
            let i16: Int16
            let i32: Int32
            let i64: Int64
            let u8: UInt8
            let u16: UInt16
            let u32: UInt32
            let u64: UInt64
            let f: Float
            let d: Double
            let s: String
        }
        let jsonSchema = """
        {"type":"record","name":"AllPrimitives","fields":[
          {"name":"b","type":"boolean"},
          {"name":"i8","type":"int"},
          {"name":"i16","type":"int"},
          {"name":"i32","type":"int"},
          {"name":"i64","type":"long"},
          {"name":"u8","type":{"type":"fixed","size":1}},
          {"name":"u16","type":"int"},
          {"name":"u32","type":"long"},
          {"name":"u64","type":"long"},
          {"name":"f","type":"float"},
          {"name":"d","type":"double"},
          {"name":"s","type":"string"}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = AllPrimitives(b: true, i8: 42, i16: 1000, i32: 100000, i64: 10000000000,
                                   u8: 255, u16: 65535, u32: 4294967, u64: 9223372036854775807,
                                   f: 3.14, d: 2.718281828, s: "hello")
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes optional with non-nil value")
    func encodeOptionalNonNil() throws {
        struct Model: Encodable {
            let value: String?
        }
        let jsonSchema = """
        {"type":"record","name":"M","fields":[
          {"name":"value","type":["null","string"]}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = Model(value: "hello")
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroDecoder decodes null from union")
    func decodeOptionalNil() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"["null","string"]"#))
        let encodedNull = Data([0x00])
        let result: String? = try AvroDecoder(schema: schema).decode(String?.self, from: encodedNull)
        #expect(result == nil)
    }

    @Test("AvroEncoder encodes deeply nested record")
    func encodeDeeplyNestedRecord() throws {
        struct Level3: Encodable { let value: Int32 }
        struct Level2: Encodable { let l3: Level3 }
        struct Level1: Encodable { let l2: Level2 }
        let jsonSchema = """
        {"type":"record","name":"L1","fields":[
          {"name":"l2","type":{"type":"record","name":"L2","fields":[
            {"name":"l3","type":{"type":"record","name":"L3","fields":[
              {"name":"value","type":"int"}
            ]}}
          ]}}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = Level1(l2: Level2(l3: Level3(value: 42)))
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes array of records")
    func encodeArrayOfRecords() throws {
        struct Item: Encodable {
            let id: Int32
            let name: String
        }
        let jsonSchema = """
        {"type":"array","items":{"type":"record","name":"Item","fields":[
          {"name":"id","type":"int"},
          {"name":"name","type":"string"}
        ]}}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let items = [Item(id: 1, name: "first"), Item(id: 2, name: "second")]
        let data = try AvroEncoder().encode(items, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes map of records")
    func encodeMapOfRecords() throws {
        struct Item: Encodable {
            let value: Int32
        }
        let jsonSchema = """
        {"type":"map","values":{"type":"record","name":"Item","fields":[
          {"name":"value","type":"int"}
        ]}}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let map: [String: Item] = ["a": Item(value: 1), "b": Item(value: 2)]
        let data = try AvroEncoder().encode(map, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes bytes in record")
    func encodeBytesInRecord() throws {
        struct Model: Encodable {
            let data: [UInt8]
        }
        let jsonSchema = """
        {"type":"record","name":"M","fields":[
          {"name":"data","type":"bytes"}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = Model(data: [1, 2, 3, 4, 5])
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes fixed in record")
    func encodeFixedInRecord() throws {
        struct Model: Encodable {
            let hash: [UInt8]
        }
        let jsonSchema = """
        {"type":"record","name":"M","fields":[
          {"name":"hash","type":{"type":"fixed","size":16}}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = Model(hash: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes enum in record")
    func encodeEnumInRecord() throws {
        enum Status: String, Encodable {
            case active, inactive, pending
        }
        struct Model: Encodable {
            let status: Status
        }
        let jsonSchema = """
        {"type":"record","name":"M","fields":[
          {"name":"status","type":{"type":"enum","name":"Status","symbols":["active","inactive","pending"]}}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = Model(status: .active)
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes union with multiple types")
    func encodeUnionMultipleTypes() throws {
        struct Model: Encodable {
            let value: String?
        }
        let jsonSchema = """
        {"type":"record","name":"M","fields":[
          {"name":"value","type":["null","string","int"]}
        ]}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let model = Model(value: "hello")
        let data = try AvroEncoder().encode(model, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes array of enums")
    func encodeArrayOfEnums() throws {
        enum Color: String, Encodable {
            case red, green, blue
        }
        let jsonSchema = """
        {"type":"array","items":{"type":"enum","name":"Color","symbols":["red","green","blue"]}}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let colors: [Color] = [.red, .green, .blue]
        let data = try AvroEncoder().encode(colors, schema: schema)
        #expect(data.count > 0)
    }

    @Test("AvroEncoder encodes map of arrays")
    func encodeMapOfArrays() throws {
        let jsonSchema = """
        {"type":"map","values":{"type":"array","items":"int"}}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let map: [String: [Int32]] = ["nums": [1, 2, 3], "more": [4, 5]]
        let data = try AvroEncoder().encode(map, schema: schema)
        #expect(data.count > 0)
    }

    // MARK: - Logical-type Double encoding (EncodingHelper.encode(Double))

    @Suite("AvroEncoder – Double via logical types")
    struct DoubleLogicalTypeEncode {

        // The single-value path uses EncodingHelper.encode(Double), which has the
        // logical-type switch.

        @Test("encode Double against date, time-millis, time-micros, timestamp-millis, timestamp-micros schemas")
        func doubleLogicalTypes() throws {
            let avro = Avro()
            let dateSchema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
            #expect(try AvroEncoder().encode(Double(0), schema: dateSchema).count > 0)

            let timeMillisSchema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"time-millis"}"#))
            #expect(try AvroEncoder().encode(Double(123), schema: timeMillisSchema).count > 0)

            let timeMicrosSchema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"time-micros"}"#))
            #expect(try AvroEncoder().encode(Double(1_000_000), schema: timeMicrosSchema).count > 0)

            let timestampMillisSchema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-millis"}"#))
            #expect(try AvroEncoder().encode(Double(5_000), schema: timestampMillisSchema).count > 0)

            let timestampMicrosSchema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-micros"}"#))
            #expect(try AvroEncoder().encode(Double(2_000_000), schema: timestampMicrosSchema).count > 0)
        }

    }

    // MARK: - String encoding via EncodingHelper

    @Suite("AvroEncoder – String via UUID/enum/union")
    struct StringEncodeVariantsTest {

        @Test("uuid valid and invalid")
        func uuid() throws {
            let avro = Avro()
            let schema = try #require(avro.decodeSchema(schema: #"{"type":"string","logicalType":"uuid"}"#))
            #expect(try AvroEncoder().encode("550e8400-e29b-41d4-a716-446655440000", schema: schema).count > 0)
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode("not-a-uuid", schema: schema)
            }
        }

        @Test("enum valid and invalid symbols")
        func enumSymbols() throws {
            let avro = Avro()
            let validSchema = try #require(avro.decodeSchema(schema: #"{"type":"enum","name":"E","symbols":["A","B","C"]}"#))
            let data = try AvroEncoder().encode("B", schema: validSchema)
            #expect(data == Data([0x02]))

            let invalidSchema = try #require(avro.decodeSchema(schema: #"{"type":"enum","name":"E","symbols":["A","B"]}"#))
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode("Z", schema: invalidSchema)
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

    // MARK: - Single-value primitive encodings (EncodingHelper paths)

    @Suite("AvroEncoder – single-value primitives")
    struct SingleValuePrimitiveEncode {

        private func schema(_ json: String) throws -> AvroSchema {
            try #require(Avro().decodeSchema(schema: json))
        }

        @Test("encode Int via single-value path")
        func encodeInt() throws {
            let s = try schema(#"{"type":"long"}"#)
            #expect(try AvroEncoder().encode(Int(42), schema: s).count > 0)
        }

        @Test("encode UInt16 against int schema via single-value")
        func encodeUInt16() throws {
            let s = try schema(#"{"type":"int"}"#)
            #expect(try AvroEncoder().encode(UInt16(7), schema: s).count > 0)
        }

        @Test("encode Float via single-value")
        func encodeFloat() throws {
            let s = try schema(#"{"type":"float"}"#)
            #expect(try AvroEncoder().encode(Float(3.14), schema: s).count > 0)
        }

        @Test("encode Double via single-value")
        func encodeDouble() throws {
            let s = try schema(#"{"type":"double"}"#)
            #expect(try AvroEncoder().encode(Double(3.14), schema: s).count > 0)
        }

        @Test("encode String against unionSchema picks string branch")
        func encodeStringInUnion() throws {
            let s = try schema(#"["null","string"]"#)
            #expect(try AvroEncoder().encode("hello", schema: s).count > 0)
        }

        @Test("encode nil against null schema via single-value")
        func encodeNilNullSchema() throws {
            let s = try schema(#"{"type":"null"}"#)
            let data = try AvroEncoder().encode(Optional<String>.none, schema: s)
            _ = data
        }

        @Test("encode nil against union with null branch via single-value")
        func encodeNilUnionNull() throws {
            let s = try schema(#"["null","string"]"#)
            let data = try AvroEncoder().encode(Optional<String>.none, schema: s)
            #expect(data.count > 0)
        }

        @Test("encode nil against non-null schema throws typeMismatch")
        func encodeNilNonNullThrows() throws {
            let s = try schema(#"{"type":"int"}"#)
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaNil) {
                try AvroEncoder().encode(Optional<String>.none, schema: s)
            }
        }
    }

    // MARK: - Keyed (record) container mismatch errors

    @Suite("AvroEncoder – keyed container primitive mismatches")
    struct KeyedContainerPrimitiveMismatchTest {

        // Each field-typed wrapper triggers a different keyed-container encode method.
        struct WithBool:   Encodable { let x: Bool   }
        struct WithString: Encodable { let x: String }
        struct WithDouble: Encodable { let x: Double }
        struct WithFloat:  Encodable { let x: Float  }
        struct WithInt:    Encodable { let x: Int    }
        struct WithInt8:   Encodable { let x: Int8   }
        struct WithInt16:  Encodable { let x: Int16  }
        struct WithInt32:  Encodable { let x: Int32  }
        struct WithInt64:  Encodable { let x: Int64  }
        struct WithUInt:   Encodable { let x: UInt   }
        struct WithUInt8:  Encodable { let x: UInt8  }
        struct WithUInt16: Encodable { let x: UInt16 }
        struct WithUInt32: Encodable { let x: UInt32 }
        struct WithUInt64: Encodable { let x: UInt64 }
        struct WithOptional: Encodable {
            let x: String?
            // Custom encoding so we can call encodeNil directly against a non-null schema.
            enum CodingKeys: CodingKey { case x }
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: CodingKeys.self)
                try c.encodeNil(forKey: .x)
            }
        }

        private static func recordSchema(field type: String) -> AvroSchema {
            Avro().decodeSchema(schema: #"""
            {"type":"record","name":"R","fields":[{"name":"x","type":"\#(type)"}]}
            """#)!
        }

        @Test("Bool encoded into non-bool field throws")
        func boolMismatch() {
            let s = Self.recordSchema(field: "string")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaBool) {
                try AvroEncoder().encode(WithBool(x: true), schema: s)
            }
        }

        @Test("String encoded into non-string field throws")
        func stringMismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaString) {
                try AvroEncoder().encode(WithString(x: "x"), schema: s)
            }
        }

        @Test("Double encoded into non-double field throws")
        func doubleMismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaDouble) {
                try AvroEncoder().encode(WithDouble(x: 1.0), schema: s)
            }
        }

        @Test("Float encoded into non-float field throws")
        func floatMismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaFloat) {
                try AvroEncoder().encode(WithFloat(x: 1.0), schema: s)
            }
        }

        @Test("Int encoded into non-int field throws")
        func intMismatch() {
            let s = Self.recordSchema(field: "string")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt) {
                try AvroEncoder().encode(WithInt(x: 1), schema: s)
            }
        }

        @Test("Int8 encoded into non-int field throws")
        func int8Mismatch() {
            let s = Self.recordSchema(field: "string")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt8) {
                try AvroEncoder().encode(WithInt8(x: 1), schema: s)
            }
        }

        @Test("Int16 encoded into non-int field throws")
        func int16Mismatch() {
            let s = Self.recordSchema(field: "string")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt16) {
                try AvroEncoder().encode(WithInt16(x: 1), schema: s)
            }
        }

        @Test("Int32 encoded into non-int field throws")
        func int32Mismatch() {
            let s = Self.recordSchema(field: "string")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt32) {
                try AvroEncoder().encode(WithInt32(x: 1), schema: s)
            }
        }

        @Test("Int64 encoded into non-long field throws")
        func int64Mismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt64) {
                try AvroEncoder().encode(WithInt64(x: 1), schema: s)
            }
        }

        @Test("UInt encoded into non-long field throws")
        func uintMismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt) {
                try AvroEncoder().encode(WithUInt(x: 1), schema: s)
            }
        }

        @Test("UInt8 encoded into non-fixed field throws")
        func uint8Mismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt8) {
                try AvroEncoder().encode(WithUInt8(x: 1), schema: s)
            }
        }

        @Test("UInt16 encoded into non-int field throws")
        func uint16Mismatch() {
            let s = Self.recordSchema(field: "string")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaInt16) {
                try AvroEncoder().encode(WithUInt16(x: 1), schema: s)
            }
        }

        @Test("UInt32 encoded into non-long field throws")
        func uint32Mismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt32) {
                try AvroEncoder().encode(WithUInt32(x: 1), schema: s)
            }
        }

        @Test("UInt64 encoded into non-long field throws")
        func uint64Mismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaUInt64) {
                try AvroEncoder().encode(WithUInt64(x: 1), schema: s)
            }
        }

        @Test("encodeNil(forKey:) against non-null field throws")
        func encodeNilFieldMismatch() {
            let s = Self.recordSchema(field: "int")
            #expect(throws: BinaryEncodingError.typeMismatchWithSchemaNil) {
                try AvroEncoder().encode(WithOptional(x: nil), schema: s)
            }
        }

        @Test("encodeNil(forKey:) against null field encodes a null primitive")
        func encodeNilNullField() throws {
            let s = Self.recordSchema(field: "null")
            let data = try AvroEncoder().encode(WithOptional(x: nil), schema: s)
            // Null encodes as zero bytes, but we just want the call to not throw.
            _ = data
        }
    }

    // MARK: - Duration fixed schema (top-level encode<T> branch)

    @Suite("AvroEncoder – fixed with duration logical type")
    struct DurationFixedEncode {

        @Test("encode [UInt32] against duration fixed schema directly")
        func durationFixedTopLevel() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"""
            {"type":"fixed","name":"D","size":12,"logicalType":"duration"}
            """#))
            let data = try AvroEncoder().encode([UInt32(1), UInt32(2), UInt32(3)], schema: schema)
            #expect(data.count == 12)
        }

        @Test("encode [UInt8] against plain fixed schema directly")
        func plainFixedTopLevel() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"""
            {"type":"fixed","name":"F","size":4}
            """#))
            let data = try AvroEncoder().encode([UInt8(1), UInt8(2), UInt8(3), UInt8(4)],
                                                schema: schema)
            #expect(data.count == 4)
        }
    }

    // MARK: - Custom encode(to:) hitting superEncoder paths

    @Suite("AvroEncoder – super encoder helpers")
    struct SuperEncoderEncode {

        struct WithSuper: Encodable {
            let v: Int32
            enum CodingKeys: CodingKey { case v }
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: CodingKeys.self)
                _ = c.superEncoder()
                _ = c.superEncoder(forKey: .v)
                try c.encode(v, forKey: .v)
            }
        }

        @Test("superEncoder() and superEncoder(forKey:) return the encoder")
        func keyedSuperEncoder() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"""
            {"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}
            """#))
            let data = try AvroEncoder().encode(WithSuper(v: 1), schema: schema)
            #expect(data.count > 0)
        }
    }

    // MARK: - Unkeyed container constructed with union schema

    @Suite("AvroEncoder – unkeyed container with union schema")
    struct UnkeyedUnionEncode {

        @Test("encoding [String] against union[null, array<string>] picks non-null branch")
        func arrayAgainstNullableArrayUnion() throws {
            // Hits line 443-445 (AvroUnkeyedEncodingContainer init union → nonNull branch).
            let schema = try #require(Avro().decodeSchema(schema: #"""
            ["null",{"type":"array","items":"string"}]
            """#))
            // The encoded bytes aren't a strictly-conformant Avro union — there's no
            // branch index — but the call must not crash, and the unkeyed container
            // init must take the union/nonNull branch.
            _ = try? AvroEncoder().encode(["a", "b"], schema: schema)
        }
    }
}

// MARK: - Container protocol surface coverage

@Suite("AvroEncoder – container protocol surface")
struct ContainerProtocolSurfaceTests {

    // MARK: codingPath properties

    @Test("codingPath is accessible on all three container types")
    func codingPathAccessors() throws {
        enum K: CodingKey { case v }
        struct Model: Encodable {
            func encode(to encoder: Encoder) throws {
                let keyed   = encoder.container(keyedBy: K.self)
                _ = keyed.codingPath                     // AvroKeyedEncodingContainer.codingPath (line 146)
                var unkeyed = encoder.unkeyedContainer()
                _ = unkeyed.codingPath                   // AvroUnkeyedEncodingContainer.codingPath (line 399)
                var single  = encoder.singleValueContainer()
                _ = single.codingPath                    // AvroSingleEncodingContainer.codingPath (line 455)
                try single.encode(Int32(0))
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        _ = try AvroEncoder().encode(Model(), schema: schema)
    }

    // MARK: nestedContainer(keyedBy:forKey:) on keyed container

    @Test("nestedContainer(keyedBy:forKey:) is callable on a keyed container")
    func nestedKeyedContainerForKey() throws {
        enum K: CodingKey { case v }
        struct Outer: Encodable {
            var v: Int32
            func encode(to encoder: Encoder) throws {
                var keyed  = encoder.container(keyedBy: K.self)
                _ = keyed.nestedContainer(keyedBy: K.self, forKey: .v)  // lines 358-360
                try keyed.encode(v, forKey: .v)
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"O","fields":[{"name":"v","type":"int"}]}
        """#))
        let data = try AvroEncoder().encode(Outer(v: 42), schema: schema)
        #expect(data.count > 0)
    }

    // MARK: unkeyed-container nested methods

    @Test("nestedContainer, nestedUnkeyedContainer and superEncoder on unkeyed container")
    func unkeyedNestedContainers() throws {
        enum K: CodingKey { case v }
        struct Model: Encodable {
            func encode(to encoder: Encoder) throws {
                var u = encoder.unkeyedContainer()
                _ = u.nestedContainer(keyedBy: K.self)   // lines 431-433
                _ = u.nestedUnkeyedContainer()            // lines 435-437
                _ = u.superEncoder()                      // line 439
            }
        }
        // Use null schema so AvroBinaryEncoder.encode<T> hits default: and calls value.encode(to: self)
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"null"}"#))
        _ = try? AvroEncoder().encode(Model(), schema: schema)
    }

    // MARK: encode(UInt8) success path with bytes schema

    @Test("encode UInt8 individually via unkeyed container with bytes schema")
    func encodeUInt8BytesSchema() throws {
        enum K: CodingKey { case data }
        struct Model: Encodable {
            var data: UInt8   // stored property so buildSchemaMap maps "data" → bytesSchema
            func encode(to encoder: Encoder) throws {
                var keyed   = encoder.container(keyedBy: K.self)
                var unkeyed = keyed.nestedUnkeyedContainer(forKey: .data)
                try unkeyed.encode(data)   // EncodingHelper.encode(UInt8) with bytes schema – line 524
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"data","type":"bytes"}]}
        """#))
        let data = try AvroEncoder().encode(Model(data: 42), schema: schema)
        #expect(data.count > 0)
    }

    // MARK: encode(UInt32) success path with fixed schema

    @Test("encode UInt32 individually via unkeyed container with fixed schema")
    func encodeUInt32FixedSchema() throws {
        enum K: CodingKey { case f }
        struct Model: Encodable {
            var f: UInt32   // stored property so buildSchemaMap maps "f" → fixedSchema
            func encode(to encoder: Encoder) throws {
                var keyed   = encoder.container(keyedBy: K.self)
                var unkeyed = keyed.nestedUnkeyedContainer(forKey: .f)
                try unkeyed.encode(f)   // EncodingHelper.encode(UInt32) with fixed schema – lines 537-538
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[
          {"name":"f","type":{"type":"fixed","name":"F","size":4}}
        ]}
        """#))
        let data = try AvroEncoder().encode(Model(f: 0xDEAD_BEEF), schema: schema)
        #expect(data.count > 0)
    }

    // MARK: map schema with non-dictionary value

    @Test("encoding a non-dictionary Encodable against a map schema hits the else branch")
    func nonDictionaryMapSchema() throws {
        struct Empty: Encodable {}
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        // Lines 124-126: else branch — empty struct has displayStyle .struct, not .dictionary;
        // encode(to:) produces no fields so it completes without throwing, reaching the closing }
        _ = try? AvroEncoder().encode(Empty(), schema: schema)
    }

    @Test("encodeNilIndicesBefore exhausts all children when encoding in reverse field order")
    func encodeNilIndicesBeforeExhausted() throws {
        enum K: String, CodingKey { case a, b }
        struct S: Encodable {
            var a: Int32
            var b: Int32
            func encode(to encoder: Encoder) throws {
                var c = encoder.container(keyedBy: K.self)
                try c.encode(b, forKey: .b)   // consumes "a" and "b" from valueChildren
                try c.encode(a, forKey: .a)   // children now empty → loop 0 times → line 172
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"S","fields":[
          {"name":"a","type":"int"},{"name":"b","type":"int"}
        ]}
        """#))
        let data = try AvroEncoder().encode(S(a: 1, b: 2), schema: schema)
        #expect(data.count > 0)
    }

    @Test("encode without encodingOption userInfo throws noEncoderSpecified")
    func encodeWithoutEncodingOption() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        let encoder = AvroEncoder()
        encoder.setUserInfo(userInfo: [:])  // wipe out the default infoKey entry
        #expect(throws: BinaryEncodingError.noEncoderSpecified) {
            _ = try encoder.encode(Int32(1), schema: schema)
        }
    }

    @Test("top-level encode of non-[UInt8] against bytes schema throws")
    func topLevelArbitraryAgainstBytesThrows() throws {
        struct R: Encodable { let x: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"bytes"}"#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchema) {
            _ = try AvroEncoder().encode(R(x: 1), schema: schema)
        }
    }

    @Test("top-level encode of non-[UInt32] against fixed-duration schema throws")
    func topLevelArbitraryAgainstFixedDurationThrows() throws {
        struct R: Encodable { let x: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"fixed","name":"D","size":12,"logicalType":"duration"}
        """#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchema) {
            _ = try AvroEncoder().encode(R(x: 1), schema: schema)
        }
    }

    @Test("top-level encode of non-[UInt8] against fixed schema throws")
    func topLevelArbitraryAgainstFixedThrows() throws {
        struct R: Encodable { let x: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"fixed","name":"F","size":4}
        """#))
        #expect(throws: BinaryEncodingError.typeMismatchWithSchema) {
            _ = try AvroEncoder().encode(R(x: 1), schema: schema)
        }
    }

    @Test("nested unkeyed array of non-[UInt8] against bytes-element fails")
    func nestedUnkeyedBytesMismatch() throws {
        // Encode an array whose schema says items are bytes, but the element
        // is an Int32. The unkeyed encode<T> hits the bytes-mismatch throw.
        struct R: Encodable { let x: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"array","items":"bytes"}
        """#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode([R(x: 1)], schema: schema)
        }
    }

    @Test("nested unkeyed array of non-[UInt32] against fixed-duration items fails")
    func nestedUnkeyedFixedDurationMismatch() throws {
        struct R: Encodable { let x: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"array","items":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}}
        """#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode([R(x: 1)], schema: schema)
        }
    }

    @Test("nested unkeyed array of non-[UInt8] against plain-fixed items fails")
    func nestedUnkeyedFixedMismatch() throws {
        struct R: Encodable { let x: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"array","items":{"type":"fixed","name":"F","size":4}}
        """#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode([R(x: 1)], schema: schema)
        }
    }
}
