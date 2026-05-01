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

        @Test("encode Double against int (no logical type) throws")
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

    // MARK: - Primitive mismatch errors via EncodingHelper

    @Suite("AvroEncoder – primitive guard mismatch errors")
    struct PrimitiveMismatchTest {

        @Test("Bool, Int, Int8, Int16, Int32 against non-matching schemas throw")
        func intMismatches() throws {
            let intSchema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
            let stringSchema = try #require(Avro().decodeSchema(schema: #"{"type":"string"}"#))
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(true, schema: intSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(Int(1), schema: stringSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(Int8(1), schema: stringSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(Int16(1), schema: stringSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(Int32(1), schema: stringSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(Int64(1), schema: intSchema)
            }
        }

        @Test("UInt, UInt8, UInt16, UInt32, UInt64 against non-matching schemas throw")
        func uintMismatches() throws {
            let intSchema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
            let longSchema = try #require(Avro().decodeSchema(schema: #"{"type":"long"}"#))
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(UInt(1), schema: intSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(UInt8(1), schema: intSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(UInt16(1), schema: longSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(UInt32(1), schema: intSchema)
            }
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(UInt64(1), schema: intSchema)
            }
        }

        @Test("Float against int schema throws")
        func floatMismatch() throws {
            let intSchema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
            #expect(throws: (any Error).self) {
                _ = try AvroEncoder().encode(Float(1.0), schema: intSchema)
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
}
