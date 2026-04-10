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
        return SwiftAvroCore().decodeSchema(schema: json)!
    }()

    // MARK: - Primitives

    @Test("Boolean encodes correctly")
    func boolean() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"boolean"}"#))
        let encoder = AvroEncoder()
        #expect(try encoder.encode(false, schema: schema) == Data([0x00]))
        #expect(try encoder.encode(true,  schema: schema) == Data([0x01]))
    }

    @Test("Int encodes correctly")
    func int() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let value  = try AvroEncoder().encode(Int32(3_209_099), schema: schema)
        #expect(value == Data([0x96, 0xde, 0x87, 0x03]))
    }

    @Test("Long encodes correctly")
    func long() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let value  = try AvroEncoder().encode(Int64(3_209_099), schema: schema)
        #expect(value == Data([0x96, 0xde, 0x87, 0x03]))
    }

    @Test("time-millis logical type encodes correctly")
    func timeMilis() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"time-millis"}"#))
        let value  = try AvroEncoder().encode(Int32(3_209_099), schema: schema)
        #expect(value == Data([0x96, 0xde, 0x87, 0x03]))
    }

    @Test("Float encodes correctly")
    func float() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"float"}"#))
        let value  = try AvroEncoder().encode(Float(3.14), schema: schema)
        #expect(value == Data([0xc3, 0xf5, 0x48, 0x40]))
    }

    @Test("Double encodes correctly")
    func double() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"double"}"#))
        let value  = try AvroEncoder().encode(Double(3.14), schema: schema)
        #expect(value == Data([0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40]))
    }

    @Test("Date encodes correctly")
    func date() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
        let value  = try AvroEncoder().encode(Date(timeIntervalSince1970: 0), schema: schema)
        #expect(value == Data([0x00]))
    }

    @Test("String encodes correctly")
    func string() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let value  = try AvroEncoder().encode("foo", schema: schema)
        #expect(value == Data([0x06, 0x66, 0x6f, 0x6f]))
    }

    // MARK: - Complex types

    @Test("Enum encodes correctly")
    func enumEncode() throws {
        enum ChannelKey: String, Codable {
            case CityIphone, CityMobileWeb, GiltAndroid, GiltcityCom
            case GiltCom, GiltIpad, GiltIpadSafari, GiltIphone
            case GiltMobileWeb, NoChannel
        }
        let jsonSchema = """
        {"type":"enum","name":"ChannelKey","doc":"Enum of valid channel keys.",
         "symbols":["CityIphone","CityMobileWeb","GiltAndroid","GiltcityCom",
                    "GiltCom","GiltIpad","GiltIpadSafari","GiltIphone",
                    "GiltMobileWeb","NoChannel"]}
        """
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let value  = try AvroEncoder().encode(ChannelKey.NoChannel, schema: schema)
        #expect(value == Data([0x12]))
    }

    @Test("Bytes encodes correctly")
    func bytes() throws {
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"bytes"}"#))
        let value  = try AvroEncoder().encode([UInt8]([0x66, 0x6f, 0x6f]), schema: schema)
        #expect(value == Data([0x06, 0x66, 0x6f, 0x6f]))
    }

    @Test("Fixed encodes correctly")
    func fixed() throws {
        let avroBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","size":4}"#))
        let value  = try AvroEncoder().encode(avroBytes, schema: schema)
        #expect(value == Data(avroBytes))
    }

    @Test("Duration encodes correctly")
    func duration() throws {
        let source:   [UInt32] = [1, 1, 1970]
        let expected: [UInt8]  = [0x01,0x00,0x00,0x00, 0x01,0x00,0x00,0x00, 0xB2,0x07,0x00,0x00]
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","size":12,"logicalType":"duration"}"#))
        let value  = try AvroEncoder().encode(source, schema: schema)
        #expect(value == Data(expected))
    }

    @Test("Inner duration field encodes correctly")
    func innerDuration() throws {
        struct Model: Encodable { let requestType: [UInt32] = [1, 1, 1970] }
        let expected: [UInt8] = [0x01,0x00,0x00,0x00, 0x01,0x00,0x00,0x00, 0xB2,0x07,0x00,0x00]
        let jsonSchema = """
        {"type":"record","fields":[
          {"name":"requestType","type":{"type":"fixed","size":12,"logicalType":"duration"}}
        ]}
        """
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let value  = try AvroEncoder().encode(Model(), schema: schema)
        #expect(value == Data(expected))
    }

    @Test("Array encodes correctly")
    func array() throws {
        let avro   = SwiftAvroCore()
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
        let avro   = SwiftAvroCore()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"map","values":{"type":"array","items":"long"}}"#))
        let value  = try AvroEncoder().encode(source, schema: schema)
        #expect(value == avroBytes1 || value == avroBytes2)
    }

    @Test("Union encodes non-nil and nil correctly")
    func union() throws {
        let avro   = SwiftAvroCore()
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
        let schema  = try #require(SwiftAvroCore().decodeSchema(schema: schemaJson))
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
        let schema = try #require(SwiftAvroCore().decodeSchema(schema: schemaJson))
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
        let schema = try #require(SwiftAvroCore().decodeSchema(schema: schemaJson))
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
        let schema = try #require(SwiftAvroCore().decodeSchema(schema: schemaJson))
        let model  = R(doubleField:11.0, stringFieldOpt:"abc",
                       doubleFieldOpt1:nil, recordFieldOpt1:nil,
                       recordFieldOpt2:Inner(Name:"tom", Number:123))
        let data   = try AvroEncoder().encode(model, schema: schema)
        let got    = try AvroDecoder(schema: schema).decode(R.self, from: data)
        #expect(got == model)
    }
}
