//
//  AvroDecodableTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Decoding")
struct AvroDecodableTests {

    // MARK: - Primitives

    @Test("Boolean false and true decode correctly")
    func boolean() throws {
        let jsonSchema = #"{"type":"boolean"}"#
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)

        let falseValue = try decoder.decode(Bool.self, from: Data([0x0]))
        #expect(!falseValue)

        let trueValue = try decoder.decode(Bool.self, from: Data([0x1]))
        #expect(trueValue)

        let anyValue = try decoder.decode(from: Data([0x1]))
        #expect(anyValue as! Bool)
    }

    @Test("Int decodes correctly")
    func int() throws {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode(Int32.self, from: data)
        #expect(Int(value) == 3209099)

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! Int32 == 3209099)
    }

    @Test("Long decodes correctly")
    func long() throws {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode(Int64.self, from: data)
        #expect(Int(value) == 3209099)

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! Int64 == 3209099)
    }

    @Test("Float decodes correctly")
    func float() throws {
        let avroBytes: [UInt8] = [0xc3, 0xf5, 0x48, 0x40]
        let expected: Float = 3.14
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"float"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode(Float.self, from: data)
        #expect(value == expected)

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! Float == expected)
    }

    @Test("Double decodes correctly")
    func double() throws {
        let avroBytes: [UInt8] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40]
        let expected: Double = 3.14
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"double"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode(Double.self, from: data)
        #expect(value == expected)

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! Double == expected)
    }

    @Test("Date logical type round-trips")
    func date() throws {
        let avroBytes: [UInt8] = [0xA0, 0x38]
        let source: Date = Date(timeIntervalSince1970: 3600)
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
        let encoder = AvroEncoder()
        let data = Data(avroBytes)

        let encoded = try encoder.encode(source, schema: schema)
        #expect(encoded == data)

        let decoder = AvroDecoder(schema: schema)
        let decoded = try decoder.decode(Date.self, from: data)
        #expect(decoded == source)

        let anyDecoded = try decoder.decode(from: data) as? Date
        #expect(anyDecoded == source)
    }

    @Test("Enum decodes to correct symbol")
    func enumDecode() throws {
        let avroBytes: [UInt8] = [0x12]
        let jsonSchema = """
        {"type":"enum","name":"ChannelKey",
         "doc":"Enum of valid channel keys.",
         "symbols":["CityIphone","CityMobileWeb","GiltAndroid","GiltcityCom",
                    "GiltCom","GiltIpad","GiltIpadSafari","GiltIphone",
                    "GiltMobileWeb","NoChannel"]}
        """
        enum ChannelKey: String, Codable {
            case CityIphone, CityMobileWeb, GiltAndroid, GiltcityCom
            case GiltCom, GiltIpad, GiltIpadSafari, GiltIphone
            case GiltMobileWeb, NoChannel
        }
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value: ChannelKey = try decoder.decode(ChannelKey.self, from: data)
        guard case .enumSchema(let attr) = schema else {
            Issue.record("Expected enum schema"); return
        }
        #expect(attr.symbols[9] == value.rawValue)

        let anyValue = try decoder.decode(from: data)
        #expect(attr.symbols[9] == anyValue as! String)
    }

    @Test("String decodes correctly")
    func string() throws {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"string"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode(String.self, from: data)
        #expect(value == "foo")

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! String == "foo")
    }

    @Test("Bytes decodes correctly")
    func bytes() throws {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"bytes"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode([UInt8].self, from: data)
        #expect(value == [0x66, 0x6f, 0x6f])

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! [UInt8] == [0x66, 0x6f, 0x6f])
    }

    @Test("Fixed decodes correctly")
    func fixed() throws {
        let avroBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","size":4}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode([UInt8].self, from: data)
        #expect(value == [0x01, 0x02, 0x03, 0x04])

        let anyValue = try decoder.decode(from: data)
        #expect(anyValue as! [UInt8] == [0x01, 0x02, 0x03, 0x04])
    }

    @Test("Duration logical type decodes correctly")
    func duration() throws {
        let expected: [UInt32] = [1, 1, 1970]
        let avroBytes: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","size":12,"logicalType":"duration"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode([UInt32].self, from: data)
        #expect(value == expected)

        let anyValue = try decoder.decode(from: data) as? [UInt32]
        #expect(anyValue == expected)
    }

    @Test("Inner duration field decodes correctly")
    func innerDuration() throws {
        struct Model: Decodable {
            struct MyFields: Decodable { let requestType: [UInt32] }
            let fields: MyFields
        }
        let expected: [UInt32] = [1, 1, 1970]
        let avroBytes: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = """
        {"type":"record","fields":[
          {"name":"requestType","type":{"type":"fixed","size":12,"logicalType":"duration"}}
        ]}
        """
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value    = try decoder.decode(Model.self, from: data)
        #expect(value.fields.requestType == expected)

        let anyValue = try decoder.decode(from: data) as! [String: [UInt32]]?
        #expect(anyValue == ["requestType": expected])
    }

    @Test("Field with named duration type decodes correctly")
    func field() throws {
        struct MyFields: Decodable { let requestType: [UInt32] }
        let expected: [UInt32] = [1, 1, 1970]
        let avroBytes: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = """
        {"type":"record","fields":[
          {"name":"requestType","type":{"name":"Duration","type":"fixed","size":12,"logicalType":"duration"}}
        ]}
        """
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)

        let value = try decoder.decode(MyFields.self, from: Data(avroBytes))
        #expect(value.requestType == expected)
    }

    // MARK: - Complex types

    @Test("Array decodes correctly", arguments: [
        [UInt8]([0x04, 0x06, 0x36, 0x02, 0x06, 0x00]),
        [UInt8]([0x03, 0x02, 0x06, 0x02, 0x36, 0x01, 0x02, 0x06, 0x00])
    ])
    func array(avroBytes: [UInt8]) throws {
        let expected: [Int64] = [3, 27, 3]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":"long"}"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let values: [Int64] = try decoder.decode([Int64].self, from: data)
        #expect(values == expected)

        let anyValues = try decoder.decode(from: data) as! [Int64]
        #expect(anyValues == expected)
    }

    @Test("Map decodes correctly", arguments: [
        [UInt8]([0x04,
                 0x06,0x66,0x6f,0x6f, 0x04,0x06,0x36,0x00,
                 0x06,0x62,0x6f,0x6f, 0x04,0x08,0x38,0x00,
                 0x02,
                 0x06,0x68,0x6f,0x6f, 0x04,0x06,0x36,0x00,
                 0x00]),
        [UInt8]([0x03, 0x10,
                 0x06,0x66,0x6f,0x6f, 0x04,0x06,0x36,0x00,
                 0x10,
                 0x06,0x62,0x6f,0x6f, 0x04,0x08,0x38,0x00,
                 0x01, 0x10,
                 0x06,0x68,0x6f,0x6f, 0x04,0x06,0x36,0x00,
                 0x00])
    ])
    func map(avroBytes: [UInt8]) throws {
        let expected: [String: [Int64]] = ["foo": [3, 27], "boo": [4, 28], "hoo": [3, 27]]
        let jsonSchema = #"{"type":"map","values":{"type":"array","items":"long"}}"#
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let values = try decoder.decode([String: [Int64]].self, from: data)
        #expect(values.count == expected.count)
        for (k, v) in expected { #expect(values[k] == v) }

        let anyValues = try decoder.decode(from: data) as? [String: [Int64]]
        let unwrapped = try #require(anyValues)
        #expect(unwrapped.count == expected.count)
        for (k, v) in expected { #expect(unwrapped[k]! == v) }
    }

    @Test("Inner empty map round-trips")
    func innerEmptyMap() throws {
        struct Model: Codable {
            var magic: [UInt8]
            var meta:  [String: [UInt8]]
            var sync:  [UInt8]
            init() {
                magic = [1]
                meta  = [:]
                sync  = withUnsafeBytes(of: UUID().uuid) { Array($0) }
            }
        }
        let model = Model()
        let jsonSchema = """
        {"type":"record","name":"org.apache.avro.file.Header","fields":[
          {"name":"magic","type":{"type":"fixed","name":"Magic","size":1}},
          {"name":"meta", "type":{"type":"map","values":"bytes"}},
          {"name":"sync", "type":{"type":"fixed","name":"Sync","size":16}}
        ]}
        """
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let encoded = try avro.encode(model)
        let decoder = AvroDecoder(schema: schema)

        let values = try decoder.decode(Model.self, from: encoded)
        #expect(values.magic == model.magic)
        #expect(values.sync  == model.sync)
        #expect(values.meta.count == 0)

        let anyValues = try decoder.decode(from: encoded) as? [String: Any]
        let unwrapped = try #require(anyValues)
        #expect(unwrapped["magic"] as! [UInt8] == model.magic)
        #expect(unwrapped["meta"]  as! [String: [UInt8]] == model.meta)
        #expect(unwrapped["sync"]  as! [UInt8] == model.sync)
    }

    @Test("Inner map with entries round-trips")
    func innerMap() throws {
        struct Model: Codable {
            var magic: [UInt8]
            var meta:  [String: [UInt8]]
            var sync:  [UInt8]
            init() {
                magic = [1]
                meta  = ["avro.codec": Array("null".utf8), "avro.schema": Array("null".utf8)]
                sync  = withUnsafeBytes(of: UUID().uuid) { Array($0) }
            }
        }
        let model = Model()
        let jsonSchema = """
        {"type":"record","name":"org.apache.avro.file.Header","fields":[
          {"name":"magic","type":{"type":"fixed","name":"Magic","size":1}},
          {"name":"meta", "type":{"type":"map","values":"bytes"}},
          {"name":"sync", "type":{"type":"fixed","name":"Sync","size":16}}
        ]}
        """
        let avro    = Avro()
        let schema  = try #require(avro.decodeSchema(schema: jsonSchema))
        let encoded = try avro.encode(model)
        let decoder = AvroDecoder(schema: schema)

        let values = try decoder.decode(Model.self, from: encoded)
        #expect(values.magic == model.magic)
        #expect(values.meta["avro.codec"]  == model.meta["avro.codec"])
        #expect(values.meta["avro.schema"] == model.meta["avro.schema"])
        #expect(values.sync == model.sync)

        let anyValues = try decoder.decode(from: encoded) as? [String: Any]
        let unwrapped = try #require(anyValues)
        #expect(unwrapped["magic"] as! [UInt8]           == model.magic)
        #expect(unwrapped["meta"]  as! [String: [UInt8]] == model.meta)
        #expect(unwrapped["sync"]  as! [UInt8]           == model.sync)
    }

    @Test("Union with optional string decodes correctly")
    func union() throws {
        let avroBytes: [UInt8] = [0x02, 0x02, 0x61]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"["null","string"]"#))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(String?.self, from: data)
        #expect(value == "a")

        let anyValue = try decoder.decode(from: data) as? String
        #expect(anyValue == "a")
    }

    @Test("Record with union field decodes correctly")
    func recordWithUnion() throws {
        let jsonSchema = """
        {"fields":[
          {"name":"bic","type":["null","string"]},
          {"name":"countryOfBirth","type":"string"},
          {"name":"customerId","type":"string"},
          {"name":"dateOfBirth","type":"string"},
          {"name":"dateOfOpened","type":"string"},
          {"name":"firstName","type":"string"},
          {"name":"lastName","type":"string"},
          {"name":"lineOfBusiness","type":"string"},
          {"name":"placeOfBirth","type":"string"},
          {"name":"title","type":["null","string"]}
        ],"name":"NeuronDemoCustomer","type":"record"}
        """
        struct Model: Codable, Equatable {
            var bic: String?; var countryOfBirth: String; var customerId: String
            var dateOfBirth: String; var dateOfOpened: String; var firstName: String
            var lastName: String; var lineOfBusiness: String; var placeOfBirth: String
            var title: String?
        }
        let expect = Model(bic:"RVOTATACXXX", countryOfBirth:"LU", customerId:"687",
                           dateOfBirth:"1969-11-16", dateOfOpened:"2021-04-11",
                           firstName:"Lara-Sophie", lastName:"Schwab", lineOfBusiness:"CORP",
                           placeOfBirth:"Ried im Innkreis", title:"Mag.")
        let data = Data([0x02,0x16,0x52,0x56,0x4f,0x54,0x41,0x54,0x41,0x43,0x58,0x58,0x58,
                         0x04,0x4c,0x55,0x06,0x36,0x38,0x37,0x14,0x31,0x39,0x36,0x39,0x2d,
                         0x31,0x31,0x2d,0x31,0x36,0x14,0x32,0x30,0x32,0x31,0x2d,0x30,0x34,
                         0x2d,0x31,0x31,0x16,0x4c,0x61,0x72,0x61,0x2d,0x53,0x6f,0x70,0x68,
                         0x69,0x65,0x0c,0x53,0x63,0x68,0x77,0x61,0x62,0x08,0x43,0x4f,0x52,
                         0x50,0x20,0x52,0x69,0x65,0x64,0x20,0x69,0x6d,0x20,0x49,0x6e,0x6e,
                         0x6b,0x72,0x65,0x69,0x73,0x02,0x08,0x4d,0x61,0x67,0x2e])
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)

        let value = try decoder.decode(Model.self, from: data)
        #expect(value == expect)

        let anyValue = try decoder.decode(from: Data(data)) as? [String: Any]
        let unwrapped = try #require(anyValue)
        #expect(unwrapped["bic"]             as? String == expect.bic)
        #expect(unwrapped["countryOfBirth"]  as! String == expect.countryOfBirth)
        #expect(unwrapped["customerId"]      as! String == expect.customerId)
        #expect(unwrapped["title"]           as? String == expect.title)
    }

    @Test("Complex record with nested types decodes correctly")
    func record() throws {
        let avroBytes: [UInt8] = [
            0x96,0xde,0x87,0x3, 0x04,0x06,0x36,0x00,
            0x04, 0x06,0x66,0x6f,0x6f, 0x06, 0x06,0x61,0x6f,0x6f, 0x04, 0x00,
            0x02, 0x06,0x62,0x6f,0x6f, 0x04,0x08,0x38,0x00, 0x00,
            0x02, 0x04, 0x06,0x64,0x6f,0x6f, 0x08, 0x00, 0x00,
        ]
        let jsonSchema = """
        {"type":"record","name":"tem","fields":[
          {"name":"data","type":"long"},
          {"name":"values","type":{"type":"array","items":"long"}},
          {"name":"kv","type":{"type":"map","values":"long"}},
          {"name":"kvs","type":{"type":"map","values":{"type":"array","items":"long"}}},
          {"name":"innerrecord","type":{"type":"record","fields":[
            {"name":"mv","type":{"type":"array","items":{"type":"map","values":"long"}}}
          ]}}
        ]}
        """
        struct Inner:    Decodable { let mv: [[String: Int64]] }
        struct MyFields: Decodable {
            let data: Int64; let values: [Int64]
            let kv: [String: Int64]; let kvs: [String: [Int64]]
            let innerrecord: Inner
        }
        struct Record: Decodable { let fields: MyFields }

        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(Record.self, from: data)
        #expect(value.fields.data   == 3209099)
        #expect(value.fields.values == [3, 27])
        #expect(value.fields.kv     == ["aoo": 2, "foo": 3])
        #expect(value.fields.kvs    == ["boo": [4, 28]])
        #expect(value.fields.innerrecord.mv == [["coo": 4]])

        let anyValue = try decoder.decode(from: data) as? [String: Any]
        let unwrapped = try #require(anyValue)
        #expect(unwrapped["data"]   as! Int64       == 3209099)
        #expect(unwrapped["values"] as! [Int64]     == [3, 27])
        #expect(unwrapped["kv"]     as! [String: Int64] == ["aoo": 2, "foo": 3])
        let inv = unwrapped["innerrecord"] as! [String: Any]
        #expect(inv["mv"] as! [[String: Int64]] == [["coo": 4]])
    }

    @Test("Nested record decodes correctly")
    func nestedRecord() throws {
        let sample = """
        {"name":"Rec","type":"record","fields":[
          {"name":"fel","type":{"name":"Fel","type":"record","fields":[
            {"name":"bea","type":"string"},
            {"name":"WebLogic","type":"string"}
          ]}}
        ]}
        """
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: sample))
        let inner  = try #require(schema.getRecord()?.fields[0].type)
        #expect(inner.getRecord()?.fields[0].name == "bea")
        #expect(inner.getRecord()!.fields[0].type.isString())
        #expect(inner.getRecord()?.fields[1].name == "WebLogic")
        #expect(inner.getRecord()!.fields[1].type.isString())

        struct Rec: Decodable { var fel: Fel; struct Fel: Decodable { var bea: String } }
        let decoder = AvroDecoder(schema: schema)
        let decoded = try decoder.decode(Rec.self, from: Data([0x06, 0x66, 0x6f, 0x6f]))
        #expect(decoded.fel.bea == "foo")
    }
}
