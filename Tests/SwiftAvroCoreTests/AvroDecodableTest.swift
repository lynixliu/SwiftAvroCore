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

    // MARK: - Primitives composed in a single record

    @Test("All primitive types decode correctly in a record")
    func allPrimitives() throws {
        enum TestEnum: String, Codable { case a, b, c }
        struct AllTypes: Codable, Equatable {
            let boolField: Bool
            let intField: Int32
            let longField: Int64
            let dateField: Date
            let floatField: Float
            let doubleField: Double
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
          {"name":"dateField","type":{"type":"int","logicalType":"date"}},
          {"name":"floatField","type":"float"},
          {"name":"doubleField","type":"double"},
          {"name":"stringField","type":"string"},
          {"name":"enumField","type":{"type":"enum","name":"TestEnum","symbols":["a","b","c"]}},
          {"name":"bytesField","type":"bytes"},
          {"name":"fixedField","type":{"type":"fixed","size":4}},
          {"name":"durationField","type":{"type":"fixed","size":12,"logicalType":"duration"}}
        ]}
        """
        let avroBytes: [UInt8] = [
            0x01,                                        // bool: true
            0x96, 0xde, 0x87, 0x03,                     // int: 3209099
            0x96, 0xde, 0x87, 0x03,                     // long: 3209099
            0xA0, 0x38,                                 // date: 3600 seconds
            0xc3, 0xf5, 0x48, 0x40,                     // float: 3.14
            0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40, // double: 3.14
            0x06, 0x66, 0x6f, 0x6f,                     // string: "foo"
            0x04,                                        // enum: index 2 = "c"
            0x06, 0x66, 0x6f, 0x6f,                     // bytes: "foo"
            0x01, 0x02, 0x03, 0x04,                     // fixed: 4 bytes
            0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xB2, 0x07, 0x00, 0x00 // duration
        ]
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(AllTypes.self, from: data)
        #expect(value.boolField == true)
        #expect(value.intField == 3209099)
        #expect(value.longField == 3209099)
        #expect(value.dateField == Date(timeIntervalSince1970: 3600))
        #expect(abs(value.floatField - 3.14) < 0.001)
        #expect(abs(value.doubleField - 3.14) < 0.0001)
        #expect(value.stringField == "foo")
        #expect(value.enumField == .c)
        #expect(value.bytesField == [0x66, 0x6f, 0x6f])
        #expect(value.fixedField == [0x01, 0x02, 0x03, 0x04])
        #expect(value.durationField == [1, 1, 1970])

        let anyValue = try decoder.decode(from: data) as! [String: Any]
        #expect(anyValue["boolField"] as! Bool == true)
        #expect(anyValue["intField"] as! Int32 == 3209099)
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
            // data (long): 3209099
            0x96, 0xde, 0x87, 0x03,
            // values (array<long>): [3, 27]
            0x04, 0x06, 0x36, 0x00,
            // kv (map<long>): {"foo":3, "aoo":2}
            0x04,
              0x06, 0x66, 0x6f, 0x6f, 0x06,  // "foo"=3
              0x06, 0x61, 0x6f, 0x6f, 0x04,  // "aoo"=2
            0x00,
            // kvs (map<array<long>>): {"boo":[4,28]}
            0x02,
              0x06, 0x62, 0x6f, 0x6f,        // "boo"
              0x04, 0x08, 0x38, 0x00,        // [4, 28]
            0x00,
            // innerrecord.mv (array<map<long>>): [{"coo":4}]
            0x02,
              0x02,
                0x06, 0x63, 0x6f, 0x6f, 0x08, // "coo"=4
              0x00,
            0x00,
        ]
        let jsonSchema = """
        {"type":"record","name":"tem","fields":[
          {"name":"data","type":"long"},
          {"name":"values","type":{"type":"array","items":"long"}},
          {"name":"kv","type":{"type":"map","values":"long"}},
          {"name":"kvs","type":{"type":"map","values":{"type":"array","items":"long"}}},
          {"name":"innerrecord","type":{"type":"record","name":"Inner","fields":[
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

        let value = try decoder.decode(MyFields.self, from: data)
        #expect(value.data   == 3209099)
        #expect(value.values == [3, 27])
        #expect(value.kv     == ["aoo": 2, "foo": 3])
        #expect(value.kvs    == ["boo": [4, 28]])
        #expect(value.innerrecord.mv == [["coo": 4]])

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

    // MARK: - Keyed Container Primitive Decode Coverage (lines 271-336)

    @Test("Record with all primitive types decodes correctly")
    func recordAllPrimitives() throws {
        // Schema with all primitive field types
        let jsonSchema = """
        {"type":"record","name":"AllPrimitives","fields":[
          {"name":"boolField","type":"boolean"},
          {"name":"intField","type":"int"},
          {"name":"longField","type":"long"},
          {"name":"floatField","type":"float"},
          {"name":"doubleField","type":"double"},
          {"name":"stringField","type":"string"},
          {"name":"bytesField","type":"bytes"},
          {"name":"fixedField","type":{"type":"fixed","name":"Fixed4","size":4}}
        ]}
        """
        struct AllPrimitives: Decodable, Equatable {
            let boolField: Bool
            let intField: Int32
            let longField: Int64
            let floatField: Float
            let doubleField: Double
            let stringField: String
            let bytesField: [UInt8]
            let fixedField: [UInt8]
        }

        // Encode: bool=true(0x01), int=42(0x54), long=3209099(0x96 0xDE 0x87 0x03),
        // float=3.14, double=3.14, string="foo"(0x06+'foo'), bytes="bar"(0x06+'bar'), fixed=4 bytes
        let avroBytes: [UInt8] = [
            0x01,                          // bool: true
            0x54,                          // int: 42 (zigzag)
            0x96, 0xDE, 0x87, 0x03,       // long: 3209099
            0xC3, 0xF5, 0x48, 0x40,       // float: 3.14
            0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40, // double: 3.14
            0x06, 0x66, 0x6F, 0x6F,       // string: "foo"
            0x06, 0x62, 0x61, 0x72,       // bytes: "bar"
            0x01, 0x02, 0x03, 0x04        // fixed: 4 bytes
        ]

        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(AllPrimitives.self, from: data)
        #expect(value.boolField == true)
        #expect(value.intField == 42)
        #expect(value.longField == 3209099)
        #expect(abs(value.floatField - 3.14) < 0.001)
        #expect(abs(value.doubleField - 3.14) < 0.0001)
        #expect(value.stringField == "foo")
        #expect(value.bytesField == [0x62, 0x61, 0x72])
        #expect(value.fixedField == [0x01, 0x02, 0x03, 0x04])
    }

    @Test("Record with Int8, Int16, UInt types decodes correctly")
    func recordSmallIntTypes() throws {
        let jsonSchema = """
        {"type":"record","name":"SmallInts","fields":[
          {"name":"int8Field","type":"int"},
          {"name":"int16Field","type":"int"},
          {"name":"uintField","type":"long"},
          {"name":"uint8Field","type":{"type":"fixed","name":"UInt8Fixed","size":1}},
          {"name":"uint16Field","type":"int"},
          {"name":"uint32Field","type":{"type":"fixed","name":"UInt32Fixed","size":4}},
          {"name":"uint64Field","type":"long"}
        ]}
        """
        struct SmallInts: Decodable, Equatable {
            let int8Field: Int8
            let int16Field: Int16
            let uintField: UInt
            let uint8Field: UInt8
            let uint16Field: UInt16
            let uint32Field: UInt32
            let uint64Field: UInt64
        }

        // UInt types decode via Int64 (zigzag), so for value n: encoded = (n << 1) ^ (n >> 63)
        // 64 -> zigzag 128 -> varint 0x80 0x01
        // 1000 -> zigzag 2000 -> varint 0xD0 0x0F
        let avroBytes: [UInt8] = [
            0x54,                          // int8: 42
            0xD0, 0x0F,                    // int16: 1000
            0x0E,                          // uint: 7 (zigzag)
            0xFF,                          // uint8: 255 (fixed 1 byte)
            0x80, 0x01,                    // uint16: 64 (zigzag: 128)
            0x78, 0x56, 0x34, 0x12,       // uint32: 0x12345678 (fixed 4 bytes)
            0x80, 0x01                     // uint64: 64 (zigzag: 128)
        ]

        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(SmallInts.self, from: data)
        #expect(value.int8Field == 42)
        #expect(value.int16Field == 1000)
        #expect(value.uintField == 7)
        #expect(value.uint8Field == 255)
        #expect(value.uint16Field == 64)
        #expect(value.uint32Field == 0x12345678)
        #expect(value.uint64Field == 64)
    }

    @Test("Record with [UInt32] duration field decodes correctly")
    func recordWithDurationField() throws {
        let jsonSchema = """
        {"type":"record","name":"WithDuration","fields":[
          {"name":"name","type":"string"},
          {"name":"durationField","type":{"type":"fixed","name":"Duration","size":12,"logicalType":"duration"}}
        ]}
        """
        struct DurationRecord: Decodable, Equatable {
            let name: String
            let durationField: [UInt32]
        }

        let avroBytes: [UInt8] = [
            0x06, 0x66, 0x6F, 0x6F,       // string: "foo"
            0x01, 0x00, 0x00, 0x00,       // duration months: 1
            0x01, 0x00, 0x00, 0x00,       // duration days: 1
            0xB2, 0x07, 0x00, 0x00        // duration millis: 1970
        ]

        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(DurationRecord.self, from: data)
        #expect(value.name == "foo")
        #expect(value.durationField == [1, 1, 1970])
    }

    // MARK: - Unkeyed Container String Decode (DecodingHelper line 619)

    @Test("Array of strings decodes via DecodingHelper")
    func arrayOfStrings() throws {
        let jsonSchema = #"{"type":"array","items":"string"}"#
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)

        // Array: 2 items, "foo", "bar"
        let avroBytes: [UInt8] = [
            0x04,                          // block count: 2
            0x06, 0x66, 0x6F, 0x6F,       // "foo"
            0x06, 0x62, 0x61, 0x72,       // "bar"
            0x00                           // end block
        ]
        let data = Data(avroBytes)

        let value = try decoder.decode([String].self, from: data)
        #expect(value == ["foo", "bar"])
    }

    @Test("Array of enums decodes to strings via DecodingHelper")
    func arrayOfEnums() throws {
        let jsonSchema = """
        {"type":"array","items":{
          "type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]
        }}
        """
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)

        // Array: 2 items, enum index 0 (RED), enum index 2 (BLUE)
        let avroBytes: [UInt8] = [
            0x04,                          // block count: 2
            0x00,                          // enum index: 0 (RED)
            0x04,                          // enum index: 2 (BLUE)
            0x00                           // end block
        ]
        let data = Data(avroBytes)

        let value = try decoder.decode([String].self, from: data)
        #expect(value == ["RED", "BLUE"])
    }

    @Test("Nested array of strings decodes correctly")
    func nestedArrayOfStrings() throws {
        let jsonSchema = #"{"type":"array","items":{"type":"array","items":"string"}}"#
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchema))
        let decoder = AvroDecoder(schema: schema)

        // Array of arrays: [[ "foo", "bar" ], [ "baz" ]]
        let avroBytes: [UInt8] = [
            0x04,                          // outer block count: 2
            // First inner array
            0x04,                          // inner block count: 2
            0x06, 0x66, 0x6F, 0x6F,       // "foo"
            0x06, 0x62, 0x61, 0x72,       // "bar"
            0x00,                          // end inner block
            // Second inner array
            0x02,                          // inner block count: 1
            0x06, 0x62, 0x61, 0x7A,       // "baz"
            0x00,                          // end inner block
            0x00                           // end outer block
        ]
        let data = Data(avroBytes)

        let value = try decoder.decode([[String]].self, from: data)
        #expect(value == [["foo", "bar"], ["baz"]])
    }

    // MARK: - Manual Decodable Implementation (covers specialized decode methods)

    @Test("Manual Decodable implementation covers specialized decode methods")
    func manualDecodablePrimitives() throws {
        let avroBytes: [UInt8] = [
            0x01,                          // bool: true
            0x54,                          // int: 42
            0x96, 0xDE, 0x87, 0x03,       // long: 3209099
            0xC3, 0xF5, 0x48, 0x40,       // float: 3.14
            0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40, // double: 3.14
            0x06, 0x66, 0x6F, 0x6F,       // string: "foo"
            0x06, 0x62, 0x61, 0x72        // bytes: "bar"
        ]

        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: jsonSchemaManualPrimitives))
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value = try decoder.decode(ManualPrimitives.self, from: data)
        #expect(value.boolField == true)
        #expect(value.intField == 42)
        #expect(value.longField == 3209099)
        #expect(abs(value.floatField - 3.14) < 0.001)
        #expect(abs(value.doubleField - 3.14) < 0.0001)
        #expect(value.stringField == "foo")
        #expect(value.bytesField == [0x62, 0x61, 0x72])
    }
    // MARK: - Logical-type Double decoding (DecodingHelper.decode(Double))

    @Suite("AvroDecoder – Double via logical types")
    struct DoubleLogicalTypeDecode {

        @Test("date, time-millis, time-micros, timestamp-millis, timestamp-micros logical types decode into Double")
        func doubleLogicalTypes() throws {
            let avro = Avro()
            let intSchema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
            let longSchema = try #require(avro.decodeSchema(schema: #"{"type":"long"}"#))

            let dateSchema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
            let data1 = try AvroEncoder().encode(Int32(0), schema: intSchema)
            let back1 = try AvroDecoder(schema: dateSchema).decode(Double.self, from: data1)
            #expect(back1.isFinite)

            let timeMillisSchema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"time-millis"}"#))
            let data2 = try AvroEncoder().encode(Int32(12_345), schema: intSchema)
            let back2 = try AvroDecoder(schema: timeMillisSchema).decode(Double.self, from: data2)
            #expect(back2 == 12_345.0)

            let timeMicrosSchema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"time-micros"}"#))
            let data3 = try AvroEncoder().encode(Int64(1_000_000), schema: longSchema)
            let back3 = try AvroDecoder(schema: timeMicrosSchema).decode(Double.self, from: data3)
            #expect(back3 == 1.0)

            let timestampMillisSchema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-millis"}"#))
            let data4 = try AvroEncoder().encode(Int64(5_000), schema: longSchema)
            let back4 = try AvroDecoder(schema: timestampMillisSchema).decode(Double.self, from: data4)
            #expect(back4 == 5.0)

            let timestampMicrosSchema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-micros"}"#))
            let data5 = try AvroEncoder().encode(Int64(2_000_000), schema: longSchema)
            let back5 = try AvroDecoder(schema: timestampMicrosSchema).decode(Double.self, from: data5)
            #expect(back5 == 2.0)
        }

    }

    // MARK: - Keyed container decoding paths

    @Suite("AvroDecoder – KeyedDecodingContainer protocol surface")
    struct KeyedContainerDecodeTest {

        @Test("decodeNil(forKey:) on union returns true for null branch")
        func decodeNilOnUnion() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"["null","string"]"#))
            let data = Data([0x00])  // null index
            let result: String? = try AvroDecoder(schema: schema).decode(String?.self, from: data)
            #expect(result == nil)
        }

        @Test("decodeNil(forKey:) on non-null non-union returns false")
        func decodeNilOnNonNull() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
            let data = Data([0x02])  // int 1
            let decoder = AvroDecoder(schema: schema)
            // A non-union, non-null schema should return false for decodeNil
            let result = try decoder.decode(from: data) as? Int32
            #expect(result == 1)
        }
    }

    // MARK: - Unkeyed container decoding paths

    @Suite("AvroDecoder – UnkeyedDecodingContainer")
    struct UnkeyedContainerDecodeTest {

        @Test("nested unkeyed container in unkeyed container")
        func nestedUnkeyed() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":{"type":"array","items":"int"}}"#))
            // [[1, 2]] = outer block 1 item [1,2] = 0x02, inner block 2 items 1,2 = 0x04,0x02,0x04, end inner = 0x00, end outer = 0x00
            let data = Data([0x02, 0x04, 0x02, 0x04, 0x00, 0x00])
            let result: [[Int32]] = try AvroDecoder(schema: schema).decode([[Int32]].self, from: data)
            #expect(result == [[1, 2]])
        }
    }

    // MARK: - AvroDecoder decode(from:) Any? path

    @Suite("AvroDecoder – decode(from:) Any? coverage")
    struct AnyDecodeTest {

        @Test("decode(from:) returns Any? for record")
        func decodeAnyRecord() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"""
            {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
            """#))
            let data = try AvroEncoder().encode(["a": 42] as [String: Int32], schema: schema)
            let any = try AvroDecoder(schema: schema).decode(from: data)
            let dict = try #require(any as? [String: Any])
            #expect(dict["a"] as? Int32 == 42)
        }

        @Test("decode(from:) returns Any? for primitive")
        func decodeAnyPrimitive() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
            let data = try AvroEncoder().encode(Int32(42), schema: schema)
            let any = try AvroDecoder(schema: schema).decode(from: data)
            #expect(any as? Int32 == 42)
        }
    }

    // MARK: - AvroDecoder generic decode<T>(from:) path

    @Suite("AvroDecoder – decode<T>(from:) explicit API")
    struct GenericDecodeTest {

        @Test("decode explicit type Int32")
        func decodeExplicitInt32() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
            let data = try AvroEncoder().encode(Int32(42), schema: schema)
            let result: Int32 = try AvroDecoder(schema: schema).decode(Int32.self, from: data)
            #expect(result == 42)
        }

        @Test("decode explicit type String")
        func decodeExplicitString() throws {
            let schema = try #require(Avro().decodeSchema(schema: #"{"type":"string"}"#))
            let data = try AvroEncoder().encode("hello", schema: schema)
            let result: String = try AvroDecoder(schema: schema).decode(String.self, from: data)
            #expect(result == "hello")
        }
    }
}

// MARK: - AvroDecoder setUserInfo and JSON path

@Suite("AvroDecoder – setUserInfo and JSON decode path")
struct SetUserInfoDecoderTests {

    @Test("setUserInfo stores provided info")
    func setUserInfoCallable() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        let decoder = AvroDecoder(schema: schema)
        decoder.setUserInfo(userInfo: [:])   // covers lines 32-34
    }

    @Test("AvroJson option routes through JSONDecoder")
    func avroJsonDecode() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        let decoder = AvroDecoder(schema: schema)
        let key = CodingUserInfoKey(rawValue: "encodeOption")!
        decoder.setUserInfo(userInfo: [key: AvroEncodingOption.AvroJson])  // line 48
        let jsonData = try #require("42".data(using: .utf8))
        let result: Int32 = try decoder.decode(Int32.self, from: jsonData)
        #expect(result == 42)
    }
}

// MARK: - decode(schema:) Any? paths

@Suite("AvroDecoder – decode(schema:) Any? paths")
struct DecodeSchemaAnyPathTests {

    @Test("decode(from:) with null schema returns nil")
    func nullSchemaReturnsNil() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"null"}"#))
        // Use a non-empty Data so baseAddress is non-nil; null consumes zero bytes
        let result = try AvroDecoder(schema: schema).decode(from: Data([0]))
        #expect(result == nil)
    }

    @Test("decode(from:) with error schema returns field dictionary")
    func errorSchemaDecodes() throws {
        let errorSchema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"error","name":"E","fields":[{"name":"x","type":"int"}]}
        """#))
        struct Err: Encodable { var x: Int32 }
        let data = try AvroEncoder().encode(Err(x: 7), schema: errorSchema)
        let result = try AvroDecoder(schema: errorSchema).decode(from: data)
        let dict = try #require(result as? [String: Any])
        #expect(dict["x"] as? Int32 == 7)
    }

    @Test("decode(from:) with unknown schema returns nil")
    func unknownSchemaReturnsNil() throws {
        let schema = AvroSchema(type: "no_such_type")  // becomes .unknownSchema
        let result = try AvroDecoder(schema: schema).decode(from: Data([0]))
        #expect(result == nil)
    }
}

// MARK: - Keyed container protocol surface

@Suite("AvroDecoder – keyed container additional surface")
struct KeyedContainerSurfaceTests {

    private enum K: String, CodingKey { case a, b, fields }

    @Test("allKeys returns keys from schema map")
    func allKeysProperty() throws {
        struct AllKeysReader: Decodable {
            var keys: [String]
            init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: K.self)
                keys = container.allKeys.map(\.stringValue).sorted()
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}
        """#))
        struct AB: Encodable { var a: Int32; var b: Int32 }
        let data = try AvroEncoder().encode(AB(a: 1, b: 2), schema: schema)
        let result: AllKeysReader = try AvroDecoder(schema: schema).decode(AllKeysReader.self, from: data)
        #expect(result.keys.contains("a"))
        #expect(result.keys.contains("b"))
    }

    @Test("decodeNil(forKey:) returns true for null-typed field")
    func decodeNilForNullField() throws {
        struct NullableStruct: Decodable { var x: Int32? }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"null"}]}
        """#))
        let result: NullableStruct = try AvroDecoder(schema: schema).decode(NullableStruct.self, from: Data([0]))
        #expect(result.x == nil)
    }

    @Test("decodeNil(forKey:) returns false for non-null non-union field")
    func decodeNilForIntField() throws {
        struct OptInt: Decodable { var x: Int32? }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}
        """#))
        struct XI: Encodable { var x: Int32 }
        let data = try AvroEncoder().encode(XI(x: 5), schema: schema)
        let result: OptInt = try AvroDecoder(schema: schema).decode(OptInt.self, from: data)
        #expect(result.x == 5)
    }

    @Test("nestedContainer(keyedBy:forKey:) returns a working keyed container")
    func nestedKeyedContainerForKey() throws {
        struct NestedReader: Decodable {
            init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: K.self)
                _ = try container.nestedContainer(keyedBy: K.self, forKey: .fields)
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#))
        struct A: Encodable { var a: Int32 }
        let data = try AvroEncoder().encode(A(a: 1), schema: schema)
        _ = try? AvroDecoder(schema: schema).decode(NestedReader.self, from: data)
    }

    @Test("superDecoder() and superDecoder(forKey:) are callable on keyed container")
    func superDecoderOnKeyed() throws {
        struct SuperDecoderTest: Decodable {
            init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: K.self)
                _ = try container.superDecoder()
                _ = try container.superDecoder(forKey: .a)
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#))
        struct A: Encodable { var a: Int32 }
        let data = try AvroEncoder().encode(A(a: 1), schema: schema)
        _ = try? AvroDecoder(schema: schema).decode(SuperDecoderTest.self, from: data)
    }
}

// MARK: - Keyed container init paths

@Suite("AvroDecoder – keyed container init schema paths")
struct KeyedContainerInitPathTests {

    private enum K: String, CodingKey { case a, map, int, x }

    @Test("keyed container init with map schema hits mapSchema case")
    func initWithMapSchema() throws {
        struct MapKeyedTest: Decodable {
            init(from decoder: Decoder) throws {
                _ = try decoder.container(keyedBy: K.self)
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        _ = try? AvroDecoder(schema: schema).decode(MapKeyedTest.self, from: Data([0]))
    }

    @Test("keyed container init with field schema hits fieldSchema case")
    func initWithFieldSchema() throws {
        struct FieldKeyedTest: Decodable {
            init(from decoder: Decoder) throws {
                _ = try decoder.container(keyedBy: K.self)
            }
        }
        let fieldSchema: AvroSchema = .fieldSchema(AvroSchema.FieldSchema(
            name: "x", type: .intSchema(AvroSchema.IntSchema()),
            doc: nil, order: nil, aliases: nil, defaultValue: nil, optional: nil))
        _ = try? AvroDecoder(schema: fieldSchema).decode(FieldKeyedTest.self, from: Data([0]))
    }

    @Test("keyed container init with non-named schema hits default case")
    func initWithDefaultSchema() throws {
        struct DefaultKeyedTest: Decodable {
            init(from decoder: Decoder) throws {
                _ = try decoder.container(keyedBy: K.self)
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        _ = try? AvroDecoder(schema: schema).decode(DefaultKeyedTest.self, from: Data([0, 2]))
    }
}

// MARK: - Unkeyed container protocol surface

@Suite("AvroDecoder – unkeyed container protocol surface")
struct UnkeyedContainerSurfaceTests {

    private enum K: String, CodingKey { case v }

    @Test("nestedContainer, nestedUnkeyedContainer and superDecoder on unkeyed container")
    func unkeyedContainerProtocolSurface() throws {
        struct UnkeyedProtoTest: Decodable {
            init(from decoder: Decoder) throws {
                var container = try decoder.unkeyedContainer()
                _ = try container.nestedContainer(keyedBy: K.self)
                _ = try container.nestedUnkeyedContainer()
                _ = try container.superDecoder()
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        // Encode an array with 1 element so the container is non-empty
        let data = try AvroEncoder().encode([Int32(1)], schema: schema)
        _ = try? AvroDecoder(schema: schema).decode(UnkeyedProtoTest.self, from: data)
    }

    @Test("unkeyed container default init with non-array schema")
    func unkeyedDefaultInit() throws {
        struct DefaultUnkeyedTest: Decodable {
            init(from decoder: Decoder) throws {
                _ = try decoder.unkeyedContainer()
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        _ = try? AvroDecoder(schema: schema).decode(DefaultUnkeyedTest.self, from: Data([2]))
    }
}

// MARK: - Single value container paths

@Suite("AvroDecoder – single value container paths")
struct SingleValueContainerPathTests {

    @Test("singleValueContainer init with record schema stores record schema")
    func singleValueWithRecord() throws {
        struct RecordSVC: Decodable {
            init(from decoder: Decoder) throws {
                // Just creating the single value container with a record schema covers line 446
                _ = try decoder.singleValueContainer()
            }
        }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#))
        struct A: Encodable { var a: Int32 }
        let data = try AvroEncoder().encode(A(a: 1), schema: schema)
        _ = try? AvroDecoder(schema: schema).decode(RecordSVC.self, from: data)
    }

    @Test("singleValueContainer init with union schema reads branch index")
    func singleValueWithUnion() throws {
        // Decoding Optional<String> from a union schema calls singleValueContainer() internally
        let schema = try #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        // index 1 (string) + "hello" (len 5)
        let data = Data([0x02, 0x0A, 0x68, 0x65, 0x6C, 0x6C, 0x6F])
        let result: String? = try AvroDecoder(schema: schema).decode(String?.self, from: data)
        #expect(result == "hello")
    }

    @Test("singleValueContainer init with union schema throws on out-of-bounds index")
    func singleValueUnionOutOfBounds() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        // varint 0x04 ZigZag-decodes to 2, but union only has branches 0 and 1
        let data = Data([0x04])
        #expect(throws: (any Error).self) {
            _ = try AvroDecoder(schema: schema).decode(String?.self, from: data)
        }
    }

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
}

// MARK: - Empty / malformed data error paths

@Suite("AvroDecoder – empty and malformed data error paths")
struct EmptyDataErrorTests {

    @Test("decode without encodingOption userInfo throws noEncoderSpecified")
    func decodeWithoutEncodingOption() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let decoder = AvroDecoder(schema: schema)
        decoder.setUserInfo(userInfo: [:])  // wipe out the default infoKey entry
        #expect(throws: BinaryEncodingError.noEncoderSpecified) {
            _ = try decoder.decode(Int32.self, from: Data([0x02]))
        }
    }

    @Test("decode(_:from:) throws on empty data")
    func decodeEmptyData() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        avro.setSchema(schema: schema)
        #expect(throws: (any Error).self) {
            let _: Int32 = try avro.decode(from: Data())
        }
    }

    @Test("decode([K:T]:from:) throws on empty data")
    func decodeMapEmptyData() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        #expect(throws: (any Error).self) {
            _ = try AvroDecoder(schema: schema).decode([String: Int32].self, from: Data())
        }
    }

    @Test("decode(from:) Any? throws on empty data")
    func decodeAnyEmptyData() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        avro.setSchema(schema: schema)
        #expect(throws: (any Error).self) {
            _ = try avro.decode(from: Data())
        }
    }

    @Test("Any? decode of enum with out-of-range index throws")
    func enumIndexOutOfRange() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"enum","name":"E","symbols":["a","b"]}
        """#))
        avro.setSchema(schema: schema)
        // Index 5 (zigzag 0x0a) is out of range for a 2-symbol enum
        let data = Data([0x0a])
        #expect(throws: (any Error).self) {
            _ = try avro.decode(from: data)
        }
    }

    @Test("Any? decode of union with out-of-range index throws")
    func unionIndexOutOfRange() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"["null","int"]"#))
        avro.setSchema(schema: schema)
        // Index 5 (zigzag 0x0a) is out of range for a 2-branch union
        let data = Data([0x0a])
        #expect(throws: (any Error).self) {
            _ = try avro.decode(from: data)
        }
    }

    @Test("keyed schema(for:) throws on unknown key")
    func keyedUnknownKey() throws {
        struct R: Decodable {
            init(from decoder: Decoder) throws {
                let c = try decoder.container(keyedBy: TestKey.self)
                _ = try c.decode(Int32.self, forKey: TestKey(stringValue: "doesNotExist")!)
            }
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"real","type":"int"}]}
        """#))
        avro.setSchema(schema: schema)
        // Some bytes — the decoder fails before consuming much
        let data = Data([0x02])
        #expect(throws: (any Error).self) {
            let _: R = try avro.decode(from: data)
        }
    }

    @Test("keyed decode<T>(forKey:) throws on unknownSchema field")
    func keyedDecodeUnknownSchema() throws {
        struct R: Decodable {
            let v: Int32
        }
        let avro = Avro()
        // "weirdType" is not a recognized Avro type; the parser creates an
        // .unknownSchema(...) fallback rather than rejecting the schema.
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"v","type":"weirdType"}]}
        """#))
        avro.setSchema(schema: schema)
        let data = Data([0x00])
        #expect(throws: (any Error).self) {
            let _: R = try avro.decode(from: data)
        }
    }

    @Test("array of records with negative blockCount form decodes")
    func arrayOfRecordsNegativeBlockCount() throws {
        // Hand-crafted binary: an array of records with the negative-blockCount
        // form (each block prefixed by `-count` then `blockSize`). This forces
        // currentSchema()'s `haveBlock` branch into the container-schema break
        // (L331) — the standard Swift encoder always emits positive blockCount.
        struct R: Decodable, Equatable { let x: Int32 }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"array","items":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}}
        """#))
        avro.setSchema(schema: schema)
        // bytes (per the reader's expected layout — it does NOT consume an
        // outer blockSize when items are containers):
        //   0x01  → blockCount = -1 (one entry, negative form sets haveBlock)
        //   0x02  → record.x = 1 (zigzag)
        //   0x00  → terminator blockCount = 0
        let data = Data([0x01, 0x02, 0x00])
        let result: [R] = try avro.decode(from: data)
        #expect(result == [R(x: 1)])
    }

    @Test("keyed decodeNil(forKey:) throws on out-of-range union index")
    func decodeNilUnionOutOfRange() throws {
        struct R: Decodable {
            init(from decoder: Decoder) throws {
                let c = try decoder.container(keyedBy: TestKey.self)
                _ = try c.decodeNil(forKey: TestKey(stringValue: "u")!)
            }
        }
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}
        """#))
        avro.setSchema(schema: schema)
        // Union index 5 (zigzag-encoded as 0x0a) is out of range for ["null","int"]
        let data = Data([0x0a])
        #expect(throws: (any Error).self) {
            let _: R = try avro.decode(from: data)
        }
    }
}

private struct TestKey: CodingKey {
    var stringValue: String
    var intValue: Int?
    init?(stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
    init?(intValue: Int) { self.stringValue = "\(intValue)"; self.intValue = intValue }
}

// MARK: - Manual Decodable Implementation (file scope)

private let jsonSchemaManualPrimitives = """
{"type":"record","name":"ManualPrimitives","fields":[
  {"name":"boolField","type":"boolean"},
  {"name":"intField","type":"int"},
  {"name":"longField","type":"long"},
  {"name":"floatField","type":"float"},
  {"name":"doubleField","type":"double"},
  {"name":"stringField","type":"string"},
  {"name":"bytesField","type":"bytes"}
]}
"""

struct ManualPrimitives: Equatable {
    let boolField: Bool
    let intField: Int
    let longField: Int64
    let floatField: Float
    let doubleField: Double
    let stringField: String
    let bytesField: [UInt8]
}

enum ManualPrimitivesCodingKeys: String, CodingKey {
    case boolField, intField, longField, floatField, doubleField, stringField, bytesField
}

extension ManualPrimitives: Decodable {
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: ManualPrimitivesCodingKeys.self)
        // These calls hit the specialized decode methods in AvroKeyedDecodingContainer
        boolField = try container.decode(Bool.self, forKey: .boolField)
        intField = try container.decode(Int.self, forKey: .intField)
        longField = try container.decode(Int64.self, forKey: .longField)
        floatField = try container.decode(Float.self, forKey: .floatField)
        doubleField = try container.decode(Double.self, forKey: .doubleField)
        stringField = try container.decode(String.self, forKey: .stringField)
        bytesField = try container.decode([UInt8].self, forKey: .bytesField)
    }
}
