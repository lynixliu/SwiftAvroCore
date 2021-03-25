//
//  AvroTest/AvroBinaryDecodingTest.swift
//
//  Created by Yang Liu on 5/09/18.
//  Copyright © 2018 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import XCTest
@testable import SwiftAvroCore
class AvroDecodableTest: XCTestCase {

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testBoolean() {
        let avroFalseBytes: [UInt8] = [0x0]
        let avroTrueBytes: [UInt8] = [0x1]
        
        let jsonSchema = "{ \"type\" : \"boolean\" }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let falsedata = Data(avroFalseBytes)
        if let flasevalue = try? decoder.decode(Bool.self, from: falsedata) {
            XCTAssert(!flasevalue, "Value should be false.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
        let truedata = Data(avroTrueBytes)
        if let truevalue = try? decoder.decode(Bool.self, from: truedata) {
            XCTAssert(truevalue, "Value should be true.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testInt() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"int\" }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(Int32.self, from: data) {
            XCTAssertEqual(Int(value), 3209099, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testLong() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"long\" }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(Int64.self, from: data) {
            XCTAssertEqual(Int(value), 3209099, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testFloat() {
        let avroBytes: [UInt8] = [0xc3, 0xf5, 0x48, 0x40]
        let jsonSchema = "{ \"type\" : \"float\" }"
        
        let expected: Float = 3.14
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(Float.self, from: data) {
            XCTAssertEqual(value, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testDouble() {
        let avroBytes: [UInt8] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40]
        let jsonSchema = "{ \"type\" : \"double\" }"
        
        let expected: Double = 3.14
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(Double.self, from: data) {
            XCTAssertEqual(value, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testDate() {
        let avroBytes: [UInt8] = [0x0]
        let jsonSchema = "{ \"type\" : \"int\", \"logicalType\": \"date\" }"
        let source: Date = Date(timeIntervalSince1970: 0)
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let data = Data(avroBytes)
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, data, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testEnum() {
        let avroBytes: [UInt8] = [0x12]
        let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        enum ChannelKey: String, Codable {
            case CityIphone, CityMobileWeb, GiltAndroid, GiltcityCom, GiltCom, GiltIpad, GiltIpadSafari, GiltIphone, GiltMobileWeb, NoChannel
        }
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        let value: ChannelKey = try! decoder.decode(ChannelKey.self, from: data)
        XCTAssertNotNil(value)
        
        switch schema {
        case .enumSchema(let attr):
            XCTAssertEqual(attr.symbols[9], value.rawValue)
        case _:
            XCTAssert(false, "Invalid avro value")
        }
    }
    
    func testString() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"string\" }"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(String.self, from: data) {
            XCTAssertEqual(value, "foo", "Strings don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testBytes() throws {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        let value = try decoder.decode([UInt8].self, from: data)
        XCTAssertEqual(value, [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
    }
    
    func testFixed() {
        let avroBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let jsonSchema = "{ \"type\" : \"fixed\", \"size\" : 4 }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode([UInt8].self, from: data) {
            XCTAssertEqual(value, [0x01, 0x02, 0x03, 0x04], "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testDuration() {
        let expected: [UInt32] = [1, 1, 1970]
        let avroBytes: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = "{ \"type\" : \"fixed\", \"size\" : 12, \"logicalType\":\"duration\"}"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode([UInt32].self, from: data)  {
            XCTAssertEqual(value, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testInnerDuration() {
        struct Model: Decodable {
            struct myFields: Decodable {
                let requestType: [UInt32]
            }
            let fields: myFields
        }
        
        let expected: [UInt32] = [1, 1, 1970]
        let avroBytes: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = """
{"type":"record",
"fields":[
{"name": "requestType", "type": {"type": "fixed", "size": 12, "logicalType":"duration"}}
]}
"""
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(Model.self, from: data) {
            XCTAssertEqual(value.fields.requestType, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testField() {
        struct myFields: Decodable {
            let requestType: [UInt32]
        }
        
        let expected: [UInt32] = [1, 1, 1970]
        let avroBytes: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = """
{"type":"record",
"fields":[
{"name": "requestType", "type": {"type": "fixed", "size": 12, "logicalType":"duration"}}
]}
"""
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(myFields.self, from: data) {
            XCTAssertEqual(value.requestType, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testArray() {
        let avroBytes: [UInt8] = [0x04, 0x06, 0x36, 0x00]
        let expected: [Int64] = [3, 27]
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"long\" }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let values: [Int64] = try? decoder.decode([Int64].self, from: data) {
            XCTAssertEqual(values.count, 2, "Wrong number of elements in array.")
            for idx in 0...1 {
                XCTAssertEqual(values[idx], expected[idx], "Unexpected value.")
                
            }
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testMap() {
        let avroBytes: [UInt8] = [0x04,// block count
                                  0x06, 0x66, 0x6f, 0x6f,// string
                                  0x04, 0x06, 0x36, 0x00,// array
                                  0x06, 0x62, 0x6f, 0x6f,// string
                                  0x04, 0x08, 0x38, 0x00,// array
                                  0x00]// end of map
        let expected: [Int64] = [3, 28]
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : {\"type\": \"array\", \"items\": \"long\"} }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let values = try? decoder.decode([String: [Int64]].self, from: data) {
            XCTAssertEqual(values.count, 2, "Wrong number of elements in map.")
            XCTAssertEqual(values["foo"]![0], expected[0], "Unexpected value.")
            XCTAssertEqual(values["boo"]![1], expected[1], "Unexpected value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testUnion() throws {
        let avroBytes: [UInt8] = [0x02, 0x02, 0x61]
        let jsonSchema = "[\"null\",\"string\"]"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        let value = try decoder.decode(String?.self, from: data)
        XCTAssertEqual(value, "a", "Unexpected string value.")
    }
    
    func testUnionNull() throws {
        let avroBytes: [UInt8] = [0x0]
        let jsonSchema = "[\"null\",\"string\"]"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)

        let value: String? = try decoder.decode(String?.self, from: data)
        XCTAssertEqual(value, nil, "Unexpected nil value.")
        
    }

    func testRecord() throws {
        struct Model: Codable {
            let requestId: Int32
            let requestName: String
            let requestType: [UInt8]
            let parameter: [Int32]
            let parameter2: [String: Int32]
        }
        
        let data: Data = Data([0x54, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x02, 0x03, 0x04, 0x04, 0x02, 0x04, 0x0, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0])

        let jsonSchema = """
                    {"type":"record",
                    "fields":[
                    {"name": "requestId", "type": "int"},
                    {"name": "requestName", "type": "string"},
                    {"name": "requestType", "type": {"type": "fixed", "size": 4}},
                    {"name": "parameter", "type": {"type":"array", "items": "int"}},
                    {"name": "parameter2", "type": {"type":"map", "values": "int"}}
                    ]}
            """
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        
//        let model = Model(requestId: 42, requestName: "hello", requestType: [1,2,3,4], parameter: [1,2], parameter2: ["foo": 2])
        let value = try decoder.decode(Model.self, from: data)
        XCTAssertEqual(value.requestId, 42, "IDs don't match.")
        XCTAssertEqual(value.requestName, "hello", "Names don't match.")
        XCTAssertEqual(value.requestType.count, 4, "Array size don't match.")
        XCTAssertEqual(value.requestType[0], 1, "Bytes don't match.")
        XCTAssertEqual(value.requestType[0], 1, "Bytes don't match.")
        XCTAssertEqual(value.requestType[3], 4, "Bytes don't match.")
        XCTAssertEqual(value.parameter.count, 2, "Array size don't match")
        XCTAssertEqual(value.parameter[0], 1, "Bytes don't match")
        XCTAssertEqual(value.parameter[1], 2, "Bytes don't match")
        XCTAssertEqual(value.parameter2["foo"], 2, "Map values don't match")
    }

    func testNestedRecord() throws {
        struct Model: Codable {
            let requestId: Int32
            let requestName: String
            let requestType: [UInt8]
            let parameter: [Int32]
            let parameter2: [String: Int32]
        }

        struct Wrapper: Codable {
            let message: Model
            let name: String
        }
        let jsonSchema = """
                         {
                         "name": "wrapper", "type": "record", "fields": [
                             { "name": "message", "type": {
                             "name" : "message", "type":"record", "fields":[
                             {"name": "requestId", "type": "int"},
                             {"name": "requestName", "type": "string"},
                             {"name": "requestType", "type": {"type": "fixed", "size": 4}},
                             {"name": "parameter", "type": {"type":"array", "items": "int"}},
                             {"name": "parameter2", "type": {"type":"map", "values": "int"}}]}},
                         {"name": "name", "type": "string"}
                         ]}
                         """
        let data: Data = Data([
            0x54, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x02, 0x03, 0x04,
            0x04, 0x02, 0x04, 0x0, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x0,
            0x08, 0x74, 0x65, 0x73, 0x74
        ])
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)

        let value = try decoder.decode(Wrapper.self, from: data)
        XCTAssertEqual(value.message.requestId, 42, "RequestId doesn't match")
    }
    
    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
            for _ in 0..<1000 {
                do {
                    try testRecord()
                } catch {
                    XCTFail("Failed testing record")
                }
            }
        }
    }

    static var allTests = [
        ("testString", testString),
        ("testBytes", testBytes),
        ("testFixed", testFixed),
        ("testInt", testInt),
        ("testLong", testLong),
        ("testFloat", testFloat),
        ("testDouble", testDouble),
        ("testBoolean", testBoolean),
        ("testEnum", testEnum),
        ("testArray", testArray),
        ("testMap", testMap),
        ("testUnion", testUnion),
        ("testRecord", testRecord),
    ]
}
