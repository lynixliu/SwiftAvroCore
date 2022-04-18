//
//  AvroTest/AvroCodableTest.swift
//
//  Created by Yang Liu on 1/09/18.
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
class AvroEnodableTest: XCTestCase {
    var schema: AvroSchema = AvroSchema()
    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
        let schemaJson = """
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
        schema = avro.decodeSchema(schema: schemaJson)!
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
        let encoder = AvroEncoder()
        let falsedata = Data(avroFalseBytes)
        if let flasevalue = try? encoder.encode(false, schema: schema) {
            XCTAssertEqual(falsedata, flasevalue, "Value should be false.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
        let truedata = Data(avroTrueBytes)
        if let truevalue = try? encoder.encode(true, schema: schema) {
            XCTAssertEqual(truevalue, truedata, "Value should be true.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testInt() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"int\" }"
        let source: Int32 = 3209099
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
    
    func testLong() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"long\" }"
        let source: Int64 = 3209099
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
    
    func testTime() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"long\" }"
        let source: Int64 = 3209099
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
    
    func testFloat() {
        let avroBytes: [UInt8] = [0xc3, 0xf5, 0x48, 0x40]
        let jsonSchema = "{ \"type\" : \"float\" }"
        
        let source: Float = 3.14
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
    
    func testDouble() {
        let avroBytes: [UInt8] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40]
        let jsonSchema = "{ \"type\" : \"double\" }"
        
        let source: Double = 3.14
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
    
    func testString() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"string\" }"
        let source = "foo"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let data = Data(avroBytes)
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, data, "Strings don't match.")
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
        let source = ChannelKey.NoChannel
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let data = Data(avroBytes)
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, data, "Strings don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    func testBytes() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"
        struct SS: Codable {
            let value: [UInt8]
            init(_ value: [UInt8]) {
                self.value = value
            }
        }
        let source: [UInt8] = [0x66, 0x6f, 0x6f]
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
    
    func testFixed() {
        let avroBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let jsonSchema = "{ \"type\" : \"fixed\", \"size\" : 4 }"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let data = Data(avroBytes)
        if let value = try? encoder.encode(avroBytes, schema: schema) {
            XCTAssertEqual(value, data, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testDuration() {
        let avroBytes: [UInt32] = [1, 1, 1970]
        let expected: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = "{ \"type\" : \"fixed\", \"size\" : 12, \"logicalType\":\"duration\"}"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        
        let data = Data(expected)
        if let value = try? encoder.encode(avroBytes, schema: schema) {
            XCTAssertEqual(value, data, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testInnerDuration() {
       
        struct myField: Encodable {
            let requestType: [UInt32] = [1, 1, 1970]
        }
        struct Model: Encodable {
            //let requestType: [UInt32] = [1, 1, 1970]
            let fields: myField
        }
        //let avroBytes: [UInt32] = [1, 1, 1970]
        let expected: [UInt8] = [1,0,0,0, 1,0,0,0, 0xB2,0x07,0,0]
        let jsonSchema = """
{"type":"record",
"fields":[
{"name": "requestType", "type": {"type": "fixed", "size": 12, "logicalType":"duration"}}
]}
"""
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let jsonEncoder = JSONEncoder()
        struct myJsonField: Encodable {
            let requestType: [UInt32] = [1, 1, 1970]
            let requestType2: [UInt8] = [1, 1, 0xB2,0x07]
            let bbb: Bool = true
        }
        _ = try? jsonEncoder.encode(myJsonField())
        let data = Data(expected)
        if let value = try? encoder.encode(Model(fields: myField()), schema: schema) {
            XCTAssertEqual(value, data, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testArray() {
        let avroBytes: [UInt8] = [0x04, 0x06, 0x36, 0x00]
        let source: [Int64] = [3, 27]
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"long\" }"
        
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
    
    func testMap() {
        let avroBytes: [UInt8] = [0x04,// block count
            0x06, 0x62, 0x6f, 0x6f,// string
            0x04, 0x08, 0x38, 0x00,// array
            0x06, 0x66, 0x6f, 0x6f,// string
            0x04, 0x06, 0x36, 0x00,// array
            0x00]// end of map
        let avroBytes2: [UInt8] = [0x04,// block count
            0x06, 0x66, 0x6f, 0x6f,// string
            0x04, 0x06, 0x36, 0x00,// array
            0x06, 0x62, 0x6f, 0x6f,// string
            0x04, 0x08, 0x38, 0x00,// array
            0x00]
        let source: [String : [Int64]] = ["boo": [4, 28], "foo": [3, 27]]
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : {\"type\": \"array\", \"items\": \"long\"} }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let data = Data(avroBytes)
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertTrue((value == data || value == Data(avroBytes2)), "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    func testUnion() {
        let jsonSchema = "[\"null\",\"string\"]"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        struct arg {
        let avroBytes: [UInt8]
        let source: String?
        }
        for a in [arg(avroBytes: [0x02, 0x02, 0x61], source: "a"),
                  arg(avroBytes: [0x00], source: nil)] {
            let data = Data(a.avroBytes)
            if let value = try? encoder.encode(a.source, schema: schema) {
                XCTAssertEqual(value, data, "Byte arrays don't match.")
            } else {
                XCTAssert(false, "Failed. Nil value")
            }
        }
    }
    
    func testRecord() {
        struct Model: Codable {
            let requestId: Int32
            let requestName: String
            let requestType: [UInt8]
            let parameter: [Int32]
            let parameter2: [String: Int32]
        }
        
        let expected: Data = Data([0x54, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x02, 0x03, 0x04, 0x04, 0x02, 0x04, 0x0, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0])

        let model = Model(requestId: 42, requestName: "hello", requestType: [1,2,3,4], parameter: [1,2], parameter2: ["foo": 2])
        let encoder = AvroEncoder()
        if let data = try? encoder.encode(model, schema: schema) {
            XCTAssertEqual(data, expected)
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testRequest() {
        struct arg {
            let model: HandshakeRequest
            let expected: Data
        }
        let encoder = AvroEncoder()
        let testSchema = Avro().newSchema(schema: MessageConstant.requestSchema)
        for t in [arg(model: HandshakeRequest(clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                     clientProtocol: nil,
                                     serverHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                     meta: nil),
                      expected: Data([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                    0, 0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,0])),
                  arg(model: HandshakeRequest(clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                               clientProtocol: "foo",
                                               serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                               meta: nil),
                                expected: Data([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                                0x02, 0x06, 0x66, 0x6f, 0x6f,
                                                0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,0])),
                  arg(model: HandshakeRequest(clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                               clientProtocol: "foo",
                                               serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                     meta: ["fo":[1,2,3]]),
                                expected: Data([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                                0x02, 0x06, 0x66, 0x6f, 0x6f,
                                                0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                                0x02,0x02, 0x04, 0x66, 0x6f, 0x06, 0x1, 0x2, 0x3, 0x0]))]{
           let data = try! encoder.encode(t.model, schema: testSchema!)
           XCTAssertTrue(data == t.expected)
        }
    }
    
    func testResponse() {
        struct arg {
            let model: HandshakeResponse
            let expected: Data
        }
        let encoder = AvroEncoder()
        let testSchema = Avro().newSchema(schema: MessageConstant.responseSchema)
        for t in [arg(model: HandshakeResponse(match: HandshakeMatch.BOTH, serverProtocol: nil, serverHash: nil),expected: Data([0,0,0,0])),
                  arg(model: HandshakeResponse(match: HandshakeMatch.CLIENT, serverProtocol: "foo", serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf]),
                      expected: Data([2,
                                      0x02,0x06,0x66,0x6f,0x6f,
                                      0x02,0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                      0])),
                  arg(model: HandshakeResponse(match: HandshakeMatch.CLIENT, serverProtocol: "foo", serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                      meta: ["fo":[1,2,3]]),
                      expected: Data([2,
                                      0x02,0x06,0x66,0x6f,0x6f,
                                      0x02,0x01,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
                                      0x02,0x02,0x04,0x66,0x6f,0x06,0x01,0x02,0x03,0x00]))
        ]{
           let data = try! encoder.encode(t.model, schema: testSchema!)
           XCTAssertTrue(data == t.expected)
        }
    }

    func testNestedRecord() {
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
        let schemaJson = """
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
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let model = Model(requestId: 42, requestName: "hello", requestType: [1,2,3,4], parameter: [1,2], parameter2: ["foo": 2])
        let wrapper = Wrapper(message: model, name: "test")
        let encoder = AvroEncoder()
        let data = try! encoder.encode(wrapper, schema: schema)

        let expected: Data = Data([0x54, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x02, 0x03, 0x04, 0x04, 0x02, 0x04, 0x0, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x0, 0x08, 0x74, 0x65, 0x73, 0x74])
        XCTAssertEqual(data, expected)
    }
    
    func testNestedRecordJson() {
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
        let schemaJson = """
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
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let model = Model(requestId: 42, requestName: "hello", requestType: [1,2,3,4], parameter: [1,2], parameter2: ["foo": 2])
        let wrapper = Wrapper(message: model, name: "test")
        let encoder = AvroEncoder()
        let codingKey = CodingUserInfoKey(rawValue: "encodeOption")!
        encoder.setUserInfo(userInfo: [codingKey: AvroEncodingOption.AvroJson])
        let data = try! encoder.encode(wrapper, schema: schema)
        let json = String(data: data, encoding: .utf8)

        let expected: String = "{\"message\":{\"requestName\":\"hello\",\"parameter2\":{\"foo\":2},\"requestId\":42,\"parameter\":[1,2],\"requestType\":\"\\u0001\\u0002\\u0003\\u0004\"},\"name\":\"test\"}"
        XCTAssertEqual(json, expected)
    }
    
    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
            for _ in 0...1000 {
                testNestedRecord()
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
