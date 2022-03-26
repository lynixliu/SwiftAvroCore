//
//  AvroTest/AvroTest.swift
//
//  Created by Yang Liu on 24/08/18.
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

import Foundation
import XCTest
//import AvroClient
@testable import SwiftAvroCore
class AvroSchemaCodingTest: XCTestCase {
    var testTarget: Avro? = nil
    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
        super.setUp()
        testTarget = Avro()
        XCTAssertNotNil(testTarget)
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    private func getPrimitivesSamples(_ type: String) -> [String] {
        let samples = ["{\"type\":\"\(type)\"}","\"\(type)\""]
        return samples
    }
    
    private func getLogicalTimeSample(type: String, logicalType: String) -> String {
        let sample = "{\"type\":\"\(type)\",\"logicalType\":\"\(logicalType)\"}"
        return sample
    }
    
    func testNull() {
        let samples = getPrimitivesSamples("null")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isNull())
        XCTAssertTrue(schemaShort!.isNull())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testBoolean() {
        let samples = getPrimitivesSamples("boolean")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isBoolean())
        XCTAssertTrue(schemaShort!.isBoolean())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testInt() {
        let samples = getPrimitivesSamples("int")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isInt())
        XCTAssertTrue(schemaShort!.isInt())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testDate() {
        let sample = getLogicalTimeSample(type: "int", logicalType: "date")
        let schema = testTarget!.decodeSchema(schema: sample)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isInt())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, sample.data(using: .utf8)!)
    }
    
    func testMillisecond() {
        let sample = getLogicalTimeSample(type: "int", logicalType: "time-millis")
        let schema = testTarget!.decodeSchema(schema: sample)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isInt())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, sample.data(using: .utf8)!)
    }
    
    func testLong() {
        let samples = getPrimitivesSamples("long")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isLong())
        XCTAssertTrue(schemaShort!.isLong())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testMicrosecond() {
        let sample = getLogicalTimeSample(type: "long", logicalType: "time-micros")
        let schema = testTarget!.decodeSchema(schema: sample)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isLong())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, sample.data(using: .utf8)!)
    }
    func testTimestampMilli() {
        let sample = getLogicalTimeSample(type: "long", logicalType: "timestamp-millis")
        let schema = testTarget!.decodeSchema(schema: sample)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isLong())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, sample.data(using: .utf8)!)
    }
    func testTimestampMicro() {
        let sample = getLogicalTimeSample(type: "long", logicalType: "timestamp-micros")
        let schema = testTarget!.decodeSchema(schema: sample)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isLong())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, sample.data(using: .utf8)!)
    }
    
    func testFloat() {
        let samples = getPrimitivesSamples("float")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isFloat())
        XCTAssertTrue(schemaShort!.isFloat())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testDouble() {
        let samples = getPrimitivesSamples("double")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isDouble())
        XCTAssertTrue(schemaShort!.isDouble())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testString() {
        let samples = getPrimitivesSamples("string")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isString())
        XCTAssertTrue(schemaShort!.isString())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testBytes() {
        let samples = getPrimitivesSamples("bytes")
        let schema = testTarget!.decodeSchema(schema: samples.first!)
        let schemaShort = testTarget!.decodeSchema(schema: samples.last!)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        XCTAssertNotNil(schema)
        XCTAssertNotNil(schemaShort)
        XCTAssertTrue(schema!.isBytes())
        XCTAssertTrue(schemaShort!.isBytes())
        XCTAssertNotNil(encoded)
        XCTAssertEqual(encoded, samples.last!.data(using: .utf8)!)
    }
    
    func testLogicDecimalBytes() {
        let samples = """
{"scale":2,"precision":4,"type":"bytes","logicalType":"decimal"}
"""
        let schema = testTarget!.decodeSchema(schema: samples)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        let newSchema = testTarget!.decodeSchema(schema: encoded!)
        print(String(data: encoded!, encoding: .utf8)!)
        print(samples)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isBytes())
        XCTAssertNotNil(encoded)
        XCTAssertNotNil(newSchema)
        XCTAssertTrue(newSchema!.isBytes())
        XCTAssertTrue(newSchema!.isDecimal())
    }
    func testEnum() {
        let sample = """
{"type": "enum","name": "Suit","symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}
"""
        let expectedSymbols = ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        let schema = testTarget!.decodeSchema(schema: sample)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isEnum())
        switch schema {
        case .enumSchema(let attributes)? :
            XCTAssertEqual(attributes.name , "Suit", "Unexpected enum name.")
            XCTAssertEqual(attributes.symbols, expectedSymbols, "enum symbols mismatch.")
        default :
            XCTAssert(false, "Enum Test Failed")
        }
    }
    
    func testArray() {
        let sample = "{\"type\": \"array\", \"items\": \"string\"}"
        let schema = testTarget!.decodeSchema(schema: sample)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isArray())
        switch schema {
        case .arraySchema(let attributes)? :
            XCTAssertTrue(attributes.items.isString(), "array items mismatch.")
        default :
            XCTAssert(false, "Array Test Failed")
        }
    }
    
    func testMap() {
        let sample = "{\"type\": \"map\", \"values\": \"long\"}"
        let schema = testTarget!.decodeSchema(schema: sample)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isMap())
        switch schema {
        case .mapSchema(let attributes)? :
            XCTAssertTrue(attributes.values.isLong(), "map values mismatch.")
        default :
            XCTAssert(false, "Map Test Failed")
        }
    }
    
    func testFixed() {
        let sample = "{\"type\": \"fixed\", \"size\": 16, \"name\": \"md5\"}"
        let schema = testTarget!.decodeSchema(schema: sample)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isFixed())
        switch schema {
        case .fixedSchema(let attributes)? :
            XCTAssertTrue(attributes.size == 16, "fixed size mismatch.")
            XCTAssertEqual(attributes.name, "md5", "fixed name mismatch.")
        default :
            XCTAssert(false, "Fixed Test Failed")
        }
    }
    
    func testLogicDecimalFixed() {
        let samples = """
{"scale":2,"precision":4,"type":"fixed","logicalType":"decimal","size":3}
"""
        let schema = testTarget!.decodeSchema(schema: samples)
        let encoded = try? testTarget!.encodeSchema(schema: schema!)
        let newSchema = testTarget!.decodeSchema(schema: encoded!)
        print(String(data: encoded!, encoding: .utf8)!)
        print(samples)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isFixed())
        XCTAssertNotNil(encoded)
        XCTAssertNotNil(newSchema)
        XCTAssertTrue(newSchema!.isFixed())
        XCTAssertTrue(newSchema!.isDecimal())
    }
    
    func testDuration() {
        let sample = "{\"type\": \"fixed\", \"logicalType\": \"duration\", \"size\": 12, \"name\": \"mmddyy\"}"
        let schema = testTarget!.decodeSchema(schema: sample)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isFixed())
        switch schema {
        case .fixedSchema(let attributes)? :
            XCTAssertTrue(attributes.size == 12, "fixed size mismatch.")
            XCTAssertEqual(attributes.name, "mmddyy", "fixed name mismatch.")
            XCTAssertEqual(attributes.logicalType?.rawValue, "duration", "fixed name mismatch.")
        default :
            XCTAssert(false, "Fixed Test Failed")
        }
    }
    
    func testUnion() {
        let sample = "[\"null\", {\"type\":\"fixed\",\"size\": 16, \"name\": \"md5\"}, \"long\"]"
        let schema = testTarget!.decodeSchema(schema: sample)
        XCTAssertNotNil(schema)
        XCTAssertTrue(schema!.isUnion())
    }

    func testRecord() {
        let sample = """
{"type": "record", "name": "Json", "namespace":"org.apache.avro.data",
"fields": [
{"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
{"name": "clientProtocol", "type": ["null", "string"]},
{"name": "serverHash", "type": "MD5"},
{"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]},
{"name": "value","type": ["long","double","string","boolean","null",
{"name": "innerRecord", "type": "record","fields": [{"name": "inner","type": ["string"],"default": "default_value"},{"name": "serverHash", "type": "MD5"}]},
{"name": "innerRecordRef","type": "innerRecord"},
{"type": "array", "items": "string"},
{"type": "map", "values": "long"},
{"name": "Suit", "type": "enum", "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]},
["null", "string"]]
}
]
}
"""
        let schema = testTarget!.decodeSchema(schema: sample)!
        XCTAssertNotNil(schema)
        XCTAssertEqual(schema.getName(),"Json")
        XCTAssertEqual(schema.getFullname(),"org.apache.avro.data.Json")
        let r = schema.getRecord()
        XCTAssertEqual(r?.fields.count, 5)
        XCTAssertEqual(r?.fields[0].name, "clientHash")
        XCTAssertTrue(r!.fields[0].type.isFixed())
        XCTAssertEqual(r!.fields[0].type.getName(),"MD5")
        XCTAssertEqual(r!.fields[0].type.getFullname(),"org.apache.avro.data.Json.clientHash.MD5")
        XCTAssertEqual(r!.fields[0].type.getFixedSize(),16)
        XCTAssertEqual(r?.fields[1].name, "clientProtocol")
        XCTAssertTrue(r!.fields[1].type.isUnion())
        let unionList = r!.fields[1].type.getUnionList()
        XCTAssertEqual(unionList.count, 2)
        XCTAssertTrue(unionList[0].isNull())
        XCTAssertTrue(unionList[1].isString())
        XCTAssertEqual(r?.fields[2].name, "serverHash")
        XCTAssertTrue(r!.fields[2].type.isFixed())
        XCTAssertEqual(r?.fields[3].name, "meta")
        XCTAssertTrue(r!.fields[3].type.isUnion())
        let metaList = r!.fields[3].type.getUnionList()
        XCTAssertEqual(metaList.count, 2)
        XCTAssertTrue(metaList[0].isNull())
        XCTAssertTrue(metaList[1].isMap())
        XCTAssertTrue(metaList[1].getMapValues()!.isBytes())
        XCTAssertEqual(r?.fields[4].name, "value")
        XCTAssertTrue(r!.fields[4].type.isUnion())
        let valueList = r!.fields[4].type.getUnionList()
        XCTAssertEqual(valueList.count, 11)
        XCTAssertTrue(valueList[0].isLong())
        XCTAssertTrue(valueList[1].isDouble())
        XCTAssertTrue(valueList[2].isString())
        XCTAssertTrue(valueList[3].isBoolean())
        XCTAssertTrue(valueList[4].isNull())
        XCTAssertTrue(valueList[5].isRecord())
        XCTAssertEqual(valueList[5].getName(), "innerRecord")
        XCTAssertEqual(valueList[5].getFullname(), "org.apache.avro.data.Json.value.innerRecord")
        XCTAssertEqual(valueList[5].getRecord()?.fields.count, 2)
        XCTAssertEqual(valueList[5].getRecord()?.fields[0].name, "inner")
        XCTAssertEqual(valueList[5].getRecord()?.fields[1].name, "serverHash")
        let innerRecordTypes = valueList[5].getRecordInnerTypes()
        XCTAssertEqual(innerRecordTypes.count, 2)
        XCTAssertTrue(innerRecordTypes[0].isUnion())
        XCTAssertTrue(innerRecordTypes[0].getUnionList()[0].isString())
        XCTAssertEqual(valueList[5].getRecord()?.fields[0].defaultValue, "default_value")
        XCTAssertTrue(innerRecordTypes[1].isFixed())
        XCTAssertEqual(innerRecordTypes[1].getName(),"MD5")
        XCTAssertEqual(innerRecordTypes[1].getFullname(),"org.apache.avro.data.Json.value.innerRecord.serverHash.MD5")
        XCTAssertTrue(valueList[6].isRecord())
        XCTAssertEqual(valueList[6].getName()!, "innerRecordRef")
        XCTAssertTrue(valueList[7].isArray())
        XCTAssertTrue(valueList[7].getArrayItems()!.isString())
        XCTAssertTrue(valueList[8].isMap())
        XCTAssertTrue(valueList[8].getMapValues()!.isLong())
        XCTAssertTrue(valueList[9].isEnum())
        XCTAssertEqual(valueList[9].getName(), "Suit")
        XCTAssertEqual(valueList[9].getFullname(), "org.apache.avro.data.Json.value.Suit")
        XCTAssertEqual(valueList[9].getEnumSymbols(), ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"])
        XCTAssertTrue(valueList[10].isUnion())
        let lastUnion = valueList[10].getUnionList()
        XCTAssertEqual(lastUnion.count, 2)
        XCTAssertTrue(lastUnion[0].isNull())
        XCTAssertTrue(lastUnion[1].isString())
    }

    func testProtocol() {
        struct Model: Codable {
            let protocolName: String
            let requestName: String
            let requestType: [UInt8]
            let parameter: [Int32]
            let parameter2: [String: Int32]
        }
        let schemaJson1 = """
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "doc": "Protocol Greetings",
  "types": [
     {"name": "Greeting", "type": "record", "fields": [{"name": "message", "type": "string"}]},
     {"name": "Curse", "type": "error", "fields": [{"name": "message", "type": "string"}]}],
  "messages": {
    "hello": {
       "doc": "Say hello.",
       "request": [{"name": "greeting", "type": "Greeting" }],
       "response": "Greeting",
       "errors": ["Curse"]
    }
  }
}
"""
        //let expected: Data = Data([0x54, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x02, 0x03, 0x04, 0x04, 0x02, 0x04, 0x0, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0])
        let schema = Avro().decodeSchema(schema: schemaJson1)!
        let encoded = try? Avro().encodeSchema(schema: schema)
        print(String(data: encoded!, encoding: .utf8)!)
        let newSchema = Avro().decodeSchema(schema: encoded!)!
        let encoded2 = try? Avro().encodeSchema(schema: newSchema)
        XCTAssertEqual(encoded, encoded2)
    }
    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
            for _ in 0..<1000 {
            testRecord()
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
