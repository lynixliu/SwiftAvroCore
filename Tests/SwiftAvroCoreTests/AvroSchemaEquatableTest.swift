//
//  AvroTest/AvroSchemaEquatableTest.swift
//
//  Created by Yang Liu on 30/08/18.
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
class AvroSchemaEquatableTest: XCTestCase {

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }
    private func testEquable(same1: String, same2: String, diff: String) {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        let avro = Avro()
        let schema1 = avro.decodeSchema(schema: same1)
        let schema2 = avro.decodeSchema(schema: same2)
        let schema3 = avro.decodeSchema(schema: diff)
        XCTAssertEqual(schema1, schema2, "same schema test failed")
        XCTAssertNotEqual(schema1, schema3, "different schema test failed")
    }
    private func testEquable(same1: String, same2: String, diff1: String, diff2: String) {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        let avro = Avro()
        let schema1 = avro.decodeSchema(schema: same1)
        let schema2 = avro.decodeSchema(schema: same2)
        let schema3 = avro.decodeSchema(schema: diff1)
        let schema4 = avro.decodeSchema(schema: diff2)
        XCTAssertEqual(schema1, schema2, "same schema test failed")
        XCTAssertNotEqual(schema1, schema3, "different schema test failed")
        XCTAssertNotEqual(schema1, schema4, "different schema test failed")
    }

    func testNull() {
        let sample1 = "{ \"type\" : \"null\"}"
        let sample2 = "{ \"type\" : \"int\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    func testBoolean() {
        let sample1 = "{ \"type\" : \"boolean\"}"
        let sample2 = "{ \"type\" : \"int\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    func testInt() {
        let sample1 = "{ \"type\" : \"int\"}"
        let sample2 = "{ \"type\" : \"long\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    func testLong() {
        let sample1 = "{ \"type\" : \"long\"}"
        let sample2 = "{ \"type\" : \"float\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    
    func testFloat() {
        let sample1 = "{ \"type\" : \"float\"}"
        let sample2 = "{ \"type\" : \"double\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    
    func testDouble() {
        let sample1 = "{ \"type\" : \"double\"}"
        let sample2 = "{ \"type\" : \"string\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    
    func testString() {
        let sample1 = "{ \"type\" : \"string\"}"
        let sample2 = "{ \"type\" : \"bytes\"}"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    
    func testBytes() {
        let sample1 = "{ \"type\" : \"bytes\"}"
        let sample2 = "{ \"type\" : \"array\", \"items\" : \"bytes\" }"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    func testArray() {
        let sample1 = "{ \"type\" : \"array\", \"items\" : \"double\" }"
        let sample2 = "{ \"type\" : \"array\", \"items\" : \"float\" }"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    func testMap() {
        let sample1 = "{ \"type\" : \"map\", \"values\" : \"string\" }"
        let sample2 = "{ \"type\" : \"map\", \"values\" : \"boolean\" }"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    func testEnum() {
        let same1 = "{ \"type\" : \"enum\", \"name\": \"same1\", \"symbols\" : [\"a\", \"b\"]}"
        let same2 = "{ \"type\" : \"enum\", \"name\": \"same1\", \"symbols\" : [\"a\"]}"
        let diff = "{ \"type\" : \"enum\", \"name\": \"same1\", \"symbols\" : [\"a\", \"b\", \"c\"]}"
        let diff2 = "{ \"type\" : \"enum\", \"name\": \"diff\", \"symbols\" : [\"a\", \"b\"]}"
        testEquable(same1: same1, same2: same2, diff1: diff, diff2: diff2)
    }
    func testFixed() {
        let same1 = "{ \"type\" : \"fixed\", \"name\": \"barcode\", \"size\" : 16 }"
        let diff1 = "{ \"type\" : \"fixed\", \"name\": \"barcode\", \"size\" : 15 }"
        let diff2 = "{ \"type\" : \"fixed\", \"name\": \"barcode2\", \"size\" : 16 }"
        testEquable(same1: same1, same2: same1, diff1: diff1, diff2: diff2)
    }
    func testUnion() {
        let sample1 = "[ \"double\", \"int\", \"long\", \"float\" ]"
        let sample2 = "[ \"double\", \"float\", \"int\", \"long\" ]"
        testEquable(same1: sample1, same2: sample1, diff: sample2)
    }
    
    func testRecord() {
        let diff1 = """
{
"type": "record",
"name": "Test",
"fields": [{"name": "f", "type": "long"}]
}
"""
        let diff2 = """
{
"type": "error",
"name": "Test",
"fields": [{"name": "f", "type": "long"}]
}
"""
        let diff3 = """
{
"type": "record",
"name": "Node",
"fields": [{"name": "f", "type": "string"}]
}
"""
        let diff4 = """
{
"type": "record",
"name": "Node",
"fields": [{"name": "label", "type": "string"}]
}
"""
        let avro = Avro()
        let schema1 = avro.decodeSchema(schema: diff1)
        let schema2 = avro.decodeSchema(schema: diff2)
        let schema3 = avro.decodeSchema(schema: diff3)
        let schema4 = avro.decodeSchema(schema: diff4)
        XCTAssertNotEqual(schema1, schema2, "different schema test failed")
        XCTAssertNotEqual(schema1, schema3, "different schema test failed")
        XCTAssertNotEqual(schema1, schema4, "different schema test failed")
    }

    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
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
