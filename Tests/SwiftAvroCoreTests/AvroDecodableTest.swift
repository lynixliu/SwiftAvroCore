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
        
        if let anyValue = try? decoder.decode(from: truedata) {
            XCTAssert(anyValue as! Bool, "Value should be true.")
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

        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! Int32, 3209099, "Byte arrays don't match.")
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
        
        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! Int64, 3209099, "Byte arrays don't match.")
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
        
        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! Float, expected, "Byte arrays don't match.")
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
        
        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! Double, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testDate() {
        let avroBytes: [UInt8] = [0xA0, 0x38]
        let jsonSchema = "{ \"type\" : \"int\", \"logicalType\": \"date\" }"
        let source: Date = Date(timeIntervalSince1970: 3600)
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        let data = Data(avroBytes)
        if let value = try? encoder.encode(source, schema: schema) {
            print("value:",value[0],value[1])
            XCTAssertEqual(value, data, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        let decoder = AvroDecoder(schema: schema)
        if let value = try? decoder.decode(Date.self,from: data) {
            XCTAssertEqual(value, source, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
        if let value = try? decoder.decode(from: data) as! Date {
            XCTAssertEqual(value, source, "Byte arrays don't match.")
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
        let anyValue = try? decoder.decode(from: data)
        switch schema {
        case .enumSchema(let attr):
            XCTAssertEqual(attr.symbols[9], anyValue as! String)
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
        
        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! String, "foo", "Strings don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testBytes() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"
        
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode([UInt8].self, from: data) {
            XCTAssertEqual(value, [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! [UInt8], [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
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
        
        if let anyValue = try? decoder.decode(from: data) {
            XCTAssertEqual(anyValue as! [UInt8], [0x01, 0x02, 0x03, 0x04], "Byte arrays don't match.")
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
        
        if let anyValue = try? decoder.decode(from: data) as! [UInt32] {
            XCTAssertEqual(anyValue, expected, "Duration arrays don't match.")
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
        
        if let anyValue = try? decoder.decode(from: data) as! [String: [UInt32]] {
            XCTAssertEqual(anyValue, ["requestType": expected], "Byte arrays don't match.")
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
        
        if let values = try? decoder.decode(from: data) {
            let arr = values as! [Int64]
            XCTAssertEqual(arr.count, 2, "Wrong number of elements in array.")
            for idx in 0...1 {
                XCTAssertEqual(arr[idx], expected[idx], "Unexpected value.")
                
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
        
        if let values = try? decoder.decode(from: data) as! [String: [Int64]] {
            XCTAssertEqual(values.count, 2, "Wrong number of elements in map.")
            XCTAssertEqual(values["foo"]![0], expected[0], "Unexpected value.")
            XCTAssertEqual(values["boo"]![1], expected[1], "Unexpected value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testInnerEmptyMap() {
        struct Model:Codable {
            var magic: [UInt8]
            var meta: [String : [UInt8]]
            var sync: [UInt8]
            
            init(){
                let version: UInt8 = 1
                self.magic =  [version]
                self.meta = Dictionary<String, [UInt8]>()
                self.sync = withUnsafeBytes(of: UUID().uuid) {buf in [UInt8](buf)}
            }
        }
        let model = Model()
        let jsonSchema = """
{"type": "record", "name": "org.apache.avro.file.Header",
"fields" : [
{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 1}},
{"name": "meta", "type": {"type": "map", "values": "bytes"}},
{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
]
}
"""
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let emptyMeta = try? avro.encode(model)
        let decoder = AvroDecoder(schema: schema)
        if let values = try? decoder.decode(Model.self, from: emptyMeta!) {
            XCTAssertEqual(values.magic, model.magic, "Wrong number of elements in map.")
            XCTAssertEqual(values.meta.count,0, "Wrong number of elements in map.")
            XCTAssertEqual(values.sync, model.sync, "Wrong number of elements in map.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        if let values = try? decoder.decode(from: emptyMeta!) as! [String:Any] {
            XCTAssertEqual(values["magic"] as! [UInt8], model.magic, "Wrong number of elements in map.")
            XCTAssertEqual(values["meta"] as! [String : [UInt8]] ,model.meta, "Wrong number of elements in map.")
            XCTAssertEqual(values["sync"] as! [UInt8], model.sync, "Wrong number of elements in map.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testInnerMap() {
        struct Model:Codable {
            var magic: [UInt8]
            var meta: [String : [UInt8]]
            var sync: [UInt8]
            
            init(){
                let version: UInt8 = 1
                self.magic = [version]
                self.meta = ["avro.codec":Array("null".utf8),
                             "avro.schema":Array("null".utf8)]
                self.sync = withUnsafeBytes(of: UUID().uuid) {buf in [UInt8](buf)}
            }
        }
        let model = Model()
        let jsonSchema = """
{"type": "record", "name": "org.apache.avro.file.Header",
"fields" : [
{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 1}},
{"name": "meta", "type": {"type": "map", "values": "bytes"}},
{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
]
}
"""
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let encoded = try? avro.encode(model)
        encoded?.forEach({ ch in
            print(ch)
        })
        if let values = try? decoder.decode(Model.self, from: encoded!) {
            XCTAssertEqual(values.magic, model.magic, "Wrong number of elements in map.")
            XCTAssertEqual(values.meta["avro.codec"], model.meta["avro.codec"], "Unexpected value.")
            XCTAssertEqual(values.meta["avro.schema"], model.meta["avro.schema"], "Unexpected value.")
            XCTAssertEqual(values.sync, model.sync, "Wrong number of elements in map.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
        if let values = try? decoder.decode(from: encoded!) as! [String: Any] {
            XCTAssertEqual(values["magic"] as! [UInt8], model.magic, "Wrong number of elements in map.")
            XCTAssertEqual(values["meta"] as! [String : [UInt8]], model.meta, "Unexpected value.")
            XCTAssertEqual(values["sync"] as! [UInt8], model.sync, "Wrong number of elements in map.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }
    
    func testUnion() {
        let avroBytes: [UInt8] = [0x02, 0x02, 0x61]
        let jsonSchema = "[\"null\",\"string\"]"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        let data = Data(avroBytes)
        if let value = try? decoder.decode(String?.self, from: data) {
            XCTAssertEqual(value, "a", "Unexpected string value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
        if let value = try? decoder.decode(from: data) as! String {
            XCTAssertEqual(value, "a", "Unexpected string value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
        
    }
    
    func testCompondeUnion() {
        let jsonSchema = """
{
"fields": [
{"name": "bic", "type": ["null", "string"]},
{"name": "countryOfBirth", "type": "string"},
{"name": "customerId", "type": "string"},
{"name": "dateOfBirth", "type": "string"},
{"name": "dateOfOpened", "type": "string"},
{"name": "firstName", "type": "string"},
{"name": "lastName", "type": "string"},
{"name": "lineOfBusiness", "type": "string"},
{"name": "placeOfBirth", "type": "string"},
{"name": "title", "type": ["null", "string"]}],
"name": "NeuronDemoCustomer",
"type": "record"}
"""
        struct Model:Codable,Equatable {
            var bic:String?
            var countryOfBirth:String
            var customerId:String
            var dateOfBirth:String
            var dateOfOpened:String
            var firstName:String
            var lastName:String
            var lineOfBusiness:String
            var placeOfBirth:String
            var title:String
        }
        let expectResult = Model(bic: "RVOTATACXXX", countryOfBirth: "LU", customerId: "687", dateOfBirth: "1969-11-16", dateOfOpened: "2021-04-11", firstName: "Lara-Sophie", lastName: "Schwab", lineOfBusiness: "CORP", placeOfBirth: "Ried im Innkreis", title: "Mag.")
        let data = Data([0x02,
                             0x16,0x52,0x56,0x4f,0x54,0x41,0x54,0x41,0x43,0x58,0x58,0x58,0x04,0x4c,0x55,0x06,0x36,0x38,0x37,0x14,0x31,0x39,0x36,0x39,0x2d,0x31,0x31,0x2d,0x31,0x36,0x14,0x32,0x30,0x32,0x31,0x2d,0x30,0x34,0x2d,0x31,0x31,0x16,0x4c,0x61,0x72,0x61,0x2d,0x53,0x6f,0x70,0x68,0x69,0x65,0x0c,0x53,0x63,0x68,0x77,0x61,0x62,0x08,0x43,0x4f,0x52,0x50,0x20,0x52,0x69,0x65,0x64,0x20,0x69,0x6d,0x20,0x49,0x6e,0x6e,0x6b,0x72,0x65,0x69,0x73,0x02,0x08,0x4d,0x61,0x67,0x2e])
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
/*
 TODO: fix decode error
        if let value = try? decoder.decode(Model.self, from: data) {
            XCTAssertEqual(value, expectResult, "Unexpected model value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
*/
        if let value = try? decoder.decode(from: Data(data)) as! [String:Any] {
            XCTAssertEqual(expectResult.bic,value["bic"] as! String?, "Unexpected string value.")
            XCTAssertEqual(expectResult.countryOfBirth,value["countryOfBirth"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.customerId,value["customerId"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.dateOfBirth,value["dateOfBirth"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.dateOfOpened,value["dateOfOpened"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.firstName,value["firstName"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.lastName,value["lastName"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.lineOfBusiness,value["lineOfBusiness"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.placeOfBirth,value["placeOfBirth"] as! String, "Unexpected string value.")
            XCTAssertEqual(expectResult.title,value["title"] as! String, "Unexpected string value.")
        }else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testRecord() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3,
                                  0x04, 0x06, 0x36, 0x00,
                                  0x04,// block count
                                  0x06, 0x66, 0x6f, 0x6f,// string
                                  0x06,//long
                                  0x06, 0x61, 0x6f, 0x6f,// string
                                  0x04,//long
                                  0x00,//map kv end
                                  0x02,// block count
                                  0x06, 0x62, 0x6f, 0x6f,// string
                                  0x04, 0x08, 0x38, 0x00,// array
                                  0x00]// map kvs end
        let jsonSchema = """
{"type":"record",
"name": "tem",
"fields":[
{"name": "data", "type": "long"},
{"name": "values", "type": {"type": "array", "items": "long"}},
{"name": "kv", "type": {"type": "map", "values": "long"}},
{"name": "kvs", "type": {"type": "map", "values": {"type" : "array", "items": "long"}}}
]}
"""
        let data = Data(avroBytes)
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroDecoder(schema: schema)
        struct myFields: Decodable {
            let data: Int64
            let values: [Int64]
            let kv: [String: Int64]
            let kvs: [String: [Int64]]
        }
        struct record: Decodable {
            let  fields: myFields
        }
        
        if let value = try? decoder.decode(record.self, from: data) {
            XCTAssertEqual(value.fields.data, 3209099, "Byte arrays don't match.")
            XCTAssertEqual(Int(value.fields.values[1]), 27, "Byte arrays don't match.")
            XCTAssertEqual(Int(value.fields.kv["foo"]!), 3, "Byte arrays don't match.")
            XCTAssertEqual(Int(value.fields.kvs["boo"]![0]), 4, "Byte arrays don't match.")
        }
        
        if let value = try? decoder.decode(from: data) as! [String:Any] {
            XCTAssertEqual(value["data"] as! Int64, 3209099, "Byte arrays don't match.")
            XCTAssertEqual(value["values"] as! [Int64] , [3,27], "Byte arrays don't match.")
            XCTAssertEqual(value["kv"] as! [String:Int64], ["aoo": 2, "foo":3], "Byte arrays don't match.")
            for (k,v) in value["kvs"] as! [String:[Any]] {
                XCTAssertEqual(k, "boo", "Byte arrays don't match.")
                XCTAssertEqual(v as! [Int64], [4,28], "Byte arrays don't match.")
            }
            
        }
    }
    
    func testObjectContainerFile() {
        let codec = NullCodec(codecName: AvroReservedConstants.NullCodec)
        var oc = try? ObjectContainer(schema: """
{
"type": "record",
"name": "test",
"fields" : [
{"name": "a", "type": "long"},
{"name": "b", "type": "string"}
]
}
""", codec: codec)
        var newOc = oc
        struct model: Codable {
            var a: UInt64 = 1
            var b: String = "hello"
        }
        do {
            try oc?.addObject(model())
            let out = try! oc?.encodeObject()
            try newOc?.decodeHeader(from: out!)
            let start = newOc?.findMarker(from: out!)
            try newOc?.decodeBlock(from: out!.subdata(in: start!..<out!.count))
            XCTAssertEqual(oc?.headerSize, start, "header size don't match.")
            XCTAssertEqual(oc?.header.marker, newOc?.header.marker, "header don't match.")
            XCTAssertEqual(oc?.blocks.count, newOc?.blocks.count, "blocks length don't match.")
            XCTAssertEqual(oc?.blocks[0].data, newOc?.blocks[0].data, "block data don't match.")
        } catch {
            XCTAssert(false, "compress failed")
        }
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
