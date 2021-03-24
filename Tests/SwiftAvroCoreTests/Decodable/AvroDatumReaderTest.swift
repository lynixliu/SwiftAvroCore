//
// Created by Kacper Kawecki on 24/03/2021.
//

import XCTest
@testable import SwiftAvroCore

class AvroDatumReaderTest: XCTestCase {
    func testInt() throws {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"int\" }"

        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let decoder = AvroBinaryReader(bytes: avroBytes)
        let reader = AvroDatumReader(writerSchema: schema)
        let value = try reader.read(decoder: decoder)
        if case .primitive(.int(let primitiveValue)) = value {
            XCTAssertEqual(3209099, primitiveValue)
        } else {
            XCTFail("Wrongly resolved schema")
        }
    }

    func testNestedModel() throws {
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
        let decoder = AvroBinaryReader(data: data)
        let reader = AvroDatumReader(writerSchema: schema)
        let value = try reader.read(decoder: decoder)
        if case .keyed(let keyMap) = value {
            if case .primitive(.string(let name)) = keyMap["name"] {
                XCTAssertEqual(name, "test")
            } else {
                XCTFail()
            }
            if case .keyed(let recordMap) = keyMap["message"] {
                if case .primitive(.int(let primitiveValue)) = recordMap["requestId"] {
                    XCTAssertEqual(primitiveValue, 42)
                } else {
                    XCTFail()
                }
            }  else {
                XCTFail()
            }
        } else {
            XCTFail("Wrongly resolved schema")
        }
    }
}
