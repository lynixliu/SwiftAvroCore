//
//  AvroFileObjectTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 15/03/22.
//

import XCTest
@testable import SwiftAvroCore
class AvroObjectTest: XCTestCase {

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
        XCTAssertEqual(oc?.header.magicValue, "Obj".utf8.map{UInt8($0)} + [1], "header magic mismatch.")
        XCTAssertEqual(oc?.header.codec, AvroReservedConstants.NullCodec, "header magic mismatch.")
        XCTAssertEqual(oc?.header.schema, """
{
"type": "record",
"name": "test",
"fields" : [
{"name": "a", "type": "long"},
{"name": "b", "type": "string"}
]
}
""", "header schema mismatch.")
        XCTAssertEqual(oc?.header.marker, newOc?.header.marker, "header marker mismatch.")
        XCTAssertEqual(oc?.blocks.count, newOc?.blocks.count, "blocks mismatch.")
        XCTAssertEqual(oc?.blocks[0].data, newOc?.blocks[0].data, "block data mismatch.")
    } catch {
        XCTAssert(false, "compress failed")
    }
}
    
}
