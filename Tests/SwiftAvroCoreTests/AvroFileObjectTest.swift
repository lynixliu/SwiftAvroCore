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
    
    
    

func testObjectContainerFileNoSchema() {
    struct model: Codable {
        var a: UInt64 = 1
        var b: String = "hello"
    }
    
    let avro = Avro()
    let schema = AvroSchema.reflecting(model())!
    let schemaJson = try! String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

    let codec = NullCodec(codecName: AvroReservedConstants.NullCodec)
    var oc = try? ObjectContainer(schema: schemaJson, codec: codec)
    var newOc = try? ObjectContainer(codec: codec)
    
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
        
        let receivedData = newOc!.blocks[0].data
        let receivedSchema = newOc!.header.schema
        let _ = avro.decodeSchema(schema: receivedSchema)
        let decodedModel: model = try avro.decode(from: receivedData)
        
        XCTAssertEqual(decodedModel.a, 1)
        XCTAssertEqual(decodedModel.b, "hello")

    } catch {
        XCTAssert(false, "compress failed")
    }
}
    
    
    
    

func testObjectContainerFileNoSchemaKitty() {

    
    let avro = Avro()
    let schema = AvroSchema.reflecting(Kitty.random())!
    let schemaJson = try! String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

    let codec = NullCodec(codecName: AvroReservedConstants.NullCodec)
    var oc = try? ObjectContainer(schema: schemaJson, codec: codec)
    var newOc = try? ObjectContainer(codec: codec)
    
    do {
        let randomKitty = Kitty.random()
        try oc?.addObject(randomKitty)
        let out = try! oc?.encodeObject()
        try newOc?.decodeHeader(from: out!)
        let start = newOc?.findMarker(from: out!)
        try newOc?.decodeBlock(from: out!.subdata(in: start!..<out!.count))
        XCTAssertEqual(oc?.headerSize, start, "header size don't match.")
        XCTAssertEqual(oc?.header.marker, newOc?.header.marker, "header don't match.")
        XCTAssertEqual(oc?.blocks.count, newOc?.blocks.count, "blocks length don't match.")
        XCTAssertEqual(oc?.blocks[0].data, newOc?.blocks[0].data, "block data don't match.")
        
        let receivedData = newOc!.blocks[0].data
        let receivedSchema = newOc!.header.schema
        let _ = avro.decodeSchema(schema: receivedSchema)
        let decodedKitty: Kitty = try avro.decode(from: receivedData)
        XCTAssertEqual(randomKitty, decodedKitty)

    } catch {
        XCTAssert(false, "compress failed")
    }
}
    
    

func testObjectContainerFileNoSchemaKitties() {
    let avro = Avro()
    let schema = AvroSchema.reflecting(Kitty.random())!
    let schemaJson = try! String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

    let codec = NullCodec(codecName: AvroReservedConstants.NullCodec)
    var oc = try? ObjectContainer(schema: schemaJson, codec: codec)
    var newOc = try? ObjectContainer(codec: codec)
    
    do {
        let randomKitties = [Kitty.random(), Kitty.random(), Kitty.random()]
        try oc?.addObjects(randomKitties)
        let out = try! oc?.encodeObject()
        try newOc?.decodeHeader(from: out!)
        let start = newOc?.findMarker(from: out!)
        try newOc?.decodeBlock(from: out!.subdata(in: start!..<out!.count))
        XCTAssertEqual(oc?.headerSize, start, "header size don't match.")
        XCTAssertEqual(oc?.header.marker, newOc?.header.marker, "header don't match.")
        XCTAssertEqual(oc?.blocks.count, newOc?.blocks.count, "blocks length don't match.")
        XCTAssertEqual(oc?.blocks[0].data, newOc?.blocks[0].data, "block data don't match.")
        
        let decodedKitties: [Kitty] = (try newOc?.decodeObjects())! as [Kitty]
        
        XCTAssertEqual(decodedKitties, randomKitties)

    } catch {
        XCTAssert(false, "compress failed")
    }
}
    
    

func testObjectContainerFileNoSchemaKittyActions() {
    let avro = Avro()
    let schema = AvroSchema.reflecting(KittyAction.random())!
    let schemaJson = try! String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

    let codec = NullCodec(codecName: AvroReservedConstants.NullCodec)
    var oc = try? ObjectContainer(schema: schemaJson, codec: codec)
    var newOc = try? ObjectContainer(codec: codec)
    
    do {
        let randomKittyActions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]
        try oc?.addObjects(randomKittyActions)
        let out = try! oc?.encodeObject()
        try newOc?.decodeHeader(from: out!)
        let start = newOc?.findMarker(from: out!)
        try newOc?.decodeBlock(from: out!.subdata(in: start!..<out!.count))
        XCTAssertEqual(oc?.headerSize, start, "header size don't match.")
        XCTAssertEqual(oc?.header.marker, newOc?.header.marker, "header don't match.")
        XCTAssertEqual(oc?.blocks.count, newOc?.blocks.count, "blocks length don't match.")
        XCTAssertEqual(oc?.blocks[0].data, newOc?.blocks[0].data, "block data don't match.")
        
        let decodedKittyActions: [KittyAction] = (try newOc?.decodeObjects())! as [KittyAction]
        
        XCTAssertEqual(decodedKittyActions.count, randomKittyActions.count)

    } catch {
        XCTAssert(false, "compress failed")
    }
}
    
    

func testObjectContainerFileNoSchemaKittyActionsSchemalessDecoding() {
    let avro = Avro()
    let schema = AvroSchema.reflecting(KittyAction.random())!
    let schemaJson = try! String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

    let codec = NullCodec(codecName: AvroReservedConstants.NullCodec)
    var oc = try? ObjectContainer(schema: schemaJson, codec: codec)
    var newOc = try? ObjectContainer(codec: codec)
    
    do {
        let randomKittyActions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]
        try oc?.addObjects(randomKittyActions)
        let out = try! oc?.encodeObject()
        try newOc?.decodeHeader(from: out!)
        let start = newOc?.findMarker(from: out!)
        try newOc?.decodeBlock(from: out!.subdata(in: start!..<out!.count))
        
        let decodedKittyActions: [[String: Any]] = (try newOc?.decodeObjects())! as! [[String: Any]]
        
        XCTAssertEqual(decodedKittyActions.count, randomKittyActions.count)
        
        let kittenAction = randomKittyActions.first!
        let kittenActionDecoded = decodedKittyActions.first!
        
        
        XCTAssertEqual(kittenAction.timestamp.timeIntervalSinceReferenceDate, (kittenActionDecoded["timestamp"] as! Date).timeIntervalSinceReferenceDate, accuracy: 1)
        XCTAssertEqual(kittenAction.dataValue, kittenActionDecoded["dataValue"] as! [UInt8])
        XCTAssertEqual(kittenAction.label, kittenActionDecoded["label"] as! String)
        XCTAssertEqual(kittenAction.type.rawValue, kittenActionDecoded["type"] as! String)
        XCTAssertEqual(kittenAction.floatValue, kittenActionDecoded["floatValue"] as! Float)
        XCTAssertEqual(kittenAction.doubleValue, kittenActionDecoded["doubleValue"] as! Double)
        
        let decodedKitty = kittenActionDecoded["kitty"] as! [String: Any]
        XCTAssertEqual(kittenAction.kitty.name, decodedKitty["name"] as! String)
        XCTAssertEqual(kittenAction.kitty.color.rawValue, decodedKitty["color"] as! String)

        

    } catch {
        XCTAssert(false, "compress failed")
    }
}
}
