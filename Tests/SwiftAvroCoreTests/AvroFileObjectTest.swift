//
//  AvroFileObjectTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro File Object Container")
struct AvroFileObjectTests {

    private let recordSchema = """
    {"type":"record","name":"test","fields":[
      {"name":"a","type":"long"},
      {"name":"b","type":"string"}
    ]}
    """

    struct SimpleModel: Codable { var a: UInt64 = 1; var b: String = "hello" }

    @Test("Header and block round-trip with null codec")
    func objectContainerFile() throws {
        let codec = NullCodec()
        var oc    = try ObjectContainer(schema: recordSchema, codec: codec)
        var newOc = oc

        try oc.addObject(SimpleModel())
        let out = try oc.encodeObject()

        try newOc.decodeHeader(from: out)
        let start = newOc.findMarker(from: out)
        try newOc.decodeBlock(from: out.subdata(in: start..<out.count))

        #expect(oc.headerSize == start)
        #expect(oc.header.magicValue == Array("Obj".utf8) + [1])
        #expect(try oc.header.codec  == AvroReservedConstants.nullCodec)
        #expect(try oc.header.schema == recordSchema)
        #expect(oc.header.marker     == newOc.header.marker)
        #expect(oc.blocks.count      == newOc.blocks.count)
        #expect(oc.blocks[0].data    == newOc.blocks[0].data)
    }

    @Test("Header and block round-trip with reflected schema")
    func objectContainerFileNoSchema() throws {
        let avro   = Avro()
        let schema = try #require(AvroSchema.reflecting(SimpleModel()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let codec = NullCodec()
        var oc    = try ObjectContainer(schema: schemaJson, codec: codec)
        var newOc = try ObjectContainer(codec: codec)

        try oc.addObject(SimpleModel())
        let out   = try oc.encodeObject()
        try newOc.decodeHeader(from: out)
        let start = newOc.findMarker(from: out)
        try newOc.decodeBlock(from: out.subdata(in: start..<out.count))

        #expect(oc.headerSize      == start)
        #expect(oc.header.marker   == newOc.header.marker)
        #expect(oc.blocks.count    == newOc.blocks.count)
        #expect(oc.blocks[0].data  == newOc.blocks[0].data)

        let receivedSchema = try newOc.header.schema
        let _ = avro.decodeSchema(schema: receivedSchema)
        let decoded: SimpleModel = try avro.decode(from: newOc.blocks[0].data)
        #expect(decoded.a == 1)
        #expect(decoded.b == "hello")
    }

    @Test("Kitty round-trip via reflected schema")
    func objectContainerFileKitty() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(Kitty.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let codec = NullCodec()
        var oc    = try ObjectContainer(schema: schemaJson, codec: codec)
        var newOc = try ObjectContainer(codec: codec)

        let kitty = Kitty.random()
        try oc.addObject(kitty)
        let out   = try oc.encodeObject()
        try newOc.decodeHeader(from: out)
        let start = newOc.findMarker(from: out)
        try newOc.decodeBlock(from: out.subdata(in: start..<out.count))

        #expect(oc.headerSize     == start)
        #expect(oc.header.marker  == newOc.header.marker)
        #expect(oc.blocks.count   == newOc.blocks.count)
        #expect(oc.blocks[0].data == newOc.blocks[0].data)

        let receivedSchema = try newOc.header.schema
        let _ = avro.decodeSchema(schema: receivedSchema)
        let decoded: Kitty = try avro.decode(from: newOc.blocks[0].data)
        #expect(decoded == kitty)
    }

    @Test("Multiple kitties round-trip via decodeObjects()")
    func objectContainerFileKitties() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(Kitty.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let codec   = NullCodec()
        var oc      = try ObjectContainer(schema: schemaJson, codec: codec)
        var newOc   = try ObjectContainer(codec: codec)
        let kitties = [Kitty.random(), Kitty.random(), Kitty.random()]

        try oc.addObjects(kitties)
        let out   = try oc.encodeObject()
        try newOc.decodeHeader(from: out)
        let start = newOc.findMarker(from: out)
        try newOc.decodeBlock(from: out.subdata(in: start..<out.count))

        #expect(oc.headerSize     == start)
        #expect(oc.header.marker  == newOc.header.marker)
        #expect(oc.blocks.count   == newOc.blocks.count)
        #expect(oc.blocks[0].data == newOc.blocks[0].data)

        let decoded: [Kitty] = try newOc.decodeObjects()
        #expect(decoded == kitties)
    }

    @Test("KittyAction round-trip via decodeObjects()")
    func objectContainerFileKittyActions() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(KittyAction.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let codec   = NullCodec()
        var oc      = try ObjectContainer(schema: schemaJson, codec: codec)
        var newOc   = try ObjectContainer(codec: codec)
        let actions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]

        try oc.addObjects(actions)
        let out   = try oc.encodeObject()
        try newOc.decodeHeader(from: out)
        let start = newOc.findMarker(from: out)
        try newOc.decodeBlock(from: out.subdata(in: start..<out.count))

        #expect(oc.blocks.count == newOc.blocks.count)

        let decoded: [KittyAction] = try newOc.decodeObjects()
        #expect(decoded.count == actions.count)
    }

    @Test("KittyAction schemaless decoding produces correct field values")
    func objectContainerFileKittyActionsSchemaless() throws {
             let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(KittyAction.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let codec   = NullCodec()
        var oc      = try ObjectContainer(schema: schemaJson, codec: codec)
        var newOc   = try ObjectContainer(codec: codec)
        let actions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]

        try oc.addObjects(actions)
        let out   = try oc.encodeObject()
        try newOc.decodeHeader(from: out)
        let start = newOc.findMarker(from: out)
        try newOc.decodeBlock(from: out.subdata(in: start..<out.count))

        let decoded: [[String: Any]] = try newOc.decodeObjects() as! [[String: Any]]
        #expect(decoded.count == actions.count)

        let first        = actions[0]
        let firstDecoded = decoded[0]
        #expect(first.timestamp.timeIntervalSinceReferenceDate ==
                (firstDecoded["timestamp"] as! Date).timeIntervalSinceReferenceDate,
                "timestamp within 1s")
        #expect(first.dataValue   == firstDecoded["dataValue"]   as! [UInt8])
        #expect(first.label       == firstDecoded["label"]       as! String)
        #expect(first.type.rawValue == firstDecoded["type"]      as! String)
        #expect(first.floatValue  == firstDecoded["floatValue"]  as! Float)
        #expect(first.doubleValue == firstDecoded["doubleValue"] as! Double)

        let decodedKitty = firstDecoded["kitty"] as! [String: Any]
        #expect(first.kitty.name        == decodedKitty["name"]  as! String)
        #expect(first.kitty.color.rawValue == decodedKitty["color"] as! String)
    }

    @Test("decodeBlock throws on corrupt data after valid magic bytes")
    func decodeBlockCorruptDataThrows() throws {
        var oc = try ObjectContainer(codec: NullCodec())
        var corrupt = Data([0x4F, 0x62, 0x6A, 0x01])  // valid magic
        corrupt.append(Data(repeating: 0xFF, count: 32))
        #expect(throws: (any Error).self) {
            try oc.decodeHeader(from: corrupt)
        }
    }
}
