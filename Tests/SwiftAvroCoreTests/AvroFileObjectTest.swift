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

    // MARK: - Helpers

    private func makeContext(
        schema: String,
        codec: any CodecProtocol = NullCodec()
    ) throws -> ObjectContainerContext {
        try ObjectContainerContext(schema: schema, codec: codec)
    }

    // MARK: - Tests

    @Test("Header and block round-trip with null codec")
    func objectContainerFile() throws {
        let writeContext = try makeContext(schema: recordSchema)
        let readContext  = try makeContext(schema: recordSchema)

        var writer = try ObjectContainerWriter(schema: recordSchema, codec: NullCodec())
        var reader = ObjectContainerReader()

        try writer.addObject(SimpleModel())
        let out = try writer.encodeObject(context: writeContext)

        try reader.decodeHeader(from: out, context: readContext)
        let start = reader.findMarker(from: out)
        try reader.decodeBlock(from: out.subdata(in: start ..< out.count), context: readContext)

        #expect(writer.headerSize(context: writeContext) == start)
        #expect(writer.header.magicValue == Array("Obj".utf8) + [1])
        #expect(try writer.header.codec  == AvroReservedConstants.nullCodec)
        #expect(try writer.header.schema == recordSchema)
        #expect(writer.header.marker     == reader.header.marker)
        #expect(!reader.blocks.isEmpty)
        #expect(!reader.blocks[0].data.isEmpty)
    }

    @Test("Header and block round-trip with reflected schema")
    func objectContainerFileNoSchema() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(SimpleModel()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let writeContext = try makeContext(schema: schemaJson)
        let readContext  = try makeContext(schema: AvroReservedConstants.dummyRecordScheme)

        var writer = try ObjectContainerWriter(schema: schemaJson, codec: NullCodec())
        var reader = ObjectContainerReader()

        try writer.addObject(SimpleModel())
        let out = try writer.encodeObject(context: writeContext)

        try reader.decodeHeader(from: out, context: readContext)
        let start = reader.findMarker(from: out)
        try reader.decodeBlock(from: out.subdata(in: start ..< out.count), context: readContext)

        #expect(writer.headerSize(context: writeContext) == start)
        #expect(writer.header.marker  == reader.header.marker)
        #expect(!reader.blocks.isEmpty)
        #expect(!reader.blocks[0].data.isEmpty)

        let receivedSchema = try reader.header.schema
        let _ = avro.decodeSchema(schema: receivedSchema)
        let decoded: SimpleModel = try avro.decode(from: reader.blocks[0].data)
        #expect(decoded.a == 1)
        #expect(decoded.b == "hello")
    }

    @Test("Kitty round-trip via reflected schema")
    func objectContainerFileKitty() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(Kitty.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let writeContext = try makeContext(schema: schemaJson)
        let readContext  = try makeContext(schema: AvroReservedConstants.dummyRecordScheme)

        var writer = try ObjectContainerWriter(schema: schemaJson, codec: NullCodec())
        var reader = ObjectContainerReader()

        let kitty = Kitty.random()
        try writer.addObject(kitty)
        let out = try writer.encodeObject(context: writeContext)

        try reader.decodeHeader(from: out, context: readContext)
        let start = reader.findMarker(from: out)
        try reader.decodeBlock(from: out.subdata(in: start ..< out.count), context: readContext)

        #expect(writer.headerSize(context: writeContext) == start)
        #expect(writer.header.marker  == reader.header.marker)
        #expect(!reader.blocks.isEmpty)
        #expect(!reader.blocks[0].data.isEmpty)

        let receivedSchema = try reader.header.schema
        let _ = avro.decodeSchema(schema: receivedSchema)
        let decoded: Kitty = try avro.decode(from: reader.blocks[0].data)
        #expect(decoded == kitty)
    }

    @Test("Multiple kitties round-trip via decodeObjects()")
    func objectContainerFileKitties() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(Kitty.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let writeContext = try makeContext(schema: schemaJson)
        let readContext  = try makeContext(schema: AvroReservedConstants.dummyRecordScheme)

        var writer  = try ObjectContainerWriter(schema: schemaJson, codec: NullCodec())
        var reader  = ObjectContainerReader()
        let kitties = [Kitty.random(), Kitty.random(), Kitty.random()]

        try writer.addObjects(kitties)
        let out = try writer.encodeObject(context: writeContext)

        try reader.decodeHeader(from: out, context: readContext)
        let start = reader.findMarker(from: out)
        try reader.decodeBlock(from: out.subdata(in: start ..< out.count), context: readContext)

        #expect(writer.headerSize(context: writeContext) == start)
        #expect(writer.header.marker  == reader.header.marker)
        #expect(!reader.blocks.isEmpty)

        let decoded: [Kitty] = try reader.decodeObjects(context: readContext)
        #expect(decoded == kitties)
    }

    @Test("KittyAction round-trip via decodeObjects()")
    func objectContainerFileKittyActions() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(KittyAction.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let writeContext = try makeContext(schema: schemaJson)
        let readContext  = try makeContext(schema: AvroReservedConstants.dummyRecordScheme)

        var writer  = try ObjectContainerWriter(schema: schemaJson, codec: NullCodec())
        var reader  = ObjectContainerReader()
        let actions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]

        try writer.addObjects(actions)
        let out = try writer.encodeObject(context: writeContext)

        try reader.decodeHeader(from: out, context: readContext)
        let start = reader.findMarker(from: out)
        try reader.decodeBlock(from: out.subdata(in: start ..< out.count), context: readContext)

        #expect(!reader.blocks.isEmpty)

        let decoded: [KittyAction] = try reader.decodeObjects(context: readContext)
        #expect(decoded.count == actions.count)
    }

    @Test("KittyAction schemaless decoding produces correct field values")
    func objectContainerFileKittyActionsSchemaless() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(KittyAction.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)

        let writeContext = try makeContext(schema: schemaJson)
        let readContext  = try makeContext(schema: AvroReservedConstants.dummyRecordScheme)

        var writer  = try ObjectContainerWriter(schema: schemaJson, codec: NullCodec())
        var reader  = ObjectContainerReader()
        let actions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]

        try writer.addObjects(actions)
        let out = try writer.encodeObject(context: writeContext)

        try reader.decodeHeader(from: out, context: readContext)
        let start = reader.findMarker(from: out)
        try reader.decodeBlock(from: out.subdata(in: start ..< out.count), context: readContext)

        let decoded: [[String: Any]] = try reader.decodeObjects(context: readContext) as! [[String: Any]]
        #expect(decoded.count == actions.count)

        let first        = actions[0]
        let firstDecoded = decoded[0]
        #expect(first.timestamp.timeIntervalSinceReferenceDate ==
                (firstDecoded["timestamp"] as! Date).timeIntervalSinceReferenceDate,
                "timestamp within 1s")
        #expect(first.dataValue     == firstDecoded["dataValue"]   as! [UInt8])
        #expect(first.label         == firstDecoded["label"]       as! String)
        #expect(first.type.rawValue == firstDecoded["type"]        as! String)
        #expect(first.floatValue    == firstDecoded["floatValue"]  as! Float)
        #expect(first.doubleValue   == firstDecoded["doubleValue"] as! Double)

        let decodedKitty = firstDecoded["kitty"] as! [String: Any]
        #expect(first.kitty.name           == decodedKitty["name"]  as! String)
        #expect(first.kitty.color.rawValue == decodedKitty["color"] as! String)
    }

    @Test("decodeHeader throws on corrupt data after valid magic bytes")
    func decodeBlockCorruptDataThrows() throws {
        let context = try makeContext(schema: AvroReservedConstants.dummyRecordScheme)
        var reader  = ObjectContainerReader()
        var corrupt = Data([0x4F, 0x62, 0x6A, 0x01])
        corrupt.append(Data(repeating: 0xFF, count: 32))
        #expect(throws: (any Error).self) {
            try reader.decodeHeader(from: corrupt, context: context)
        }
    }
}
