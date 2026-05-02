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

    struct SimpleModel: Codable { var a: Int64 = 1; var b: String = "hello" }

    private func makeAvro(schema: String) -> Avro {
        let avro = Avro()
        avro.decodeSchema(schema: schema)
        return avro
    }

    // MARK: -

    @Test("Header and block round-trip with null codec")
    func objectContainerFile() throws {
        let codec = NullCodec()
        let avro  = makeAvro(schema: recordSchema)

        var oc = ObjectContainer(schema: recordSchema)
        try oc.addObject(SimpleModel(), avro: avro)
        let out = try oc.encode(avro: avro, codec: codec)

        var newOc = ObjectContainer()
        try newOc.decode(from: out, avro: avro, codec: codec)

        #expect(oc.header.magicValue  == Array("Obj".utf8) + [1])
        #expect(try oc.header.codec   == AvroReservedConstants.nullCodec)
        #expect(try oc.header.schema  == recordSchema)
        #expect(oc.header.marker      == newOc.header.marker)
        #expect(oc.blocks.count       == newOc.blocks.count)
        #expect(oc.blocks[0].data     == newOc.blocks[0].data)
    }

    @Test("Header and block round-trip with reflected schema")
    func objectContainerFileNoSchema() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(SimpleModel()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        avro.decodeSchema(schema: schemaJson)

        let codec = NullCodec()
        var oc    = ObjectContainer(schema: schemaJson)
        try oc.addObject(SimpleModel(), avro: avro)
        let out = try oc.encode(avro: avro, codec: codec)

        var newOc = ObjectContainer()
        try newOc.decode(from: out, avro: avro, codec: codec)

        #expect(oc.header.marker  == newOc.header.marker)
        #expect(oc.blocks.count   == newOc.blocks.count)
        #expect(oc.blocks[0].data == newOc.blocks[0].data)

        let receivedSchema = try newOc.header.schema
        avro.decodeSchema(schema: receivedSchema)
        let decoded: SimpleModel = try avro.decode(from: newOc.blocks[0].data)
        #expect(decoded.a == 1)
        #expect(decoded.b == "hello")
    }

    @Test("Kitty round-trip via reflected schema")
    func objectContainerFileKitty() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(Kitty.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        avro.decodeSchema(schema: schemaJson)

        let codec = NullCodec()
        let kitty = Kitty.random()
        var oc    = ObjectContainer(schema: schemaJson)
        try oc.addObject(kitty, avro: avro)
        let out = try oc.encode(avro: avro, codec: codec)

        var newOc = ObjectContainer()
        try newOc.decode(from: out, avro: avro, codec: codec)

        #expect(oc.header.marker  == newOc.header.marker)
        #expect(oc.blocks.count   == newOc.blocks.count)
        #expect(oc.blocks[0].data == newOc.blocks[0].data)

        let receivedSchema = try newOc.header.schema
        avro.decodeSchema(schema: receivedSchema)
        let decoded: Kitty = try avro.decode(from: newOc.blocks[0].data)
        #expect(decoded == kitty)
    }

    @Test("Multiple kitties round-trip via decodeAll()")
    func objectContainerFileKitties() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(Kitty.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        avro.decodeSchema(schema: schemaJson)

        let codec   = NullCodec()
        let kitties = [Kitty.random(), Kitty.random(), Kitty.random()]
        var oc      = ObjectContainer(schema: schemaJson)
        try oc.addObjects(kitties, avro: avro)
        let out = try oc.encode(avro: avro, codec: codec)

        var newOc = ObjectContainer()
        try newOc.decode(from: out, avro: avro, codec: codec)

        #expect(oc.header.marker  == newOc.header.marker)
        #expect(oc.blocks.count   == newOc.blocks.count)
        #expect(oc.blocks[0].data == newOc.blocks[0].data)

        let decoded: [Kitty] = try newOc.decodeAll(avro: avro)
        #expect(decoded == kitties)
    }

    @Test("KittyAction round-trip via decodeAll()")
    func objectContainerFileKittyActions() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(KittyAction.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        avro.decodeSchema(schema: schemaJson)

        let codec   = NullCodec()
        let actions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]
        var oc      = ObjectContainer(schema: schemaJson)
        try oc.addObjects(actions, avro: avro)
        let out = try oc.encode(avro: avro, codec: codec)

        var newOc = ObjectContainer()
        try newOc.decode(from: out, avro: avro, codec: codec)

        #expect(oc.blocks.count == newOc.blocks.count)

        let decoded: [KittyAction] = try newOc.decodeAll(avro: avro)
        #expect(decoded.count == actions.count)
    }

    @Test("KittyAction schemaless decoding produces correct field values")
    func objectContainerFileKittyActionsSchemaless() throws {
        let avro       = Avro()
        let schema     = try #require(AvroSchema.reflecting(KittyAction.random()))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        avro.decodeSchema(schema: schemaJson)

        let codec   = NullCodec()
        let actions = [KittyAction.random(), KittyAction.random(), KittyAction.random()]
        var oc      = ObjectContainer(schema: schemaJson)
        try oc.addObjects(actions, avro: avro)
        let out = try oc.encode(avro: avro, codec: codec)

        var newOc = ObjectContainer()
        try newOc.decode(from: out, avro: avro, codec: codec)

        let decoded: [[String: Any]] = try newOc.decodeAll(avro: avro) as? [[String: Any]] ?? []
        #expect(decoded.count == actions.count)

        let first        = actions[0]
        let firstDecoded = decoded[0]
        let encodedTime  = first.timestamp.timeIntervalSinceReferenceDate
        let decodedTime  = (firstDecoded["timestamp"] as! Date).timeIntervalSinceReferenceDate
        #expect(abs(encodedTime - decodedTime) < 1.0, "timestamp within 1s")
        #expect(first.dataValue     == firstDecoded["dataValue"]   as! [UInt8])
        #expect(first.label         == firstDecoded["label"]       as! String)
        #expect(first.type.rawValue == firstDecoded["type"]        as! String)
        #expect(first.floatValue    == firstDecoded["floatValue"]  as! Float)
        #expect(first.doubleValue   == firstDecoded["doubleValue"] as! Double)

        let decodedKitty = firstDecoded["kitty"] as! [String: Any]
        #expect(first.kitty.name           == decodedKitty["name"]  as! String)
        #expect(first.kitty.color.rawValue == decodedKitty["color"] as! String)
    }

    @Test("decode throws on corrupt data after valid magic bytes")
    func decodeCorruptDataThrows() throws {
        let avro = Avro()
        var oc   = ObjectContainer()
        var corrupt = Data([0x4F, 0x62, 0x6A, 0x01])  // valid magic
        corrupt.append(Data(repeating: 0xFF, count: 32))
        #expect(throws: (any Error).self) {
            try oc.decode(from: corrupt, avro: avro, codec: NullCodec())
        }
    }

    // MARK: - Header missing-metadata error paths

    @Test("Header.codec throws when codec metadata is absent")
    func headerMissingCodecThrows() {
        let header = Header()
        #expect(throws: (any Error).self) {
            _ = try header.codec
        }
    }

    @Test("Header.schema throws when schema metadata is absent")
    func headerMissingSchemaThrows() {
        let header = Header()
        #expect(throws: (any Error).self) {
            _ = try header.schema
        }
    }

    // MARK: - makeFileObjectContainer

    @Test("Avro.makeFileObjectContainer returns an ObjectContainer")
    func makeFileObjectContainerWorks() {
        let avro = Avro()
        let oc = avro.makeFileObjectContainer(schema: recordSchema)
        #expect(oc.header.magicValue == Array("Obj".utf8) + [1])
    }
}
