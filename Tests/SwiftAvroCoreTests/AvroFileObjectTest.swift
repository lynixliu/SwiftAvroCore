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

    // MARK: -

    @Test("Header and block round-trip with null codec")
    func objectContainerFile() throws {
        let codec = NullCodec()
        let (avro, marker) = createAvroWithMarker(recordSchema)

        var oc = ObjectContainer(schema: recordSchema, syncMarker: marker)
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
        let marker = makeSyncMarker()
        var oc    = ObjectContainer(schema: schemaJson, syncMarker: marker)
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
        let marker = makeSyncMarker()
        var oc    = ObjectContainer(schema: schemaJson, syncMarker: marker)
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
        let marker = makeSyncMarker()
        var oc      = ObjectContainer(schema: schemaJson, syncMarker: marker)
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
        let marker = makeSyncMarker()
        var oc      = ObjectContainer(schema: schemaJson, syncMarker: marker)
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
        let marker = makeSyncMarker()
        var oc      = ObjectContainer(schema: schemaJson, syncMarker: marker)
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

    // sync marker length is validated after header decode; the header schema
    // enforces 16 bytes so a wrong-length marker is caught at the schema level.
    // The guard in decode(from:avro:codec:) acts as defense-in-depth.

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

    @Test("ObjectContainer.encode throws when sync marker is not 16 bytes")
    func encodeThrowsOnBadMarkerLength() throws {
        let avro = Avro()
        var oc = ObjectContainer(schema: recordSchema, syncMarker: [1, 2, 3])
        #expect(throws: AvroContainerError.self) {
            _ = try oc.encode(avro: avro, codec: NullCodec())
        }
    }

    @Test("ObjectContainer.decode throws on corrupt block sync marker")
    func decodeThrowsOnCorruptSyncMarker() throws {
        let codec = NullCodec()
        let (avro, marker) = createAvroWithMarker(recordSchema)
        var oc = ObjectContainer(schema: recordSchema, syncMarker: marker)
        try oc.addObject(SimpleModel(), avro: avro)
        var out = try oc.encode(avro: avro, codec: codec)

        // Corrupt the last 16 bytes (block sync marker) so it no longer matches.
        let markerStart = out.count - AvroReservedConstants.syncSize
        for i in markerStart..<out.count {
            out[i] ^= 0xFF
        }

        var newOc = ObjectContainer()
        #expect(throws: AvroContainerError.self) {
            try newOc.decode(from: out, avro: avro, codec: codec)
        }
    }
}

// MARK: - AvroDataReader schema evolution

@Suite("AvroDataReader – schema evolution")
struct AvroDataReaderEvolutionTests {

    @Test("decode(writerSchema:readerSchema:) fills default for new reader field across multiple records")
    func fillsDefaultAcrossMultipleRecords() throws {
        struct V1: Codable { var x: Int64 }
        struct V2: Codable, Equatable { var x: Int64; var label: String }

        let avro = Avro()
        let ws = try #require(avro.newSchema(schema: #"""
            {"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}
        """#))
        let rs = try #require(avro.newSchema(schema: #"""
            {"type":"record","name":"R","fields":[
                {"name":"x","type":"long"},
                {"name":"label","type":"string","default":"n/a"}
            ]}
        """#))

        // Simulate a container block: three V1 records encoded back-to-back.
        var block = Data()
        for i in 1...3 {
            block.append(try avro.encodeFrom(V1(x: Int64(i)), schema: ws))
        }

        let reader = avro.makeDataReader(data: block)
        var results: [V2] = []
        while !reader.isAtEnd {
            results.append(try reader.decode(writerSchema: ws, readerSchema: rs))
        }
        #expect(results == [V2(x: 1, label: "n/a"), V2(x: 2, label: "n/a"), V2(x: 3, label: "n/a")])
    }

    @Test("decode(writerSchema:readerSchema:) discards writer-only fields across multiple records")
    func discardsWriterOnlyFieldsAcrossMultipleRecords() throws {
        struct V1: Codable { var x: Int64; var extra: String }
        struct V2: Codable, Equatable { var x: Int64 }

        let avro = Avro()
        let ws = try #require(avro.newSchema(schema: #"""
            {"type":"record","name":"R","fields":[
                {"name":"x","type":"long"},
                {"name":"extra","type":"string"}
            ]}
        """#))
        let rs = try #require(avro.newSchema(schema: #"""
            {"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}
        """#))

        var block = Data()
        for i in 1...3 {
            block.append(try avro.encodeFrom(V1(x: Int64(i), extra: "drop-\(i)"), schema: ws))
        }

        let reader = avro.makeDataReader(data: block)
        var results: [V2] = []
        while !reader.isAtEnd {
            results.append(try reader.decode(writerSchema: ws, readerSchema: rs))
        }
        #expect(results == [V2(x: 1), V2(x: 2), V2(x: 3)])
    }
}

// MARK: - ObjectContainer schema evolution

@Suite("ObjectContainer – schema evolution")
struct ObjectContainerEvolutionTests {

    @Test("decodeAll(readerSchema:) fills default for new field")
    func decodeAllFillsDefault() throws {
        struct V1: Codable { var a: Int64; var b: String }
        struct V2: Codable, Equatable { var a: Int64; var b: String; var c: String }

        let writerJson = #"""
            {"type":"record","name":"Rec","fields":[
                {"name":"a","type":"long"},
                {"name":"b","type":"string"}
            ]}
        """#
        let readerJson = #"""
            {"type":"record","name":"Rec","fields":[
                {"name":"a","type":"long"},
                {"name":"b","type":"string"},
                {"name":"c","type":"string","default":"default-c"}
            ]}
        """#

        let avro = Avro()
        let rs   = try #require(avro.newSchema(schema: readerJson))

        var oc = ObjectContainer(schema: writerJson, syncMarker: makeSyncMarker())
        try oc.addObjects([V1(a: 1, b: "one"), V1(a: 2, b: "two")], avro: avro)
        let encoded = try oc.encode(avro: avro, codec: NullCodec())

        var reader = ObjectContainer()
        try reader.decode(from: encoded, avro: avro, codec: NullCodec())
        let results: [V2] = try reader.decodeAll(avro: avro, readerSchema: rs)
        #expect(results == [V2(a: 1, b: "one", c: "default-c"), V2(a: 2, b: "two", c: "default-c")])
    }

    @Test("decodeAll(readerSchema:) discards writer-only fields")
    func decodeAllDiscardsWriterOnlyField() throws {
        struct V1: Codable { var id: Int32; var temperature: Double; var location: String }
        struct V2: Codable, Equatable { var id: Int32; var temperature: Double }

        let writerJson = #"""
            {"type":"record","name":"Reading","fields":[
                {"name":"id","type":"int"},
                {"name":"temperature","type":"double"},
                {"name":"location","type":"string"}
            ]}
        """#
        let readerJson = #"""
            {"type":"record","name":"Reading","fields":[
                {"name":"id","type":"int"},
                {"name":"temperature","type":"double"}
            ]}
        """#

        let avro = Avro()
        let rs   = try #require(avro.newSchema(schema: readerJson))

        var oc = ObjectContainer(schema: writerJson, syncMarker: makeSyncMarker())
        try oc.addObjects([V1(id: 1, temperature: 22.5, location: "Auckland"),
                           V1(id: 2, temperature: 18.3, location: "Wellington")], avro: avro)
        let encoded = try oc.encode(avro: avro, codec: NullCodec())

        var reader = ObjectContainer()
        try reader.decode(from: encoded, avro: avro, codec: NullCodec())
        let results: [V2] = try reader.decodeAll(avro: avro, readerSchema: rs)
        #expect(results == [V2(id: 1, temperature: 22.5), V2(id: 2, temperature: 18.3)])
    }

    @Test("decodeAll(readerSchema:) works across multiple blocks")
    func decodeAllAcrossMultipleBlocks() throws {
        struct V1: Codable { var n: Int64 }
        struct V2: Codable, Equatable { var n: Int64; var tag: String }

        let writerJson = """
            {"type":"record","name":"N","fields":[{"name":"n","type":"long"}]}
            """
        let readerJson = """
            {"type":"record","name":"N","fields":[
                {"name":"n","type":"long"},
                {"name":"tag","type":"string","default":"x"}
            ]}
            """

        // Use createAvroWithMarker so avro.schema is set before addObjects is called.
        let (avro, marker) = createAvroWithMarker(writerJson)
        let rs = try #require(avro.newSchema(schema: readerJson))

        var oc = ObjectContainer(schema: writerJson, syncMarker: marker)
        // Force three blocks of 2 objects each by flushing manually.
        try oc.addObjects([V1(n: 1), V1(n: 2)], avro: avro); oc.flush()
        try oc.addObjects([V1(n: 3), V1(n: 4)], avro: avro); oc.flush()
        try oc.addObjects([V1(n: 5), V1(n: 6)], avro: avro)
        let encoded = try oc.encode(avro: avro, codec: NullCodec())

        var reader = ObjectContainer()
        try reader.decode(from: encoded, avro: avro, codec: NullCodec())
        let results: [V2] = try reader.decodeAll(avro: avro, readerSchema: rs)
        #expect(results == (1...6).map { V2(n: Int64($0), tag: "x") })
    }
}
