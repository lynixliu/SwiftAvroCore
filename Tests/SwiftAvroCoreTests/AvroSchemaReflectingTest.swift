//
//  AvroSchemaReflectingTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Schema Reflecting")
struct AvroSchemaReflectingTests {

    @Test("Kitty encodes and decodes via reflected schema with double decode")
    func kittensDoubleDecode() throws {
        let kitten = Kitty.random()
        let avro   = Avro()
        let schema = try #require(AvroSchema.reflecting(kitten))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        let _ = avro.decodeSchema(schema: schemaJson)

        let binary:  Data  = try avro.encode(kitten)
        let decoded: Kitty = try avro.decode(from: binary)
        #expect(decoded == kitten)
    }

    @Test("Kitty encodes and decodes via directly set schema")
    func kittensDirectSet() throws {
        let kitten = Kitty.random()
        let avro   = Avro()
        let schema = try #require(AvroSchema.reflecting(kitten))
        avro.setSchema(schema: schema)

        let binary:  Data  = try avro.encode(kitten)
        let decoded: Kitty = try avro.decode(from: binary)
        #expect(decoded == kitten)
    }

    @Test("KittyAction encodes and decodes via reflected schema")
    func kittenActions() throws {
        let action = KittyAction.random()
        let avro   = Avro()
        let schema = try #require(AvroSchema.reflecting(action))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        let _ = avro.decodeSchema(schema: schemaJson)

        let binary:  Data        = try avro.encode(action)
        let decoded: KittyAction = try avro.decode(from: binary)
        #expect(decoded.dataValue   == action.dataValue)
        #expect(decoded.label       == action.label)
        #expect(decoded.type        == action.type)
        #expect(decoded.floatValue  == action.floatValue)
        #expect(decoded.doubleValue == action.doubleValue)
        #expect(decoded.kitty       == action.kitty)
        #expect(abs(decoded.timestamp.timeIntervalSinceReferenceDate -
                    action.timestamp.timeIntervalSinceReferenceDate) < 1)
    }
}
