//
//  SchemalessCodingDecodingTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Schemaless Coding / Decoding")
struct SchemalessCodingDecodingTests {

    @Test("Kitty decodes to [String:Any] with correct field values")
    func schemalessKittensDoubleDecode() throws {
        let kitten = Kitty.random()
        let avro   = SwiftAvroCore()
        let schema = try #require(AvroSchema.reflecting(kitten))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        let _ = avro.decodeSchema(schema: schemaJson)

        let binary = try avro.encode(kitten)
        let decoded = try avro.decode(from: binary) as! [String: Any]
        #expect(kitten.name        == decoded["name"]  as! String)
        #expect(kitten.color.rawValue == decoded["color"] as! String)
    }

    @Test("KittyAction decodes to [String:Any] with correct field values")
    func schemalessKittenActions() throws {
        let action = KittyAction.random()
        let avro   = SwiftAvroCore()
        let schema = try #require(AvroSchema.reflecting(action))
        let schemaJson = try String(decoding: avro.encodeSchema(schema: schema), as: UTF8.self)
        let _ = avro.decodeSchema(schema: schemaJson)

        let binary  = try avro.encode(action)
        let decoded = try avro.decode(from: binary) as! [String: Any]
        #expect(abs((decoded["timestamp"] as! Date).timeIntervalSinceReferenceDate -
                    action.timestamp.timeIntervalSinceReferenceDate) < 1)
        #expect(action.dataValue      == decoded["dataValue"]   as! [UInt8])
        #expect(action.label          == decoded["label"]        as! String)
        #expect(action.type.rawValue  == decoded["type"]         as! String)
        #expect(action.floatValue     == decoded["floatValue"]   as! Float)
        #expect(action.doubleValue    == decoded["doubleValue"]  as! Double)

        let kitty = decoded["kitty"] as! [String: Any]
        #expect(action.kitty.name        == kitty["name"]  as! String)
        #expect(action.kitty.color.rawValue == kitty["color"] as! String)
    }
}
