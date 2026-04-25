//
//  SwiftAvroCoreTests.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("SwiftAvroCore End-to-End")
struct SwiftAvroCoreTests {

    @Test("End-to-end encode/decode with JSON schema")
    func endToEnd() throws {
        let jsonSchema = """
        {"type":"record","fields":[
          {"name":"requestId",   "type":"int"},
          {"name":"requestName", "type":"string"},
          {"name":"parameter",   "type":{"type":"array","items":"int"}}
        ]}
        """
        struct Model: Codable { var requestId: Int32; var requestName: String; var parameter: [Int32] }
        let avro    = Avro()
        let model   = Model(requestId: 42, requestName: "hello", parameter: [1, 2])
        let _ = try #require(avro.decodeSchema(schema: jsonSchema))
        let binary: Data  = try avro.encode(model)
        let decoded: Model = try avro.decode(from: binary)
        #expect(decoded.requestId   == model.requestId)
        #expect(decoded.requestName == model.requestName)
        #expect(decoded.parameter   == model.parameter)
    }

    @Test("End-to-end encode/decode with reflected schema")
    func endToEndReflectedSchema() throws {
        struct Model: Codable { var requestId: Int32; var requestName: String; var parameter: [Int32] }
        let avro   = Avro()
        let model  = Model(requestId: 42, requestName: "hello", parameter: [1, 2])
        let schema = try #require(AvroSchema.reflecting(model))
        avro.setSchema(schema: schema)
        let binary:  Data  = try avro.encode(model)
        let decoded: Model = try avro.decode(from: binary)
        #expect(decoded.requestId   == model.requestId)
        #expect(decoded.requestName == model.requestName)
        #expect(decoded.parameter   == model.parameter)
    }
}
