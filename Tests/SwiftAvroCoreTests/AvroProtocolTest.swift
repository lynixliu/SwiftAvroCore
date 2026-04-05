//
//  AvroProtocolTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Protocol")
struct AvroProtocolTests {

    private let helloWorldJSON = """
    {
      "namespace": "com.acme", "protocol": "HelloWorld", "doc": "Protocol Greetings",
      "types": [
        {"name":"Greeting","type":"record","fields":[{"name":"message","type":"string"}]},
        {"name":"Curse",   "type":"error", "fields":[{"name":"message","type":"string"}]}
      ],
      "messages": {
        "hello": {
          "doc": "Say hello.",
          "request":  [{"name":"greeting","type":"Greeting"}],
          "response": "Greeting",
          "errors":   ["Curse"]
        }
      }
    }
    """

    private func decoded() throws -> AvroProtocol {
        let data = try #require(helloWorldJSON.data(using: .utf8))
        return try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    @Test("Top-level protocol fields decode correctly")
    func topLevelFields() throws {
        let proto = try decoded()
        #expect(proto.type      == "protocol")
        #expect(proto.name      == "HelloWorld")
        #expect(proto.namespace == "com.acme")
        #expect(proto.doc       == "Protocol Greetings")
    }

    @Test("Types array has correct count")
    func typesCount() throws {
        #expect(try decoded().types?.count == 2)
    }

    @Test("Greeting record type decodes correctly")
    func greetingRecord() throws {
        let proto    = try decoded()
        let greeting = try #require(proto.types?[0])
        #expect(greeting.getName()     == "Greeting")
        #expect(greeting.getTypeName() == "record")
        let fields = try #require(greeting.getRecord()?.fields)
        #expect(fields.count   == 1)
        #expect(fields[0].name == "message")
        #expect(fields[0].type == AvroSchema.stringSchema)
    }

    @Test("Curse error type decodes correctly")
    func curseError() throws {
        let proto = try decoded()
        let curse = try #require(proto.types?[1])
        #expect(curse.getName()     == "Curse")
        #expect(curse.getTypeName() == "error")
        let fields = try #require(curse.getError()?.fields)
        #expect(fields.count   == 1)
        #expect(fields[0].name == "message")
        #expect(fields[0].type == AvroSchema.stringSchema)
    }

    @Test("Messages map has correct count")
    func messagesCount() throws {
        #expect(try decoded().messages?.count == 1)
    }

    @Test("Hello message decodes correctly")
    func helloMessage() throws {
        let proto = try decoded()
        let hello = try #require(proto.messages?["hello"])
        #expect(hello.doc              == "Say hello.")
        #expect(hello.request?.count   == 1)
        #expect(hello.request?[0].name == "greeting")
        #expect(hello.request?[0].type == "Greeting")
        #expect(hello.response         == "Greeting")
        #expect(hello.errors           == ["Curse"])
    }

    @Test("Two decodings of same JSON are equal")
    func equality() throws {
        #expect(try decoded() == decoded())
    }

    @Test("Missing protocol key throws")
    func missingProtocolKeyThrows() {
        let badJSON = #"{"namespace":"com.acme","types":[],"messages":{}}"#
        let data    = badJSON.data(using: .utf8)!
        #expect(throws: (any Error).self) {
            try JSONDecoder().decode(AvroProtocol.self, from: data)
        }
    }

    @Test("HandshakeRequest with no clientProtocol and no meta decodes correctly")
    func requestDecodeNullProtocolNoMeta() throws {
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0,
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0
        ])
        let avro   = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.requestSchema))
        let model  = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
        #expect(model.clientHash == [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        #expect(model.clientProtocol == nil)
        #expect(model.meta == nil)
    }

    @Test("HandshakeRequest with clientProtocol and no meta decodes correctly")
    func requestDecodeWithProtocolNoMeta() throws {
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0x02, 0x06, 0x66, 0x6f, 0x6f,
            0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0
        ])
        let avro  = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.requestSchema))
        let model  = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
        #expect(model.clientProtocol == "foo")
        #expect(model.meta == nil)
    }

    @Test("HandshakeResponse BOTH match encodes to four zero bytes")
    func responseEncodeBOTH() throws {
        let avro   = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.responseSchema))
        let data   = try AvroEncoder().encode(
            HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil),
            schema: schema)
        #expect(data == Data([0, 0, 0, 0]))
    }

    @Test("HandshakeResponse NONE first byte is zigzag 4")
    func responseEncodeNONE() throws {
        let avro   = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.responseSchema))
        let resp   = HandshakeResponse(
            match: .NONE, serverProtocol: "foo",
            serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        let data   = try AvroEncoder().encode(resp, schema: schema)
        #expect(data.first == 4)
    }
}
