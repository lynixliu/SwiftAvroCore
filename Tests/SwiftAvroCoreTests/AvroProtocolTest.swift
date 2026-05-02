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
        #expect(fields[0].type == AvroSchema.stringSchema(AvroSchema.StringSchema()))
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
        #expect(fields[0].type == AvroSchema.stringSchema(AvroSchema.StringSchema()))
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

    // MARK: - AvroProtocol addType tests

    @Test("addType adds new schema")
    func addType() throws {
        var proto = try decoded()
        let initialCount = proto.types?.count ?? 0
        let newSchema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"NewRecord","fields":[]}"#))
        proto.addType(schema: newSchema)
        #expect(proto.types?.count == initialCount + 1)
    }

    @Test("addType does not duplicate schema")
    func addTypeDuplicate() throws {
        var proto = try decoded()
        let greeting = try #require(proto.types?[0])
        proto.addType(schema: greeting)
        // count should not increase
        #expect(proto.types?.count == 2)
    }

    // MARK: - AvroProtocol addMessage tests

    @Test("addMessage adds new message")
    func addMessage() throws {
        var proto = try decoded()
        let recordSchema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Req","fields":[]}"#))
        proto.addType(schema: recordSchema)
        let respSchema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Resp","fields":[]}"#))
        proto.addType(schema: respSchema)

        let message = Message(doc: nil, request: nil, response: "Resp", errors: nil, oneway: nil)
        proto.addMessage(name: "newMessage", message: message)
        #expect(proto.messages?["newMessage"] != nil)
    }

    @Test("addMessage ignores invalid message")
    func addMessageInvalid() throws {
        var proto = try decoded()
        let message = Message(doc: nil, request: nil, response: "NonExistent", errors: nil, oneway: nil)
        proto.addMessage(name: "badMessage", message: message)
        #expect(proto.messages?["badMessage"] == nil)
    }

    // MARK: - getRequest tests

    @Test("getRequest returns schemas")
    func getRequest() throws {
        let proto = try decoded()
        let requestSchemas = proto.getRequest(messageName: "hello")
        #expect(requestSchemas?.count == 1)
    }

    @Test("getRequest returns nil for missing message")
    func getRequestMissing() throws {
        let proto = try decoded()
        let requestSchemas = proto.getRequest(messageName: "nonexistent")
        #expect(requestSchemas == nil)
    }

    // MARK: - getResponse tests

    @Test("getResponse returns schema")
    func getResponse() throws {
        let proto = try decoded()
        let response = proto.getResponse(messageName: "hello")
        #expect(response != nil)
    }

    @Test("getResponse returns nil for missing message")
    func getResponseMissing() throws {
        let proto = try decoded()
        let response = proto.getResponse(messageName: "nonexistent")
        #expect(response == nil)
    }

    // MARK: - getErrors tests

    @Test("getErrors returns error schemas")
    func getErrors() throws {
        let proto = try decoded()
        let errors = proto.getErrors(messageName: "hello")
        #expect(errors?["Curse"] != nil)
    }

    @Test("getErrors returns nil for missing message")
    func getErrorsMissing() throws {
        let proto = try decoded()
        let errors = proto.getErrors(messageName: "nonexistent")
        #expect(errors == nil)
    }

    // MARK: - Message tests

    @Test("Message validate passes")
    func messageValidate() throws {
        let proto = try decoded()
        let message = try #require(proto.messages?["hello"])
        let types = try #require(proto.types)
        #expect(message.validate(types: types) == true)
    }

    @Test("Message validate fails for missing type")
    func messageValidateFail() throws {
        let message = Message(doc: nil, request: nil, response: "MissingType", errors: nil, oneway: nil)
        let types: [AvroSchema] = []
        #expect(message.validate(types: types) == false)
    }

    @Test("Message addRequest adds type")
    func messageAddRequest() throws {
        var message = Message(doc: nil, request: nil, response: "Resp", errors: nil, oneway: nil)
        let recSchema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Req","fields":[]}"#))
        if case .recordSchema = recSchema {
            let types: [AvroSchema] = [recSchema]
            message.addRequest(types: types, name: "req", type: "Req")
        }
    }

    @Test("Message addError adds error")
    func messageAddError() throws {
        var message = Message(doc: nil, request: nil, response: "Resp", errors: nil, oneway: nil)
        let errSchema = try #require(Avro().decodeSchema(schema: #"{"type":"error","name":"Err","fields":[]}"#))
        message.addError(types: [errSchema], errorName: "Err")
        #expect(message.errors?.count == 1)
    }

    // MARK: - AvroProtocol mutation coverage

    private let minimalJSON = """
    {"namespace":"x","protocol":"P","messages":{}}
    """

    @Test("decode protocol with no `types` field initialises empty types/typeMap")
    func protocolNoTypes() throws {
        let data = try #require(minimalJSON.data(using: .utf8))
        var p = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(p.types?.isEmpty == true)
        // addType on empty protocol
        let s = AvroSchema.recordSchema(AvroSchema.RecordSchema(
            name: "R", namespace: nil, type: "record",
            fields: [], aliases: nil, doc: nil
        ))
        p.addType(schema: s)
        #expect(p.types?.count == 1)
    }

    @Test("addType is no-op for already-present schema - gap test")
    func addTypeDuplicateGap() throws {
        let data = try #require(minimalJSON.data(using: .utf8))
        var p = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let s = AvroSchema.recordSchema(AvroSchema.RecordSchema(
            name: "R", namespace: nil, type: "record",
            fields: [], aliases: nil, doc: nil
        ))
        p.addType(schema: s)
        p.addType(schema: s)
        #expect(p.types?.count == 1)
    }

    @Test("addMessage with valid types stores message - gap test")
    func addMessageStoresGap() throws {
        let withTypes = """
        {"namespace":"x","protocol":"P",
         "types":[{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}],
         "messages":{}}
        """
        let data = try #require(withTypes.data(using: .utf8))
        var p = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let m = Message(doc: nil,
                        request: [RequestType(name: "x", type: "R")],
                        response: "R",
                        errors: nil,
                        oneway: false)
        p.addMessage(name: "do", message: m)
        #expect(p.messages?["do"] != nil)
    }

    @Test("Message.addRequest dedupes existing entries; addError appends")
    func messageMutators() throws {
        let withTypes = """
        {"namespace":"x","protocol":"P",
         "types":[
           {"type":"record","name":"R","fields":[]},
           {"type":"error","name":"E","fields":[]}
         ],
         "messages":{}}
        """
        let data = try #require(withTypes.data(using: .utf8))
        let p = try JSONDecoder().decode(AvroProtocol.self, from: data)
        var m = Message(doc: nil, request: nil, response: "R",
                        errors: nil, oneway: false)
        m.addRequest(types: p.types ?? [], name: "param", type: "R")
        // Calling again with the same name+type should be a no-op.
        m.addRequest(types: p.types ?? [], name: "param", type: "R")
        #expect(m.request?.count == 1)

        // Unknown type is not added.
        m.addRequest(types: p.types ?? [], name: "other", type: "Unknown")
        #expect(m.request?.count == 1)

        m.addError(types: p.types ?? [], errorName: "E")
        m.addError(types: p.types ?? [], errorName: "E")  // dedup
        #expect(m.errors?.count == 1)

        m.addError(types: p.types ?? [], errorName: "Unknown")  // unknown ignored
        #expect(m.errors?.count == 1)
    }

    @Test("Message.validate true when types match; false otherwise - gap test")
    func messageValidateGap() throws {
        let withTypes = """
        {"namespace":"x","protocol":"P",
         "types":[
           {"type":"record","name":"R","fields":[]},
           {"type":"error","name":"E","fields":[]}
         ],
         "messages":{}}
        """
        let data = try #require(withTypes.data(using: .utf8))
        let p = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let goodMsg = Message(doc: nil,
                              request: [RequestType(name: "p", type: "R")],
                              response: "R", errors: ["E"], oneway: false)
        #expect(goodMsg.validate(types: p.types ?? []) == true)

        let badRequest = Message(doc: nil,
                               request: [RequestType(name: "p", type: "Bogus")],
                               response: "R", errors: nil, oneway: false)
        #expect(badRequest.validate(types: p.types ?? []) == false)

        let badResponse = Message(doc: nil,
                                   request: nil, response: "Bogus",
                                   errors: nil, oneway: false)
        #expect(badResponse.validate(types: p.types ?? []) == false)

        let badError = Message(doc: nil,
                               request: nil, response: nil,
                               errors: ["Bogus"], oneway: false)
        #expect(badError.validate(types: p.types ?? []) == false)
    }

    // MARK: - Protocol equality when types is nil

    @Test("two protocols with nil types are equal")
    func equalityBothTypesNil() throws {
        let json = #"""
        {"namespace":"com.example","protocol":"Ping","messages":{}}
        """#
        let data = try #require(json.data(using: .utf8))
        var p1 = try JSONDecoder().decode(AvroProtocol.self, from: data)
        var p2 = try JSONDecoder().decode(AvroProtocol.self, from: data)
        p1.types = nil
        p2.types = nil
        #expect(p1 == p2)
    }

    // MARK: - addType initialises types array on first call

    @Test("addType when types is nil initialises the array")
    func addTypeInitialisesWhenNil() throws {
        let json = #"""
        {"namespace":"com.example","protocol":"Empty","messages":{}}
        """#
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        proto.types = nil
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        proto.addType(schema: schema)
        #expect(proto.types?.count == 1)
    }
}
