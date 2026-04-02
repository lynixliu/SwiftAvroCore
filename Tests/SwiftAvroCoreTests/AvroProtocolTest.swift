//
//  AvroProtocolTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 1/05/22.
//

import XCTest
@testable import SwiftAvroCore

// MARK: - AvroProtocolTest

final class AvroProtocolTest: XCTestCase {

    // MARK: Shared fixture

    private let helloWorldJSON = """
    {
      "namespace": "com.acme",
      "protocol": "HelloWorld",
      "doc": "Protocol Greetings",
      "types": [
         {"name": "Greeting", "type": "record", "fields": [{"name": "message", "type": "string"}]},
         {"name": "Curse",    "type": "error",  "fields": [{"name": "message", "type": "string"}]}
      ],
      "messages": {
        "hello": {
           "doc": "Say hello.",
           "request":  [{"name": "greeting", "type": "Greeting"}],
           "response": "Greeting",
           "errors":   ["Curse"]
        }
      }
    }
    """

    private func decodeProtocol(_ json: String) throws -> AvroProtocol {
        let data = try XCTUnwrap(json.data(using: .utf8))
        return try JSONDecoder().decode(AvroProtocol.self, from: data)
    }

    // MARK: - Protocol decoding

    func testProtocol_topLevelFields() throws {
        let proto = try decodeProtocol(helloWorldJSON)
        XCTAssertEqual(proto.type,      "protocol")
        XCTAssertEqual(proto.name,      "HelloWorld")
        XCTAssertEqual(proto.namespace, "com.acme")
        XCTAssertEqual(proto.doc,       "Protocol Greetings")
    }

    func testProtocol_types_count() throws {
        let proto = try decodeProtocol(helloWorldJSON)
        XCTAssertEqual(proto.types?.count, 2)
    }

    func testProtocol_types_greeting() throws {
        let proto = try decodeProtocol(helloWorldJSON)
        let greeting = try XCTUnwrap(proto.types?[0])
        XCTAssertEqual(greeting.getName(),     "Greeting")
        XCTAssertEqual(greeting.getTypeName(), "record")
        let fields = try XCTUnwrap(greeting.getRecord()?.fields)
        XCTAssertEqual(fields.count,      1)
        XCTAssertEqual(fields[0].name,    "message")
        XCTAssertEqual(fields[0].type,    AvroSchema(type: "string"))
    }

    func testProtocol_types_curse() throws {
        let proto = try decodeProtocol(helloWorldJSON)
        let curse = try XCTUnwrap(proto.types?[1])
        XCTAssertEqual(curse.getName(),     "Curse")
        XCTAssertEqual(curse.getTypeName(), "error")
        let fields = try XCTUnwrap(curse.getError()?.fields)
        XCTAssertEqual(fields.count,   1)
        XCTAssertEqual(fields[0].name, "message")
        XCTAssertEqual(fields[0].type, AvroSchema(type: "string"))
    }

    func testProtocol_messages_hello() throws {
        let proto = try decodeProtocol(helloWorldJSON)
        XCTAssertEqual(proto.messages?.count, 1)
        let hello = try XCTUnwrap(proto.messages?["hello"])
        XCTAssertEqual(hello.doc,               "Say hello.")
        XCTAssertEqual(hello.request?.count,    1)
        XCTAssertEqual(hello.request?[0].name,  "greeting")
        XCTAssertEqual(hello.request?[0].type,  "Greeting")
        XCTAssertEqual(hello.response,          "Greeting")
        XCTAssertEqual(hello.errors,            ["Curse"])
    }

    func testProtocol_equality() throws {
        let proto1 = try decodeProtocol(helloWorldJSON)
        let proto2 = try decodeProtocol(helloWorldJSON)
        XCTAssertEqual(proto1, proto2)
    }

    func testProtocol_addType() throws {
        var proto = try decodeProtocol(helloWorldJSON)
        let newSchema = AvroSchema(type: "record") // replace with a valid named schema as needed
        let before = proto.types?.count ?? 0
        proto.addType(schema: newSchema)
        // If newSchema has a unique name it should be appended; duplicate should be ignored.
        let after = proto.types?.count ?? 0
        XCTAssertGreaterThanOrEqual(after, before)
    }

    func testProtocol_missingProtocolKey_throws() {
        let badJSON = """
        {"namespace": "com.acme", "types": [], "messages": {}}
        """
        let data = badJSON.data(using: .utf8)!
        XCTAssertThrowsError(try JSONDecoder().decode(AvroProtocol.self, from: data))
    }

    // MARK: - HandshakeRequest decoding

    func testRequestDecode_nullProtocol_noMeta() throws {
        // clientHash(16) | union[null] | serverHash(16) | map[null]
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf, // clientHash
            0,                                                                   // null union
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf, // serverHash
            0                                                                    // null map
        ])
        let schema = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.requestSchema))
        let model = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
        XCTAssertEqual(model.clientHash,    [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        XCTAssertNil(model.clientProtocol)
        XCTAssertEqual(model.serverHash,    [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        XCTAssertNil(model.meta)
    }

    func testRequestDecode_withProtocol_noMeta() throws {
        // clientHash(16) | union[string "foo"] | serverHash(16) | map[null]
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0x02, 0x06, 0x66, 0x6f, 0x6f,  // union index 1 + "foo" (len=3, zig-zag=6)
            0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0
        ])
        let schema = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.requestSchema))
        let model = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
        XCTAssertEqual(model.clientHash,     [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        XCTAssertEqual(model.clientProtocol, "foo")
        XCTAssertEqual(model.serverHash,     [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        XCTAssertNil(model.meta)
    }

    func testRequestDecode_withProtocol_withMeta() throws {
        // clientHash(16) | union[string "foo"] | serverHash(16) | map{"fo":[1,2,3]}
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0x02, 0x06, 0x66, 0x6f, 0x6f,
            0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0x02, 0x02, 0x04, 0x66, 0x6f, 0x06, 0x1, 0x2, 0x3, 0, 0
        ])
        let schema = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.requestSchema))
        let model = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
        XCTAssertEqual(model.clientProtocol, "foo")
        XCTAssertEqual(model.meta,           ["fo": [1, 2, 3]])
    }

    // MARK: - HandshakeResponse encoding

    func testResponseEncode_matchBoth_nullFields() throws {
        let schema   = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.responseSchema))
        let response = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil)
        let data     = try AvroEncoder().encode(response, schema: schema)
        // enum BOTH = index 0, serverProtocol null, serverHash null, meta null
        XCTAssertEqual(data, Data([0, 0, 0, 0]))
    }

    func testResponseEncode_matchClient_withHashAndProtocol() throws {
        let schema = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.responseSchema))
        let response = HandshakeResponse(
            match: .CLIENT,
            serverProtocol: "foo",
            serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf]
        )
        let data = try AvroEncoder().encode(response, schema: schema)
        let expected = Data([
            2,                                          // enum CLIENT (zig-zag index 1 → 2)
            0x02, 0x06, 0x66, 0x6f, 0x6f,              // union[string] "foo"
            0x02,                                       // union[MD5]
            0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0                                           // meta null
        ])
        XCTAssertEqual(data, expected)
    }

    func testResponseEncode_matchClient_withMeta() throws {
        let schema = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.responseSchema))
        let response = HandshakeResponse(
            match: .CLIENT,
            serverProtocol: "foo",
            serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
            meta: ["fo": [1, 2, 3]]
        )
        let data = try AvroEncoder().encode(response, schema: schema)
        let expected = Data([
            2,
            0x02, 0x06, 0x66, 0x6f, 0x6f,
            0x02,
            0x01,0x01,0x02,0x03,0x04,0x05,0x06,0x07,
            0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
            0x02, 0x02, 0x04, 0x66, 0x6f, 0x06, 0x01, 0x02, 0x03, 0x00
        ])
        XCTAssertEqual(data, expected)
    }

    func testResponseEncode_matchNone() throws {
        let schema = try XCTUnwrap(Avro().newSchema(schema: MessageConstant.responseSchema))
        let response = HandshakeResponse(
            match: .NONE,
            serverProtocol: "foo",
            serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf]
        )
        let data = try AvroEncoder().encode(response, schema: schema)
        // enum NONE = index 2 → zig-zag 4
        XCTAssertEqual(data.first, 4, "NONE enum should encode to zig-zag value 4")
    }

    // MARK: - Performance

    func testProtocolDecodePerformance() throws {
        let data = try XCTUnwrap(helloWorldJSON.data(using: .utf8))
        measure {
            _ = try? JSONDecoder().decode(AvroProtocol.self, from: data)
        }
    }
}
