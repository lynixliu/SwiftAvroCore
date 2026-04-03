//
//  AvroRequestResponseTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 15/03/22.
//

import XCTest
@testable import SwiftAvroCore
// MARK: - AvroRequestResponseTest

// MARK: - AvroRequestResponseTest

final class AvroRequestResponseTest: XCTestCase {

    // MARK: Shared fixture

    private struct Fixture {
        let supportProtocol: String = """
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
        let clientHash: MD5Hash = [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10]
        let serverHash: MD5Hash = [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10]
        let context = Context(requestMeta: [:], responseMeta: [:])
    }

    private struct Greeting: Codable, Equatable { var message: String }
    private struct Curse:    Codable, Equatable { var message: String }

    // MARK: - Context

    func testContext_schemasInitialised() {
        let ctx = Context(requestMeta: [:], responseMeta: [:])
        XCTAssertEqual(ctx.requestSchema.getName(),  "HandshakeRequest")
        XCTAssertEqual(ctx.responseSchema.getName(), "HandshakeResponse")
        // metaSchema is a map — getName() returns "map"
        XCTAssertEqual(ctx.metaSchema.getTypeName(), "map")
    }

    // MARK: - Handshake: NONE → retry → BOTH

    func testHandshake_fullFlow_NONE_then_BOTH() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)

        // Step 1: initial request — clientProtocol=null, serverHash=clientHash (spec §7)
        let initialReq = try client.initHandshakeRequest()
        XCTAssertEqual(initialReq, Data([
            0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,   // clientHash
            0,                                           // null clientProtocol
            0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,   // serverHash = clientHash
            0                                            // null meta
        ]), "initial handshake request encoding mismatch")

        // Step 2: server doesn't know client → NONE + server protocol + server hash
        let (_, noneData) = try server.resolveHandshakeRequest(from: initialReq)
        let (noneResp, nonePayload) = try client.decodeResponse(from: noneData)
        XCTAssertEqual(noneResp.match,          .NONE,               "expected NONE")
        XCTAssertEqual(noneResp.serverHash,     fix.serverHash,      "server hash mismatch")
        XCTAssertEqual(noneResp.serverProtocol, fix.supportProtocol, "server protocol mismatch")
        XCTAssertNil(noneResp.meta)
        XCTAssertEqual(nonePayload, Data(), "no payload expected with NONE response")

        // Step 3: client retries with protocol + correct server hash
        let retryReq = try XCTUnwrap(
            client.resolveHandshakeResponse(noneResp),
            "client must produce a retry request after NONE"
        )

        // Step 4: server registers client → BOTH
        let (_, bothData) = try server.resolveHandshakeRequest(from: retryReq)
        let (bothResp, bothPayload) = try client.decodeResponse(from: bothData)
        XCTAssertEqual(bothResp.match, .BOTH, "expected BOTH after retry")
        XCTAssertNil(bothResp.serverProtocol)
        XCTAssertNil(bothResp.serverHash)
        XCTAssertEqual(bothPayload, Data(), "no payload expected with BOTH response")
    }

    // MARK: - Handshake: known client with stale server hash → CLIENT

    func testHandshake_clientMatch() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)

        // Pre-register client so server knows it, but client sends wrong serverHash
        try server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.clientHash)

        let (_, responseData) = try server.resolveHandshakeRequest(from: client.initHandshakeRequest())
        let (response, _)     = try client.decodeResponse(from: responseData)

        XCTAssertEqual(response.match,          .CLIENT,             "expected CLIENT match")
        XCTAssertEqual(response.serverHash,     fix.serverHash,      "server hash mismatch")
        XCTAssertEqual(response.serverProtocol, fix.supportProtocol, "server protocol mismatch")
        XCTAssertNil(response.meta)

        // CLIENT match: resolveHandshakeResponse must return nil (session cached, no retry needed)
        let followUp = try client.resolveHandshakeResponse(response)
        XCTAssertNil(followUp, "CLIENT match should not produce a follow-up request")
    }

    // MARK: - Handshake: already-registered client with correct server hash → BOTH directly

    func testHandshake_knownClient_correctServerHash_BOTH() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        // Register client with the matching protocol
        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)

        let (_, respData) = try server.resolveHandshakeRequest(from: reqData)
        let (resp, _)     = try client.decodeResponse(from: respData)

        XCTAssertEqual(resp.match, .BOTH)
        XCTAssertNil(resp.serverHash)
        XCTAssertNil(resp.serverProtocol)
        XCTAssertNil(resp.meta)
    }

    // MARK: - Ping (empty message name)

    func testRequestPing_emptyMessageName() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try server.resolveHandshakeRequest(from: reqData)

        struct EmptyMessage: Codable {}
        let msgData = try client.writeRequest(messageName: "", parameters: [EmptyMessage()])

        // Avro IPC spec: empty name → 0 (null meta) + 0 (zero-length string zig-zag)
        XCTAssertEqual(msgData, Data([0, 0]), "ping encoding mismatch")

        let (header, params) = try server.readRequest(header: handshake, from: msgData)
                                   as (RequestHeader, [EmptyMessage])
        XCTAssertNil(header.meta)
        XCTAssertEqual(header.name,  "")
        XCTAssertEqual(params.count, 0)
    }

    // MARK: - Normal request / response round-trip

    func testRequestResponse_normalOK() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try server.resolveHandshakeRequest(from: reqData)

        // ── encode request ───────────────────────────────────────────────────────
        let msgData = try client.writeRequest(messageName: "hello",
                                              parameters: [Greeting(message: "requestData")])

        var expectedReq = Data()
        expectedReq.append(contentsOf: [0])               // null meta
        expectedReq.append(contentsOf: [10])              // "hello" zig-zag = 5×2
        expectedReq.append("hello".data(using: .utf8)!)
        expectedReq.append(contentsOf: [22])              // "requestData" zig-zag = 11×2
        expectedReq.append("requestData".data(using: .utf8)!)
        XCTAssertEqual(msgData, expectedReq, "request encoding mismatch")

        // ── server reads request ─────────────────────────────────────────────────
        let (reqHeader, greetings) = try server.readRequest(header: handshake, from: msgData)
                                         as (RequestHeader, [Greeting])
        XCTAssertNil(reqHeader.meta)
        XCTAssertEqual(reqHeader.name,       "hello")
        XCTAssertEqual(greetings.count,      1)
        XCTAssertEqual(greetings[0].message, "requestData")

        // ── server writes response ───────────────────────────────────────────────
        let resData = try server.writeResponse(header: handshake,
                                               messageName: reqHeader.name,
                                               parameter: Greeting(message: "responseData"))

        var expectedRes = Data()
        expectedRes.append(contentsOf: [0])               // null meta
        expectedRes.append(contentsOf: [0])               // false flag
        expectedRes.append(contentsOf: [24])              // "responseData" zig-zag = 12×2
        expectedRes.append("responseData".data(using: .utf8)!)
        XCTAssertEqual(resData, expectedRes, "response encoding mismatch")

        // ── client reads response ────────────────────────────────────────────────
        let (resHeader, responses) = try client.readResponse(header: handshake,
                                                             messageName: "hello",
                                                             from: resData)
                                         as (ResponseHeader, [Greeting])
        XCTAssertNil(resHeader.meta)
        XCTAssertFalse(resHeader.flag,        "flag must be false for normal response")
        XCTAssertEqual(responses.count,       1)
        XCTAssertEqual(responses[0].message,  "responseData")
    }

    // MARK: - Error response round-trip

    func testRequestResponse_errorResponse() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try server.resolveHandshakeRequest(from: reqData)

        let resData = try server.writeErrorResponse(header: handshake,
                                                    messageName: "hello",
                                                    errors: ["Curse": Curse(message: "responseError")])

        var expectedErr = Data()
        expectedErr.append(contentsOf: [0])    // null meta
        expectedErr.append(contentsOf: [1])    // true flag (error)
        expectedErr.append(contentsOf: [2])    // union index 1 → zig-zag 2
        expectedErr.append(contentsOf: [26])   // "responseError" zig-zag = 13×2
        expectedErr.append("responseError".data(using: .utf8)!)
        XCTAssertEqual(resData, expectedErr, "error response encoding mismatch")

        let (resHeader, errors) = try client.readResponse(header: handshake,
                                                          messageName: "hello",
                                                          from: resData)
                                      as (ResponseHeader, [Curse])
        XCTAssertNil(resHeader.meta)
        XCTAssertTrue(resHeader.flag,       "flag must be true for error response")
        XCTAssertEqual(errors.count,        1)
        XCTAssertEqual(errors[0].message,   "responseError")
    }

    // MARK: - decodeFromContinue (Avro façade)

    func testDecodeFromContinue_handshakeRequest() throws {
        // Verify that Avro.decodeFromContinue returns the correct consumed-byte offset.
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0,
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0
        ])
        let avro   = Avro()
        let schema = try XCTUnwrap(avro.newSchema(schema: MessageConstant.requestSchema))
        let (model, consumed): (HandshakeRequest, Int) =
            try avro.decodeFromContinue(from: raw, schema: schema)

        XCTAssertEqual(consumed, raw.count, "all bytes should be consumed for this request")
        XCTAssertNil(model.clientProtocol)
        XCTAssertNil(model.meta)
    }

    func testDecodeFromContinue_partialBuffer() throws {
        // Encode a HandshakeResponse, append garbage, verify offset stops at message boundary.
        let avro         = Avro()
        let respSchema   = try XCTUnwrap(avro.newSchema(schema: MessageConstant.responseSchema))
        let response     = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil)
        let encoded      = try AvroEncoder().encode(response, schema: respSchema)
        let garbage      = Data([0xFF, 0xFF, 0xFF])
        let combined     = encoded + garbage

        let (decoded, consumed): (HandshakeResponse, Int) =
            try avro.decodeFromContinue(from: combined, schema: respSchema)

        XCTAssertEqual(decoded.match, .BOTH)
        XCTAssertEqual(consumed, encoded.count,
                       "decodeFromContinue must stop at message boundary, not consume garbage")
    }

    // MARK: - encodeFrom / decodeFrom round-trip (Avro façade)

    func testEncodeFrom_decodeFrom_roundTrip() throws {
        let avro     = Avro()
        let schema   = try XCTUnwrap(avro.newSchema(schema: MessageConstant.responseSchema))
        let original = HandshakeResponse(
            match: .CLIENT,
            serverProtocol: "test",
            serverHash: Array(repeating: 0xAB, count: 16)
        )
        let encoded: Data = try avro.encodeFrom(original, schema: schema)
        let decoded: HandshakeResponse = try avro.decodeFrom(from: encoded, schema: schema)

        XCTAssertEqual(decoded.match,          original.match)
        XCTAssertEqual(decoded.serverProtocol, original.serverProtocol)
        XCTAssertEqual(decoded.serverHash,     original.serverHash)
    }

    // MARK: - Session management

    func testSessionCache_addAndRemove() throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)

        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        XCTAssertNotNil(client.sessionCache[fix.serverHash],
                        "session should be present after addSession")

        client.removeSession(for: fix.serverHash)
        XCTAssertNil(client.sessionCache[fix.serverHash],
                     "session should be absent after removeSession")
    }

    func testSessionCache_clearAll() throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)

        try client.addSession(hash: fix.clientHash, protocolString: fix.supportProtocol)
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        client.clearSessions()
        XCTAssertTrue(client.sessionCache.isEmpty, "clearSessions must empty the cache")
    }

    func testSessionCache_invalidJSON_throws() throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)
        XCTAssertThrowsError(
            try client.addSession(hash: fix.serverHash, protocolString: "{not valid json}"),
            "invalid protocol JSON must throw"
        )
    }

    func testServerSessionCache_addAndRemove() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)

        try server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.clientHash)
        XCTAssertNotNil(server.sessionCache[fix.clientHash])

        server.removeSession(for: fix.clientHash)
        XCTAssertNil(server.sessionCache[fix.clientHash])
    }

    func testServerSessionCache_clearAll() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)

        try server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.clientHash)
        try server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.serverHash)
        server.clearSessions()
        XCTAssertTrue(server.sessionCache.isEmpty)
    }

    // MARK: - Framing

    func testFraming_cases() {
        struct Case {
            let name:           String
            let input:          [UInt8]
            let expectFramed:   [UInt8]          // changed to [UInt8] for easier reading
            let expectDeframed: [[UInt8]]
        }
        
        let frameLen: Int = 4
        let cases: [Case] = [
            Case(name: "empty",
                 input: [],
                 expectFramed:   [0,0,0,0],
                 expectDeframed: []),
            
            Case(name: "less than frameLen",
                 input: [1,2,3],
                 expectFramed:   [0,0,0,3, 1,2,3, 0,0,0,0],
                 expectDeframed: [[1,2,3]]),
            
            Case(name: "equal to frameLen",
                 input: [1,2,3,4],
                 expectFramed:   [0,0,0,4, 1,2,3,4, 0,0,0,0],
                 expectDeframed: [[1,2,3,4]]),
            
            Case(name: "2 frames split",
                 input: [1,2,3,4,5],
                 expectFramed:   [0,0,0,4, 1,2,3,4, 0,0,0,1, 5, 0,0,0,0],
                 expectDeframed: [[1,2,3,4], [5]]),
            
            Case(name: "2 full frames",
                 input: [1,2,3,4,5,6,7,8],
                 expectFramed:   [0,0,0,4, 1,2,3,4, 0,0,0,4, 5,6,7,8, 0,0,0,0],
                 expectDeframed: [[1,2,3,4], [5,6,7,8]]),
            
            Case(name: "3 frames",
                 input: [1,2,3,4,5,6,7,8,9,10],
                 expectFramed:   [0,0,0,4, 1,2,3,4, 0,0,0,4, 5,6,7,8, 0,0,0,2, 9,10, 0,0,0,0],
                 expectDeframed: [[1,2,3,4], [5,6,7,8], [9,10]]),
        ]
        
        for c in cases {
            var data = Data(c.input)
            data.frame(maxFrameLength: frameLen)                    // using the mutating version
            
            XCTAssertEqual(data, Data(c.expectFramed), "\(c.name): framing mismatch")
            
            let deframed = data.deFraming()
            let deframedBytes = deframed.map { Array($0) }
            XCTAssertEqual(deframedBytes, c.expectDeframed, "\(c.name): deframing mismatch")
        }
    }

    func testFraming_roundTrip() {
        let payload = Data((0..<100).map { UInt8($0 % 256) })
        
        for frameLen: Int32 in [1, 4, 16, 64, 256] {
            var framed = payload
            framed.frame(maxFrameLength: Int(frameLen))   // mutating version
            
            let frames = framed.deFraming()
            let recovered = frames.reduce(Data(), +)
            
            XCTAssertEqual(recovered, payload,
                           "round-trip failed for frameLen=\(frameLen)")
            
            // Correct assertions:
            XCTAssertFalse(frames.isEmpty, "should recover at least one frame for non-empty payload")
            XCTAssertGreaterThanOrEqual(frames.count, 1, "should have at least one frame")
            
            // Optional: verify that the framed data actually ends with the zero terminator
            XCTAssertTrue(framed.count >= 4, "framed data must be at least 4 bytes (terminator)")
            let lastFour = framed.suffix(4)
            XCTAssertEqual(Array(lastFour), [0, 0, 0, 0],
                           "framed data must end with zero-length terminator")
        }
    }

    // MARK: - Performance

    func testHandshakePerformance() throws {
        let fix = Fixture()
        measure {
            guard
                let server = try? MessageResponse(context: fix.context,
                                                  serverHash: fix.serverHash,
                                                  serverProtocol: fix.supportProtocol),
                let client = try? MessageRequest(context: fix.context,
                                                 clientHash: fix.clientHash,
                                                 clientProtocol: fix.supportProtocol),
                let req    = try? client.initHandshakeRequest()
            else { return }
            _ = try? server.resolveHandshakeRequest(from: req)
        }
    }

    func testRequestResponsePerformance() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        // Establish session once, then benchmark the message encode/decode hot path.
        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try server.resolveHandshakeRequest(from: reqData)

        measure {
            guard
                let msgData = try? client.writeRequest(messageName: "hello",
                                                       parameters: [Greeting(message: "hi")]),
                let (_, _)  = try? server.readRequest(header: handshake, from: msgData)
                                   as (RequestHeader, [Greeting])
            else { return }
        }
    }
}
