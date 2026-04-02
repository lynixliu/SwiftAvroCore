//
//  AvroRequestResponseTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 15/03/22.
//

import XCTest
@testable import SwiftAvroCore
// MARK: - AvroRequestResponseTest

final class AvroRequestResponseTest: XCTestCase {

    // MARK: Shared fixtures

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

    // MARK: - Handshake state machine

    /// Full NONE → retry with protocol → BOTH sequence.
    func testHandshake_fullFlow() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash, serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context,  clientHash: fix.clientHash, clientProtocol: fix.supportProtocol)

        // 1. Initial request: clientProtocol = null, serverHash = clientHash
        let initialRequest = try client.initHandshakeRequest()
        XCTAssertEqual(
            initialRequest,
            Data([0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,  // clientHash
                  0,                                          // null clientProtocol
                  0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,  // serverHash = clientHash
                  0]),                                        // null meta
            "initial request encoding mismatch"
        )

        // 2. Server doesn't know the client — responds NONE with its own protocol + hash
        let (_, responseNoneData) = try server.resolveHandshakeRequest(from: initialRequest)
        let (responseNone, payload1) = try client.decodeResponse(from: responseNoneData)
        XCTAssertEqual(responseNone.match,          .NONE,                 "expected NONE match")
        XCTAssertEqual(responseNone.serverHash,     fix.serverHash,        "server hash mismatch")
        XCTAssertEqual(responseNone.serverProtocol, fix.supportProtocol,   "server protocol mismatch")
        XCTAssertNil(responseNone.meta)
        XCTAssertEqual(payload1, Data(), "no payload expected after handshake")

        // 3. Client retries with its protocol and the correct server hash
        let retryRequest = try XCTUnwrap(
            try client.resolveHandshakeResponse(responseNone),
            "client should produce a retry request after NONE"
        )

        // 4. Server now knows the client — responds BOTH
        let (_, responseBothData) = try server.resolveHandshakeRequest(from: retryRequest)
        let (responseBoth, payload2) = try client.decodeResponse(from: responseBothData)
        XCTAssertEqual(responseBoth.match, .BOTH, "expected BOTH after retry")
        XCTAssertNil(responseBoth.serverProtocol)
        XCTAssertNil(responseBoth.serverHash)
        XCTAssertEqual(payload2, Data(), "no payload expected")
    }

    /// Client already knows the server hash but server hasn't seen the client yet
    /// (wrong server hash on first attempt) → CLIENT match.
    func testHandshake_clientMatch() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash, serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context,  clientHash: fix.clientHash, clientProtocol: fix.supportProtocol)

        // Pre-register the client's protocol on the server so it produces CLIENT, not NONE.
        try server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.clientHash)

        // Client sends a request with wrong server hash (= clientHash)
        let request = try client.initHandshakeRequest()
        let (_, responseData) = try server.resolveHandshakeRequest(from: request)
        let (response, _) = try client.decodeResponse(from: responseData)

        XCTAssertEqual(response.match,          .CLIENT,            "expected CLIENT match")
        XCTAssertEqual(response.serverHash,     fix.serverHash,     "server hash mismatch")
        XCTAssertEqual(response.serverProtocol, fix.supportProtocol,"server protocol mismatch")

        // resolveHandshakeResponse for CLIENT should return nil (no further request needed)
        let followUp = try client.resolveHandshakeResponse(response)
        XCTAssertNil(followUp, "CLIENT match requires no retry request")
    }

    /// Ping: empty message name — server ignores parameters and returns empty response.
    func testHandshake_ping() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash, serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context,  clientHash: fix.serverHash, clientProtocol: fix.supportProtocol)

        let requestData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash, clientProtocol: fix.supportProtocol, serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)

        let (handshake, responseData) = try server.resolveHandshakeRequest(from: requestData)
        let (response, _) = try client.decodeResponse(from: responseData)
        XCTAssertEqual(response.match, .BOTH)
        XCTAssertNil(response.serverHash)
        XCTAssertNil(response.serverProtocol)
        XCTAssertNil(response.meta)

        struct EmptyMessage: Codable {}
        let msgData = try client.writeRequest(messageName: "", parameters: [EmptyMessage()])

        // Per spec: empty message name → 0 (null meta) + 0 (empty string zig-zag length)
        XCTAssertEqual(msgData, Data([0, 0]), "ping message encoding mismatch")

        let (reqHeader, params) = try server.readRequest(header: handshake, from: msgData) as (RequestHeader, [EmptyMessage])
        XCTAssertNil(reqHeader.meta)
        XCTAssertEqual(reqHeader.name, "")
        XCTAssertEqual(params.count, 0)
    }

    // MARK: - Normal request / response

    func testRequestResponse_normalOK() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash, serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context,  clientHash: fix.serverHash, clientProtocol: fix.supportProtocol)

        let requestData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash, clientProtocol: fix.supportProtocol, serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try server.resolveHandshakeRequest(from: requestData)

        // --- Encode request ---
        let greeting = Greeting(message: "requestData")
        let msgData  = try client.writeRequest(messageName: "hello", parameters: [greeting])

        var expectedRequest = Data()
        expectedRequest.append(contentsOf: [0])          // null meta
        expectedRequest.append(contentsOf: [10])         // "hello" zig-zag length = 5*2 = 10
        expectedRequest.append("hello".data(using: .utf8)!)
        expectedRequest.append(contentsOf: [22])         // "requestData" zig-zag length = 11*2 = 22
        expectedRequest.append("requestData".data(using: .utf8)!)
        XCTAssertEqual(msgData, expectedRequest, "request encoding mismatch")

        // --- Server reads request ---
        let (reqHeader, requests) = try server.readRequest(header: handshake, from: msgData) as (RequestHeader, [Greeting])
        XCTAssertNil(reqHeader.meta)
        XCTAssertEqual(reqHeader.name,          "hello")
        XCTAssertEqual(requests.count,          1)
        XCTAssertEqual(requests[0].message,     "requestData")

        // --- Server writes response ---
        let responseGreeting = Greeting(message: "responseData")
        let resData = try server.writeResponse(header: handshake, messageName: reqHeader.name, parameter: responseGreeting)

        var expectedResponse = Data()
        expectedResponse.append(contentsOf: [0])         // null meta
        expectedResponse.append(contentsOf: [0])         // false flag (no error)
        expectedResponse.append(contentsOf: [24])        // "responseData" zig-zag = 12*2 = 24
        expectedResponse.append("responseData".data(using: .utf8)!)
        XCTAssertEqual(resData, expectedResponse, "response encoding mismatch")

        // --- Client reads response ---
        let (resHeader, responses) = try client.readResponse(
            header: handshake, messageName: "hello", from: resData
        ) as (ResponseHeader, [Greeting])
        XCTAssertNil(resHeader.meta)
        XCTAssertFalse(resHeader.flag, "flag should be false for normal response")
        XCTAssertEqual(responses.count,       1)
        XCTAssertEqual(responses[0].message,  "responseData")
    }

    // MARK: - Error response

    func testRequestResponse_errorResponse() throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash, serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context,  clientHash: fix.serverHash, clientProtocol: fix.supportProtocol)

        let requestData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash, clientProtocol: fix.supportProtocol, serverHash: fix.serverHash)
        )
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try server.resolveHandshakeRequest(from: requestData)

        let curse   = Curse(message: "responseError")
        let resData = try server.writeErrorResponse(header: handshake, messageName: "hello", errors: ["Curse": curse])

        var expectedError = Data()
        expectedError.append(contentsOf: [0])    // null meta
        expectedError.append(contentsOf: [1])    // true flag (error)
        expectedError.append(contentsOf: [2])    // union index 1 → zig-zag 2
        expectedError.append(contentsOf: [26])   // "responseError" zig-zag = 13*2 = 26
        expectedError.append("responseError".data(using: .utf8)!)
        XCTAssertEqual(resData, expectedError, "error response encoding mismatch")

        let (resHeader, errors) = try client.readResponse(
            header: handshake, messageName: "hello", from: resData
        ) as (ResponseHeader, [Curse])
        XCTAssertNil(resHeader.meta)
        XCTAssertTrue(resHeader.flag, "flag should be true for error response")
        XCTAssertEqual(errors.count,       1)
        XCTAssertEqual(errors[0].message,  "responseError")
    }

    // MARK: - Session management

    func testSessionCache_addAndRemove() throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash, clientProtocol: fix.supportProtocol)

        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        XCTAssertNotNil(client.sessionCache[fix.serverHash], "session should be cached after addSession")

        client.removeSession(for: fix.serverHash)
        XCTAssertNil(client.sessionCache[fix.serverHash], "session should be removed")
    }

    func testSessionCache_clearAll() throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash, clientProtocol: fix.supportProtocol)

        try client.addSession(hash: fix.clientHash, protocolString: fix.supportProtocol)
        try client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        client.clearSessions()
        XCTAssertTrue(client.sessionCache.isEmpty, "all sessions should be cleared")
    }

    func testSessionCache_invalidProtocolString_throws() throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash, clientProtocol: fix.supportProtocol)

        XCTAssertThrowsError(
            try client.addSession(hash: fix.serverHash, protocolString: "{invalid json}"),
            "adding an invalid protocol string should throw"
        )
    }

    // MARK: - Framing

    func testFraming() {
        let frameLen: Int32 = 4
        struct Case {
            let name:            String
            let input:           [UInt8]
            let expectFramed:    Data
            let expectDeframed:  [Data]
        }
        let cases: [Case] = [
            Case(name: "empty",
                 input: [],
                 expectFramed: Data([0,0,0,0]),
                 expectDeframed: []),
            Case(name: "less than frameLen",
                 input: [1,2,3],
                 expectFramed: Data([0,0,0,3, 1,2,3, 0,0,0,0]),
                 expectDeframed: [Data([1,2,3])]),
            Case(name: "equal to frameLen",
                 input: [1,2,3,4],
                 expectFramed: Data([0,0,0,4, 1,2,3,4, 0,0,0,0]),
                 expectDeframed: [Data([1,2,3,4])]),
            Case(name: "2 frames (split)",
                 input: [1,2,3,4,5],
                 expectFramed: Data([0,0,0,4, 1,2,3,4, 0,0,0,1, 5, 0,0,0,0]),
                 expectDeframed: [Data([1,2,3,4]), Data([5])]),
            Case(name: "2 full frames",
                 input: [1,2,3,4,5,6,7,8],
                 expectFramed: Data([0,0,0,4, 1,2,3,4, 0,0,0,4, 5,6,7,8, 0,0,0,0]),
                 expectDeframed: [Data([1,2,3,4]), Data([5,6,7,8])]),
            Case(name: "3 frames",
                 input: [1,2,3,4,5,6,7,8,9,10],
                 expectFramed: Data([0,0,0,4, 1,2,3,4, 0,0,0,4, 5,6,7,8, 0,0,0,2, 9,10, 0,0,0,0]),
                 expectDeframed: [Data([1,2,3,4]), Data([5,6,7,8]), Data([9,10])]),
        ]
        for c in cases {
            var data = Data(c.input)
            data.framing(frameLength: frameLen)
            XCTAssertEqual(data, c.expectFramed,    "\(c.name): framing mismatch")
            let deframed = data.deFraming()
            XCTAssertEqual(deframed, c.expectDeframed, "\(c.name): deframing mismatch")
        }
    }

    func testFraming_roundTrip() {
        // Property: framing then deframing recovers the original bytes in every frame.
        let payload = Data((0..<100).map { UInt8($0 % 256) })
        for frameLen: Int32 in [1, 4, 16, 64, 256] {
            var framed = payload
            framed.framing(frameLength: frameLen)
            let recovered = framed.deFraming().reduce(Data(), +)
            XCTAssertEqual(recovered, payload, "round-trip failed for frameLen=\(frameLen)")
        }
    }

    // MARK: - Context initialisation

    func testContext_schemasInitialised() {
        let ctx = Context(requestMeta: [:], responseMeta: [:])
        // Ensure schemas were decoded without crashing — getName() is a basic sanity check.
        XCTAssertEqual(ctx.requestSchema.getName(),  "HandshakeRequest")
        XCTAssertEqual(ctx.responseSchema.getName(), "HandshakeResponse")
    }

    // MARK: - Performance

    func testHandshakePerformance() throws {
        let fix = Fixture()
        measure {
            let server = try? MessageResponse(context: fix.context, serverHash: fix.serverHash, serverProtocol: fix.supportProtocol)
            let client = try? MessageRequest(context: fix.context,  clientHash: fix.clientHash, clientProtocol: fix.supportProtocol)
            guard let server, let client else { return }
            let req = try? client.initHandshakeRequest()
            guard let req else { return }
            _ = try? server.resolveHandshakeRequest(from: req)
        }
    }
}
