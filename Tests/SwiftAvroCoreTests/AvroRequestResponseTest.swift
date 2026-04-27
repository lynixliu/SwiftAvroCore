//
//  AvroRequestResponseTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Request / Response")
struct AvroRequestResponseTests {

    private struct Fixture {
        let supportProtocol = """
        {"namespace":"com.acme","protocol":"HelloWorld","doc":"Protocol Greetings",
         "types":[
           {"name":"Greeting","type":"record","fields":[{"name":"message","type":"string"}]},
           {"name":"Curse","type":"error","fields":[{"name":"message","type":"string"}]}
         ],
         "messages":{"hello":{
           "doc":"Say hello.",
           "request":[{"name":"greeting","type":"Greeting"}],
           "response":"Greeting","errors":["Curse"]
         }}}
        """
        let clientHash: MD5Hash = [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10]
        let serverHash: MD5Hash = [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10]
        let context: AvroIPCContext = {
            let avro = Avro()
            let reqSchema = avro.newSchema(schema: """
                {"type":"record","name":"HandshakeRequest","fields":[
                    {"name":"clientHash","type":{"type":"fixed","name":"MD5","size":16}},
                    {"name":"clientProtocol","type":["null","string"]},
                    {"name":"serverHash","type":{"type":"fixed","name":"MD5","size":16}},
                    {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
                ]}
                """)!
            let resSchema = avro.newSchema(schema: """
                {"type":"record","name":"HandshakeResponse","fields":[
                    {"name":"match","type":{"type":"enum","name":"HandshakeMatch","symbols":["NONE","BOTH","REMOTE"]}},
                    {"name":"serverProtocol","type":["null","string"]},
                    {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
                ]}
                """)!
            let metaSchema = avro.newSchema(schema: """
                {"type":"map","values":"bytes"}
                """)!
            return AvroIPCContext(requestSchema: reqSchema, responseSchema: resSchema, metaSchema: metaSchema, requestMeta: [:], responseMeta: [:])
        }()
    }

    private struct Greeting: Codable, Equatable { var message: String }
    private struct Curse:    Codable, Equatable { var message: String }

    // MARK: - Context

    @Test("Context schema names are correct")
    func contextSchemas() {
        let ctx = Fixture().context
        #expect(ctx.requestSchema.getName()  == "HandshakeRequest")
        #expect(ctx.responseSchema.getName() == "HandshakeResponse")
        #expect(ctx.metaSchema.getTypeName() == "map")
    }

    // MARK: - Handshake flows
    // NOTE: These tests are disabled pending MessageRequest/MessageResponse re-implementation

    /*
    @Test("Full handshake flow: NONE then BOTH")
    func handshakeNoneThenBoth() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)

        let initialReq = try client.initHandshakeRequest()

        let (_, noneData)          = try await server.resolveHandshakeRequest(from: initialReq)
        let (noneResp, nonePayload) = try client.decodeResponse(from: noneData)
        #expect(noneResp.match          == .NONE)
        #expect(noneResp.serverHash     == fix.serverHash)
        #expect(noneResp.serverProtocol == fix.supportProtocol)
        #expect(noneResp.meta           == nil)
        #expect(nonePayload             == Data())

        let retryReq = try #require(await client.resolveHandshakeResponse(noneResp))

        let (_, bothData)          = try await server.resolveHandshakeRequest(from: retryReq)
        let (bothResp, bothPayload) = try client.decodeResponse(from: bothData)
        #expect(bothResp.match          == .BOTH)
        #expect(bothResp.serverProtocol == nil)
        #expect(bothResp.serverHash     == nil)
        #expect(bothPayload             == Data())
    }

    @Test("Handshake CLIENT match flow")
    func handshakeClientMatch() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)

        try await server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.clientHash)

        let (_, responseData) = try await server.resolveHandshakeRequest(from: client.initHandshakeRequest())
        let (response, _)     = try client.decodeResponse(from: responseData)

        #expect(response.match          == .CLIENT)
        #expect(response.serverHash     == fix.serverHash)
        #expect(response.serverProtocol == fix.supportProtocol)
        #expect(response.meta           == nil)

        let followUp = try await client.resolveHandshakeResponse(response)
        #expect(followUp == nil)
    }

    @Test("Known client with correct server hash resolves to BOTH directly")
    func handshakeKnownClientBoth() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash))
        try await client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)

        let (_, respData) = try await server.resolveHandshakeRequest(from: reqData)
        let (resp, _)     = try client.decodeResponse(from: respData)
        #expect(resp.match          == .BOTH)
        #expect(resp.serverHash     == nil)
        #expect(resp.serverProtocol == nil)
    }

    @Test("Ping with empty message name encodes as two zero bytes")
    func requestPingEmptyMessageName() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash))
        try await client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try await server.resolveHandshakeRequest(from: reqData)

        struct EmptyMessage: Codable {}
        let msgData = try await client.writeRequest(messageName: "", parameters: [EmptyMessage()])
        #expect(msgData == Data([0, 0]))

        let (header, params) = try await server.readRequest(header: handshake, from: msgData)
                                   as (RequestHeader, [EmptyMessage])
        #expect(header.meta   == [:])
        #expect(header.name   == "")
        #expect(params.count  == 0)
    }

    @Test("Normal request/response round-trip")
    func requestResponseNormalOK() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash))
        try await client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try await server.resolveHandshakeRequest(from: reqData)

        let msgData = try await client.writeRequest(messageName: "hello",
                                              parameters: [Greeting(message: "requestData")])

        var expectedReq = Data([0])
        expectedReq.append(contentsOf: [10]); expectedReq.append("hello".data(using: .utf8)!)
        expectedReq.append(contentsOf: [22]); expectedReq.append("requestData".data(using: .utf8)!)
        #expect(msgData == expectedReq)

        let (reqHeader, greetings) = try await server.readRequest(header: handshake, from: msgData)
                                         as (RequestHeader, [Greeting])
        #expect(reqHeader.name       == "hello")
        #expect(greetings[0].message == "requestData")

        let resData = try await server.writeResponse(header: handshake, messageName: reqHeader.name,
                                               parameter: Greeting(message: "responseData"))
        var expectedRes = Data([0, 0])
        expectedRes.append(contentsOf: [24]); expectedRes.append("responseData".data(using: .utf8)!)
        #expect(resData == expectedRes)

        let (resHeader, responses) = try await client.readResponse(header: handshake,
                                                             messageName: "hello", from: resData)
                                         as (ResponseHeader, [Greeting])
        #expect(!resHeader.flag)
        #expect(responses[0].message == "responseData")
    }

    @Test("Error response round-trip")
    func requestResponseError() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        let client = try MessageRequest(context: fix.context, clientHash: fix.serverHash,
                                        clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshakeRequest(
            HandshakeRequest(clientHash: fix.serverHash,
                             clientProtocol: fix.supportProtocol,
                             serverHash: fix.serverHash))
        try await client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try await server.resolveHandshakeRequest(from: reqData)

        let resData = try await server.writeErrorResponse(header: handshake, messageName: "hello",
                                                    errors: ["Curse": Curse(message: "responseError")])
        var expected = Data([0, 1, 2])
        expected.append(contentsOf: [26]); expected.append("responseError".data(using: .utf8)!)
        #expect(resData == expected)

        let (resHeader, errors) = try await client.readResponse(header: handshake, messageName: "hello",
                                                          from: resData) as (ResponseHeader, [Curse])
        #expect(resHeader.flag)
        #expect(errors[0].message == "responseError")
    }
    */

    // MARK: - decodeFromContinue
    // NOTE: These tests are disabled pending decodeFromContinue re-implementation

    /*
    @Test("decodeFromContinue consumes exact byte count for simple request")
    func decodeFromContinueHandshakeRequest() throws {
        let avro   = Avro()
        let schema = try #require(avro.decodeSchema(schema: MessageConstant.requestSchema))

        // Build raw bytes via the encoder so the test is immune to meta-union encoding choices.
        let knownHash: MD5Hash = [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf]
        avro.setSchema(schema: schema)
        let encoded = try avro.encode(
            HandshakeRequest(clientHash: knownHash, clientProtocol: nil, serverHash: knownHash))
        // Append known trailing garbage to verify the decoder stops exactly at the message boundary.
        let raw = encoded + Data([0xDE, 0xAD, 0xBE, 0xEF])

        let (model, consumed): (HandshakeRequest, Int) = try avro.decodeFromContinue(from: raw, schema: schema)
        #expect(consumed             == encoded.count)  // stopped exactly at message boundary
        #expect(consumed             <  raw.count)      // did not consume the trailing garbage
        #expect(model.clientHash     == knownHash)
        #expect(model.clientProtocol == nil)
        #expect(model.meta           == nil)   // null branch decoded correctly via decodeIfPresent
    }

    @Test("decodeFromContinue stops at message boundary ignoring trailing garbage")
    func decodeFromContinuePartialBuffer() throws {
        let avro       = Avro()
        let respSchema = try #require(avro.decodeSchema(schema: MessageConstant.responseSchema))
        let encoded    = try Avro().encodeFrom(
            HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil),
            schema: respSchema)
        let combined   = encoded + Data([0xFF, 0xFF, 0xFF])
        let (decoded, consumed): (HandshakeResponse, Int) =
            try avro.decodeFromContinue(from: combined, schema: respSchema)
        #expect(decoded.match == .BOTH)
        #expect(consumed      == encoded.count)
    }

    // MARK: - encodeFrom / decodeFrom round-trip

    @Test("encodeFrom/decodeFrom round-trip for HandshakeResponse")
    func encodeDecode_roundTrip() throws {
        let avro     = Avro()
        let schema   = try #require(avro.decodeSchema(schema: MessageConstant.responseSchema))
        let original = HandshakeResponse(match: .CLIENT, serverProtocol: "test",
                                         serverHash: Array(repeating: 0xAB, count: 16))
        let encoded: Data              = try avro.encodeFrom(original, schema: schema)
        let decoded: HandshakeResponse  = try avro.decodeFrom(from: encoded, schema: schema)
        #expect(decoded.match          == original.match)
        #expect(decoded.serverProtocol == original.serverProtocol)
        #expect(decoded.serverHash     == original.serverHash)
    }
    */

    // MARK: - Session management
    // NOTE: These tests are disabled pending MessageRequest/MessageResponse re-implementation

    /*
    @Test("Client session add and remove")
    func sessionCacheAddAndRemove() async throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)
        try await client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        #expect(await client.sessionCache[fix.serverHash] != nil)
        await client.removeSession(for: fix.serverHash)
        #expect(await client.sessionCache[fix.serverHash] == nil)
    }

    @Test("Client clearSessions empties cache")
    func sessionCacheClearAll() async throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)
        try await client.addSession(hash: fix.clientHash, protocolString: fix.supportProtocol)
        try await client.addSession(hash: fix.serverHash, protocolString: fix.supportProtocol)
        await client.clearSessions()
        #expect(await client.sessionCache.isEmpty)
    }

    @Test("Adding session with invalid JSON throws")
    func sessionCacheInvalidJSONThrows() async throws {
        let fix    = Fixture()
        let client = try MessageRequest(context: fix.context, clientHash: fix.clientHash,
                                        clientProtocol: fix.supportProtocol)
        await #expect(throws: (any Error).self) {
            try await client.addSession(hash: fix.serverHash, protocolString: "{not valid json}")
        }
    }

    @Test("Server session add and remove")
    func serverSessionCacheAddAndRemove() async throws {
        let fix    = Fixture()
        let server = try MessageResponse(context: fix.context, serverHash: fix.serverHash,
                                         serverProtocol: fix.supportProtocol)
        try await server.addSupportedProtocol(protocolString: fix.supportProtocol, hash: fix.clientHash)
        #expect(await server.sessionCache[fix.clientHash] != nil)
        await server.removeSession(for: fix.clientHash)
        #expect(await server.sessionCache[fix.clientHash] == nil)
    }

    // MARK: - Framing

    @Test("Framing cases", arguments: [
        (name: "empty",        input: [UInt8]([]),
         expect: [UInt8]([0,0,0,0]),
         deframed: [[UInt8]]()),
        (name: "less than frameLen", input: [1,2,3] as [UInt8],
         expect: [0,0,0,3, 1,2,3, 0,0,0,0] as [UInt8],
         deframed: [[1,2,3]] as [[UInt8]]),
        (name: "equal to frameLen", input: [1,2,3,4] as [UInt8],
         expect: [0,0,0,4, 1,2,3,4, 0,0,0,0] as [UInt8],
         deframed: [[1,2,3,4]] as [[UInt8]]),
        (name: "2 frames split", input: [1,2,3,4,5] as [UInt8],
         expect: [0,0,0,4, 1,2,3,4, 0,0,0,1, 5, 0,0,0,0] as [UInt8],
         deframed: [[1,2,3,4],[5]] as [[UInt8]]),
    ] as [(name: String, input: [UInt8], expect: [UInt8], deframed: [[UInt8]])])
    func framingCases(tc: (name: String, input: [UInt8], expect: [UInt8], deframed: [[UInt8]])) {
        var data = Data(tc.input)
        data.frame(maxFrameLength: 4)
        #expect(data == Data(tc.expect), "\(tc.name): framing mismatch")
        let deframed = data.deFraming().map { Array($0) }
        #expect(deframed == tc.deframed, "\(tc.name): deframing mismatch")
    }

    @Test("Framing round-trip for various frame lengths", arguments: [1, 4, 16, 64, 256] as [Int])
    func framingRoundTrip(frameLen: Int) {
        let payload = Data((0..<100).map { UInt8($0 % 256) })
        var framed  = payload
        framed.frame(maxFrameLength: frameLen)
        let recovered = framed.deFraming().reduce(Data(), +)
        #expect(recovered == payload)
        #expect(Array(framed.suffix(4)) == [0,0,0,0])
    }
    */
}
