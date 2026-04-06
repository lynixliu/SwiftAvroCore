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
        let context = AvroIPCContext(requestMeta: [:], responseMeta: [:])
    }

    private struct Greeting: Codable, Equatable { var message: String }
    private struct Curse:    Codable, Equatable { var message: String }

    // MARK: - Context

    @Test("AvroIPCContext schema names are correct")
    func contextSchemas() {
        let ctx = AvroIPCContext(requestMeta: [:], responseMeta: [:])
        #expect(ctx.requestSchema.getName()  == "HandshakeRequest")
        #expect(ctx.responseSchema.getName() == "HandshakeResponse")
        #expect(ctx.metaSchema.getTypeName() == "map")
    }

    // MARK: - Handshake flows

    @Test("Full handshake flow: NONE then BOTH")
    func handshakeNoneThenBoth() async throws {
        let fix         = Fixture()
        let serverCache = ServerSessionCache()
        let clientCache = ClientSessionCache()
        let server      = AvroIPCResponse(serverHash: fix.serverHash,
                                          serverProtocol: fix.supportProtocol)
        let client      = AvroIPCRequest(clientHash: fix.clientHash,
                                         clientProtocol: fix.supportProtocol)

        let initialReq = try client.encodeInitialHandshake(context: fix.context)

        let (_, noneData) = try await server.resolveHandshake(
            from: initialReq, cache: serverCache, context: fix.context
        )
        let (noneResp, nonePayload) = try client.decodeHandshakeResponse(
            from: noneData, context: fix.context
        )
        #expect(noneResp.match          == .NONE)
        #expect(noneResp.serverHash     == fix.serverHash)
        #expect(noneResp.serverProtocol == fix.supportProtocol)
        #expect(noneResp.meta           == nil)
        #expect(nonePayload             == Data())

        let followUp = try await client.resolveHandshakeResponse(
            noneResp, cache: clientCache, context: fix.context
        )
        let retryReq = try #require(followUp)

        let (_, bothData) = try await server.resolveHandshake(
            from: retryReq, cache: serverCache, context: fix.context
        )
        let (bothResp, bothPayload) = try client.decodeHandshakeResponse(
            from: bothData, context: fix.context
        )
        #expect(bothResp.match          == .BOTH)
        #expect(bothResp.serverProtocol == nil)
        #expect(bothResp.serverHash     == nil)
        #expect(bothPayload             == Data())
    }

    @Test("Handshake CLIENT match flow")
    func handshakeClientMatch() async throws {
        let fix         = Fixture()
        let serverCache = ServerSessionCache()
        let clientCache = ClientSessionCache()
        let server      = AvroIPCResponse(serverHash: fix.serverHash,
                                          serverProtocol: fix.supportProtocol)
        let client      = AvroIPCRequest(clientHash: fix.clientHash,
                                         clientProtocol: fix.supportProtocol)

        try await serverCache.add(hash: fix.clientHash, protocolString: fix.supportProtocol)

        let initialReq = try client.encodeInitialHandshake(context: fix.context)
        let (_, responseData) = try await server.resolveHandshake(
            from: initialReq, cache: serverCache, context: fix.context
        )
        let (response, _) = try client.decodeHandshakeResponse(
            from: responseData, context: fix.context
        )

        #expect(response.match          == .CLIENT)
        #expect(response.serverHash     == fix.serverHash)
        #expect(response.serverProtocol == fix.supportProtocol)
        #expect(response.meta           == nil)

        let followUp = try await client.resolveHandshakeResponse(
            response, cache: clientCache, context: fix.context
        )
        #expect(followUp == nil)
    }

    @Test("Known client with correct server hash resolves to BOTH directly")
    func handshakeKnownClientBoth() async throws {
        let fix         = Fixture()
        let serverCache = ServerSessionCache()
        let clientCache = ClientSessionCache()
        let server      = AvroIPCResponse(serverHash: fix.serverHash,
                                          serverProtocol: fix.supportProtocol)
        let client      = AvroIPCRequest(clientHash: fix.serverHash,
                                         clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshake(
            serverHash: fix.serverHash, context: fix.context
        )
        try await clientCache.add(hash: fix.serverHash, protocolString: fix.supportProtocol)

        let (_, respData) = try await server.resolveHandshake(
            from: reqData, cache: serverCache, context: fix.context
        )
        let (resp, _) = try client.decodeHandshakeResponse(
            from: respData, context: fix.context
        )
        #expect(resp.match          == .BOTH)
        #expect(resp.serverHash     == nil)
        #expect(resp.serverProtocol == nil)
    }

    // MARK: - Ping

    @Test("Ping with empty message name encodes as two zero bytes")
    func requestPingEmptyMessageName() async throws {
        let fix         = Fixture()
        let serverCache = ServerSessionCache()
        let clientCache = ClientSessionCache()
        let server      = AvroIPCResponse(serverHash: fix.serverHash,
                                          serverProtocol: fix.supportProtocol)
        let client      = AvroIPCRequest(clientHash: fix.serverHash,
                                         clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshake(
            serverHash: fix.serverHash, context: fix.context
        )
        try await clientCache.add(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try await server.resolveHandshake(
            from: reqData, cache: serverCache, context: fix.context
        )

        struct EmptyMessage: Codable {}
        let msgData = try await client.encodeCall(
            messageName: "",
            parameters: [EmptyMessage()],
            serverHash: fix.serverHash,
            cache: clientCache,
            context: fix.context
        )
        #expect(msgData == Data([0, 0]))

        let (header, params): (RequestHeader, [EmptyMessage]) = try await server.decodeCall(
            header: handshake, from: msgData, cache: serverCache, context: fix.context
        )
        #expect(header.meta  == nil)
        #expect(header.name  == "")
        #expect(params.count == 0)
    }

    // MARK: - Normal round-trip

    @Test("Normal request/response round-trip")
    func requestResponseNormalOK() async throws {
        let fix         = Fixture()
        let serverCache = ServerSessionCache()
        let clientCache = ClientSessionCache()
        let server      = AvroIPCResponse(serverHash: fix.serverHash,
                                          serverProtocol: fix.supportProtocol)
        let client      = AvroIPCRequest(clientHash: fix.clientHash,
                                         clientProtocol: fix.supportProtocol)

        // Step 1: client sends initial handshake (no clientProtocol)
        let initialReq = try client.encodeInitialHandshake(context: fix.context)

        // Step 2: server responds with NONE — it doesn't know this client yet
        let (_, noneData) = try await server.resolveHandshake(
            from: initialReq, cache: serverCache, context: fix.context
        )
        let (noneResp, _) = try client.decodeHandshakeResponse(
            from: noneData, context: fix.context
        )
        #expect(noneResp.match == .NONE)

        // Step 3: client retries with full clientProtocol
        let retryReq = try await client.resolveHandshakeResponse(
            noneResp, cache: clientCache, context: fix.context
        )
        let fullReq = try #require(retryReq)

        // Step 4: server now registers the client and responds BOTH
        let (handshake, bothData) = try await server.resolveHandshake(
            from: fullReq, cache: serverCache, context: fix.context
        )
        let (bothResp, _) = try client.decodeHandshakeResponse(
            from: bothData, context: fix.context
        )
        #expect(bothResp.match == .BOTH)

        // At this point:
        // - serverCache has clientHash -> protocol (registered in step 4)
        // - clientCache has serverHash -> protocol (registered in step 3 via CLIENT/NONE resolution)
        // But clientCache was populated via resolveHandshakeResponse only on CLIENT match.
        // On BOTH match the client already knew the server — so manually seed clientCache:
        try await clientCache.add(hash: fix.serverHash, protocolString: fix.supportProtocol)

        // Step 5: encode and verify the call
        let msgData = try await client.encodeCall(
            messageName: "hello",
            parameters: [Greeting(message: "requestData")],
            serverHash: fix.serverHash,
            cache: clientCache,
            context: fix.context
        )
        var expectedReq = Data([0])           // empty meta
        expectedReq.append(contentsOf: [10])  // "hello" length (5 << 1 = 10)
        expectedReq.append("hello".data(using: .utf8)!)
        expectedReq.append(contentsOf: [22])  // "requestData" length (11 << 1 = 22)
        expectedReq.append("requestData".data(using: .utf8)!)
        #expect(msgData == expectedReq)

        // Step 6: server encodes response
        let resData = try await server.encodeResponse(
            header: handshake,
            messageName: "hello",
            parameter: Greeting(message: "responseData"),
            cache: serverCache,
            context: fix.context
        )
        var expectedRes = Data([0, 0])
        expectedRes.append(contentsOf: [24])
        expectedRes.append("responseData".data(using: .utf8)!)
        #expect(resData == expectedRes)

        // Step 7: client decodes response
        let response: Greeting = try await client.decodeResponse(
            messageName: "hello",
            from: resData,
            serverHash: fix.serverHash,
            cache: clientCache,
            context: fix.context
        )
        #expect(response.message == "responseData")
    }

    // MARK: - Error response

    @Test("Error response round-trip")
    func requestResponseError() async throws {
        let fix         = Fixture()
        let serverCache = ServerSessionCache()
        let clientCache = ClientSessionCache()
        let server      = AvroIPCResponse(serverHash: fix.serverHash,
                                          serverProtocol: fix.supportProtocol)
        let client      = AvroIPCRequest(clientHash: fix.serverHash,
                                         clientProtocol: fix.supportProtocol)

        let reqData = try client.encodeHandshake(
            serverHash: fix.serverHash, context: fix.context
        )
        try await clientCache.add(hash: fix.serverHash, protocolString: fix.supportProtocol)
        let (handshake, _) = try await server.resolveHandshake(
            from: reqData, cache: serverCache, context: fix.context
        )

        let resData = try await server.encodeErrorResponse(
            header: handshake,
            messageName: "hello",
            errors: ["Curse": Curse(message: "responseError")],
            cache: serverCache,
            context: fix.context
        )
        var expected = Data([0, 1, 2])
        expected.append(contentsOf: [26])
        expected.append("responseError".data(using: .utf8)!)
        #expect(resData == expected)
    }

    // MARK: - decodeFromContinue

    @Test("decodeFromContinue consumes exact byte count for simple request")
    func decodeFromContinueHandshakeRequest() throws {
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0,
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0
        ])
        let avro   = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.requestSchema))
        let (model, consumed): (HandshakeRequest, Int) = try avro.decodeFromContinue(
            from: raw, schema: schema
        )
        #expect(consumed             == raw.count)
        #expect(model.clientProtocol == nil)
        #expect(model.meta           == nil)
    }

    @Test("decodeFromContinue stops at message boundary ignoring trailing garbage")
    func decodeFromContinuePartialBuffer() throws {
        let avro       = Avro()
        let respSchema = try #require(avro.newSchema(schema: MessageConstant.responseSchema))
        let encoded    = try AvroEncoder().encode(
            HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil),
            schema: respSchema
        )
        let combined = encoded + Data([0xFF, 0xFF, 0xFF])
        let (decoded, consumed): (HandshakeResponse, Int) =
            try avro.decodeFromContinue(from: combined, schema: respSchema)
        #expect(decoded.match == .BOTH)
        #expect(consumed      == encoded.count)
    }

    // MARK: - encodeFrom / decodeFrom round-trip

    @Test("encodeFrom/decodeFrom round-trip for HandshakeResponse")
    func encodeDecode_roundTrip() throws {
        let avro     = Avro()
        let schema   = try #require(avro.newSchema(schema: MessageConstant.responseSchema))
        let original = HandshakeResponse(
            match: .CLIENT,
            serverProtocol: "test",
            serverHash: Array(repeating: 0xAB, count: 16)
        )
        let encoded: Data              = try avro.encodeFrom(original, schema: schema)
        let decoded: HandshakeResponse = try avro.decodeFrom(from: encoded, schema: schema)
        #expect(decoded.match          == original.match)
        #expect(decoded.serverProtocol == original.serverProtocol)
        #expect(decoded.serverHash     == original.serverHash)
    }

    // MARK: - Session management

    @Test("Client session add and remove")
    func sessionCacheAddAndRemove() async throws {
        let fix   = Fixture()
        let cache = ClientSessionCache()
        try await cache.add(hash: fix.serverHash, protocolString: fix.supportProtocol)
        #expect(await cache.avroProtocol(for: fix.serverHash) != nil)
        await cache.remove(for: fix.serverHash)
        #expect(await cache.avroProtocol(for: fix.serverHash) == nil)
    }

    @Test("Client clearSessions empties cache")
    func sessionCacheClearAll() async throws {
        let fix   = Fixture()
        let cache = ClientSessionCache()
        try await cache.add(hash: fix.clientHash, protocolString: fix.supportProtocol)
        try await cache.add(hash: fix.serverHash, protocolString: fix.supportProtocol)
        await cache.clear()
        #expect(await cache.avroProtocol(for: fix.clientHash) == nil)
        #expect(await cache.avroProtocol(for: fix.serverHash) == nil)
    }

    @Test("Adding session with invalid JSON throws")
    func sessionCacheInvalidJSONThrows() async throws {
        let fix   = Fixture()
        let cache = ClientSessionCache()
        await #expect(throws: (any Error).self) {
            try await cache.add(hash: fix.serverHash, protocolString: "{not valid json}")
        }
    }

    @Test("Server session add and remove")
    func serverSessionCacheAddAndRemove() async throws {
        let fix   = Fixture()
        let cache = ServerSessionCache()
        try await cache.add(hash: fix.clientHash, protocolString: fix.supportProtocol)
        #expect(await cache.contains(hash: fix.clientHash))
        await cache.remove(for: fix.clientHash)
        #expect(await cache.contains(hash: fix.clientHash) == false)
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
}
