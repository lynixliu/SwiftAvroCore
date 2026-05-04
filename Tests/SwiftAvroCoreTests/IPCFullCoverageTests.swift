//
//  IPCFullCoverageTests.swift
//  SwiftAvroCoreTests
//
//  Completes 100% code coverage for:
//    - Request.swift  (AvroIPCRequest)
//    - Response.swift (AvroIPCResponse)
//    - SessionCache.swift (SessionCache<Role>)
//    - AvroProtocol.swift (AvroProtocol, Message, RequestType)
//

import Testing
import Foundation
@testable import SwiftAvroCore

// MARK: - Shared fixtures

private let coverageProtocolJSON = """
{
  "namespace": "com.test", "protocol": "CoverageProtocol",
  "types": [
    {"name":"ReqType",  "type":"record","fields":[{"name":"value","type":"int"}]},
    {"name":"RespType", "type":"record","fields":[{"name":"result","type":"string"}]},
    {"name":"ErrType",  "type":"error", "fields":[{"name":"reason","type":"string"}]}
  ],
  "messages": {
    "echo": {
      "request":  [{"name":"input","type":"ReqType"}],
      "response": "RespType",
      "errors":   ["ErrType"],
      "one-way": true
    }
  }
}
"""

private let clientHash: MD5Hash = Array(repeating: 0x01, count: 16)
private let serverHash: MD5Hash = Array(repeating: 0x02, count: 16)

private struct ReqType: Codable, Equatable { var value: Int }
private struct RespType: Codable, Equatable { var result: String }
private struct ErrType: Codable, Equatable { var reason: String }

// MARK: - Helper to build a session

private func makeSession(knownProtocols: Set<String>? = nil) -> (
    context: AvroIPCContext, session: AvroIPCSession, avro: Avro
) {
    let avro = Avro()
    let reqSchema = avro.newSchema(schema: MessageConstant.requestSchema)!
    let respSchema = avro.newSchema(schema: MessageConstant.responseSchema)!
    let metaSchema = avro.newSchema(schema: MessageConstant.metadataSchema)!
    let ctx = AvroIPCContext(
        requestSchema: reqSchema, responseSchema: respSchema, metaSchema: metaSchema,
        requestMeta: [:], responseMeta: [:],
        knownProtocols: knownProtocols
    )
    return (ctx, AvroIPCSession(context: ctx), avro)
}

// MARK: - Helper to build a loaded session (both caches populated)

private func makeLoadedSession() async throws -> (
    avro: Avro, session: AvroIPCSession,
    client: AvroIPCRequest, server: AvroIPCResponse
) {
    let (_, session, avro) = makeSession()
    let client = try AvroIPCRequest(
        clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
    let server = AvroIPCResponse(
        serverHash: serverHash, serverProtocol: coverageProtocolJSON)

    // Perform full NONE -> BOTH handshake so both caches are populated
    let initialData = try client.encodeInitialHandshake(avro: avro, session: session)
    let (_, noneResp, _) = try await server.resolveHandshake(
        avro: avro, from: initialData, session: session)
    let noneRespDecoded = try client.decodeHandshakeResponse(
        avro: avro, from: noneResp, session: session).0
    _ = try await client.resolveHandshakeResponse(
        noneRespDecoded, avro: avro, session: session)

    let retryData = try client.encodeHandshake(
        avro: avro, serverHash: serverHash, session: session)
    let (_, bothResp, _) = try await server.resolveHandshake(
        avro: avro, from: retryData, session: session)
    let bothRespDecoded = try client.decodeHandshakeResponse(
        avro: avro, from: bothResp, session: session).0
    _ = try await client.resolveHandshakeResponse(
        bothRespDecoded, avro: avro, session: session)

    return (avro, session, client, server)
}

// ===========================================================================
// MARK: - AvroIPCRequest full coverage
// ===========================================================================

@Suite("AvroIPCRequest full coverage")
struct AvroIPCRequestFullCoverageTests {

    // MARK: init

    @Test("init stores fields; knownProtocols=nil always passes")
    func initHappyPath() throws {
        let (_, session, _) = makeSession(knownProtocols: nil)
        let req = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        #expect(req.clientHash == clientHash)
        #expect(req.clientProtocol == coverageProtocolJSON)
    }

    @Test("init: allowed protocol passes, unknown protocol throws")
    func initKnownProtocols() throws {
        let (_, session, _) = makeSession(knownProtocols: ["CoverageProtocol"])
        _ = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: "CoverageProtocol", session: session)
        #expect(throws: AvroHandshakeError.self) {
            _ = try AvroIPCRequest(
                clientHash: clientHash, clientProtocol: "Unknown", session: session)
        }
    }

    // MARK: encodeInitialHandshake / encodeHandshake

    @Test("encodeInitialHandshake sets clientProtocol=nil")
    func encodeInitialHandshakeNil() async throws {
        let (context, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)

        let initial = try client.encodeInitialHandshake(avro: avro, session: session)
        avro.setSchema(schema: context.requestSchema)
        let decoded: HandshakeRequest = try avro.decode(from: initial)
        #expect(decoded.clientProtocol == nil)
        #expect(decoded.clientHash == clientHash)
    }

    @Test("encodeHandshake sets full protocol")
    func encodeHandshakeFull() async throws {
        let (context, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)

        let retry = try client.encodeHandshake(
            avro: avro, serverHash: serverHash, session: session)
        avro.setSchema(schema: context.requestSchema)
        let decoded: HandshakeRequest = try avro.decode(from: retry)
        #expect(decoded.clientProtocol == coverageProtocolJSON)
        #expect(decoded.serverHash == serverHash)
    }

    // MARK: decodeHandshakeResponse

    @Test("decodeHandshakeResponse cleanly splits response bytes")
    func decodeHandshakeResponseSplit() async throws {
        let (context, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)

        let serverResp = HandshakeResponse(
            match: .NONE, serverProtocol: coverageProtocolJSON,
            serverHash: serverHash, meta: nil)
        avro.setSchema(schema: context.responseSchema)
        let payload = Data([0xDE, 0xAD, 0xBE, 0xEF])
        let combined = try avro.encode(serverResp) + payload

        let (decoded, remainder) = try client.decodeHandshakeResponse(
            avro: avro, from: combined, session: session)
        #expect(decoded.match == .NONE)
        #expect(remainder == payload)
    }

    // MARK: resolveHandshakeResponse

    @Test("resolveHandshakeResponse: BOTH -> nil")
    func resolveBoth() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let both = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil, meta: nil)
        #expect(try await client.resolveHandshakeResponse(both, avro: avro, session: session) == nil)
    }

    @Test("resolveHandshakeResponse: NONE with hash returns retry data")
    func resolveNoneWithHash() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let none = HandshakeResponse(
            match: .NONE, serverProtocol: coverageProtocolJSON,
            serverHash: serverHash, meta: nil)
        let retry = try await client.resolveHandshakeResponse(none, avro: avro, session: session)
        #expect(retry != nil && !retry!.isEmpty)
    }

    @Test("resolveHandshakeResponse: NONE missing hash throws")
    func resolveNoneNoHash() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let noneNoHash = HandshakeResponse(
            match: .NONE, serverProtocol: nil, serverHash: nil, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.resolveHandshakeResponse(noneNoHash, avro: avro, session: session)
        }
    }

    @Test("resolveHandshakeResponse: CLIENT populates cache")
    func resolveClient() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let full = HandshakeResponse(
            match: .CLIENT, serverProtocol: coverageProtocolJSON,
            serverHash: serverHash, meta: nil)
        #expect(try await client.resolveHandshakeResponse(full, avro: avro, session: session) == nil)
        let proto = await session.clientCache.avroProtocol(for: serverHash)
        #expect(proto?.name == "CoverageProtocol")
    }

    @Test("resolveHandshakeResponse: CLIENT missing hash throws")
    func resolveClientNoHash() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let noHash = HandshakeResponse(
            match: .CLIENT, serverProtocol: coverageProtocolJSON,
            serverHash: nil, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.resolveHandshakeResponse(noHash, avro: avro, session: session)
        }
    }

    @Test("resolveHandshakeResponse: CLIENT missing protocol throws")
    func resolveClientNoProtocol() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let noProto = HandshakeResponse(
            match: .CLIENT, serverProtocol: nil,
            serverHash: serverHash, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.resolveHandshakeResponse(noProto, avro: avro, session: session)
        }
    }

    // MARK: encodeCall

    @Test("encodeCall with parameters produces non-empty data")
    func encodeCallWithParams() async throws {
        let (avro, session, client, _) = try await makeLoadedSession()

        let callData = try await client.encodeCall(
            avro: avro, messageName: "echo",
            parameters: [ReqType(value: 42)],
            serverHash: serverHash, session: session)

        #expect(!callData.isEmpty)
    }

    @Test("encodeCall ping produces two zero bytes")
    func encodeCallPing() async throws {
        let (avro, session, client, _) = try await makeLoadedSession()

        struct Empty: Codable {}

        let callData = try await client.encodeCall(
            avro: avro, messageName: "",
            parameters: [Empty()],
            serverHash: serverHash, session: session)
        #expect(callData == Data([0x00, 0x00]))
    }

    @Test("encodeCall throws missingSchema when serverHash absent")
    func encodeCallMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.encodeCall(
                avro: avro, messageName: "echo",
                parameters: [ReqType(value: 1)],
                serverHash: serverHash, session: session)
        }
    }

    // MARK: decodeResponse

    @Test("decodeResponse with success flag=false returns correct response")
    func decodeResponseSuccess() async throws {
        let (avro, session, client, server) = try await makeLoadedSession()

        let handshakeReq = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        let responseData = try await server.encodeResponse(
            avro: avro, header: handshakeReq,
            messageName: "echo",
            parameter: RespType(result: "hello"),
            session: session)

        let (header, responses): (ResponseHeader, [RespType]) =
            try await client.decodeResponse(
                avro: avro, messageName: "echo",
                from: responseData, serverHash: serverHash, session: session)

        #expect(header.flag == false)
        #expect(responses.count == 1)
        #expect(responses[0].result == "hello")
    }

    @Test("decodeResponse with error flag=true returns error objects")
    func decodeResponseError() async throws {
        let (avro, session, client, server) = try await makeLoadedSession()

        let handshakeReq = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        let responseData = try await server.encodeErrorResponse(
            avro: avro, header: handshakeReq,
            messageName: "echo",
            errors: ["ErrType": ErrType(reason: "something broke")],
            session: session)

        let (header, errors): (ResponseHeader, [ErrType]) =
            try await client.decodeResponse(
                avro: avro, messageName: "echo",
                from: responseData, serverHash: serverHash, session: session)

        #expect(header.flag == true)
        #expect(errors.count > 0)
        #expect(errors[0].reason == "something broke")
    }

    @Test("decodeResponse throws missingSchema when serverHash absent")
    func decodeResponseMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)
        let raw = Data([0x00, 0x00, 0x06]) + "foo".data(using: .utf8)!
        await #expect(throws: AvroHandshakeError.self) {
            let _: (ResponseHeader, [RespType]) =
                try await client.decodeResponse(
                    avro: avro, messageName: "echo",
                    from: raw, serverHash: serverHash,
                    session: session)
        }
    }
}

// ===========================================================================
// MARK: - AvroIPCResponse full coverage
// ===========================================================================

@Suite("AvroIPCResponse full coverage")
struct AvroIPCResponseFullCoverageTests {

    // MARK: init

    @Test("init stores serverHash and serverProtocol")
    func initStoresFields() {
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        #expect(server.serverHash == serverHash)
        #expect(server.serverProtocol == coverageProtocolJSON)
    }

    // MARK: resolveHandshake

    @Test("resolveHandshake NONE: unknown client without clientProtocol")
    func resolveHandshakeNone() async throws {
        let (context, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        let client = try AvroIPCRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON, session: session)

        let data = try client.encodeInitialHandshake(avro: avro, session: session)
        let (_, responseData, _) = try await server.resolveHandshake(
            avro: avro, from: data, session: session)

        avro.setSchema(schema: context.responseSchema)
        let response: HandshakeResponse = try avro.decode(from: responseData)
        #expect(response.match == .NONE)
        #expect(response.serverHash == serverHash)
        #expect(response.serverProtocol == coverageProtocolJSON)
    }

    @Test("resolveHandshake CLIENT: known client with wrong serverHash")
    func resolveHandshakeClientWrongHash() async throws {
        let (context, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        try await session.serverCache.add(hash: clientHash, protocolString: coverageProtocolJSON)

        let reqSchema = avro.newSchema(schema: MessageConstant.requestSchema)!
        let req = HandshakeRequest(
            clientHash: clientHash, clientProtocol: coverageProtocolJSON,
            serverHash: clientHash, meta: [:])
        avro.setSchema(schema: reqSchema)
        let data = try avro.encode(req)

        let (_, responseData, _) = try await server.resolveHandshake(
            avro: avro, from: data, session: session)
        avro.setSchema(schema: context.responseSchema)
        let response: HandshakeResponse = try avro.decode(from: responseData)
        #expect(response.match == .CLIENT)
        #expect(response.serverHash == serverHash)
        #expect(response.serverProtocol == coverageProtocolJSON)
    }

    @Test("resolveHandshake throws invalidClientHashLength for short clientHash")
    func resolveHandshakeInvalidHashLength() async throws {
        let (_, _, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)

        let shortHashSchema = avro.newSchema(schema: """
            {"type":"record","name":"HandshakeRequest","fields":[
                {"name":"clientHash","type":{"type":"fixed","name":"MD4","size":4}},
                {"name":"clientProtocol","type":["null","string"]},
                {"name":"serverHash","type":{"type":"fixed","name":"MD4","size":4}},
                {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
            ]}
        """)!
        let (_, respSchema, metaSchema) = makeSchemas(avro: avro)
        let shortSession = AvroIPCSession(context: AvroIPCContext(
            requestSchema: shortHashSchema, responseSchema: respSchema, metaSchema: metaSchema))

        let shortHash: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        avro.setSchema(schema: shortHashSchema)
        let data = try avro.encode(HandshakeRequest(
            clientHash: shortHash, clientProtocol: nil,
            serverHash: shortHash, meta: [:]))

        await #expect(throws: AvroHandshakeError.self) {
            _ = try await server.resolveHandshake(
                avro: avro, from: data, session: shortSession)
        }
    }

    // MARK: decodeCall

    @Test("decodeCall with parameters decodes correctly")
    func decodeCallWithParams() async throws {
        let (avro, session, client, server) = try await makeLoadedSession()

        let callData = try await client.encodeCall(
            avro: avro, messageName: "echo",
            parameters: [ReqType(value: 99)],
            serverHash: serverHash, session: session)

        let handshakeReq = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        let (header, params): (RequestHeader, [ReqType]) =
            try await server.decodeCall(
                avro: avro, header: handshakeReq,
                from: callData, session: session)

        #expect(header.name == "echo")
        #expect(params.count == 1)
        #expect(params[0].value == 99)
    }

    @Test("decodeCall ping returns empty parameters")
    func decodeCallPing() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        try await session.serverCache.add(hash: clientHash, protocolString: coverageProtocolJSON)

        var data = Data()
        data.append(try avro.encodeFrom([String: [UInt8]](), schema: session.context.metaSchema))
        data.append(try avro.encodeFrom("", schema: AvroSchema(type: "string")))

        let header = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        let (h, p): (RequestHeader, [ReqType]) =
            try await server.decodeCall(avro: avro, header: header, from: data, session: session)
        #expect(h.name == "")
        #expect(p.isEmpty)
    }

    @Test("decodeCall throws for unknown message name")
    func decodeCallUnknownMessage() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        try await session.serverCache.add(hash: clientHash, protocolString: coverageProtocolJSON)

        var data = Data()
        data.append(try avro.encodeFrom([String: [UInt8]](), schema: session.context.metaSchema))
        data.append(try avro.encodeFrom("bogus", schema: AvroSchema(type: "string")))

        let header = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        await #expect(throws: AvroHandshakeError.self) {
            let _: (RequestHeader, [ReqType]) =
                try await server.decodeCall(avro: avro, header: header, from: data, session: session)
        }
    }

    // MARK: encodeResponse

    @Test("encodeResponse produces valid data")
    func encodeResponseHappyPath() async throws {
        let (avro, session, client, server) = try await makeLoadedSession()

        let handshakeReq = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        let data = try await server.encodeResponse(
            avro: avro, header: handshakeReq,
            messageName: "echo",
            parameter: RespType(result: "success"),
            session: session)

        #expect(!data.isEmpty)

        let (header, responses): (ResponseHeader, [RespType]) =
            try await client.decodeResponse(
                avro: avro, messageName: "echo",
                from: data, serverHash: serverHash, session: session)

        #expect(header.flag == false)
        #expect(responses[0].result == "success")
    }

    @Test("encodeResponse throws missingSchema when clientHash absent")
    func encodeResponseMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        let dummy = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await server.encodeResponse(
                avro: avro, header: dummy, messageName: "echo",
                parameter: RespType(result: "x"), session: session)
        }
    }

    // MARK: encodeErrorResponse

    @Test("encodeErrorResponse produces valid data")
    func encodeErrorResponseHappyPath() async throws {
        let (avro, session, client, server) = try await makeLoadedSession()

        let handshakeReq = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        let data = try await server.encodeErrorResponse(
            avro: avro, header: handshakeReq,
            messageName: "echo",
            errors: ["ErrType": ErrType(reason: "oops")],
            session: session)

        #expect(!data.isEmpty)

        let (header, errors): (ResponseHeader, [ErrType]) =
            try await client.decodeResponse(
                avro: avro, messageName: "echo",
                from: data, serverHash: serverHash, session: session)

        #expect(header.flag == true)
        #expect(errors[0].reason == "oops")
    }

    @Test("encodeErrorResponse with key not in protocol falls back to string union")
    func encodeErrorResponseUnknownKey() async throws {
        let (avro, session, client, server) = try await makeLoadedSession()

        let handshakeReq = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: [:])

        struct Wrapper: Codable, Equatable { var message: String }

        let data = try await server.encodeErrorResponse(
            avro: avro, header: handshakeReq, messageName: "echo",
            errors: ["Mystery": Wrapper(message: "fallback string")],
            session: session)

        #expect(data.count > 0)

        let (resHeader, parts): (ResponseHeader, [String]) =
            try await client.decodeResponse(
                avro: avro, messageName: "echo",
                from: data, serverHash: serverHash, session: session)
        #expect(resHeader.flag)
        #expect(parts.first?.contains("fallback") == true)
    }

    @Test("encodeErrorResponse throws missingSchema when clientHash absent")
    func encodeErrorResponseMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: coverageProtocolJSON)
        let dummy = HandshakeRequest(
            clientHash: clientHash, clientProtocol: nil,
            serverHash: serverHash, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await server.encodeErrorResponse(
                avro: avro, header: dummy, messageName: "echo",
                errors: ["ErrType": ErrType(reason: "x")], session: session)
        }
    }
}

// MARK: - Helper for makeLoadedSession

private func makeSchemas(avro: Avro = Avro()) -> (AvroSchema, AvroSchema, AvroSchema) {
    let req = avro.newSchema(schema: MessageConstant.requestSchema)!
    let resp = avro.newSchema(schema: MessageConstant.responseSchema)!
    let meta = avro.newSchema(schema: MessageConstant.metadataSchema)!
    return (req, resp, meta)
}

// ===========================================================================
// MARK: - SessionCache full coverage
// ===========================================================================

@Suite("SessionCache full coverage")
struct SessionCacheFullCoverageTests {

    // MARK: ClientSessionCache tests

    @Test("ClientSessionCache starts empty")
    func clientCacheEmpty() async {
        let cache = ClientSessionCache()
        #expect(await cache.avroProtocol(for: clientHash) == nil)
    }

    @Test("ClientSessionCache add then retrieve returns correct protocol name")
    func clientCacheAddRetrieve() async throws {
        let cache = ClientSessionCache()
        let json = """
        {"protocol":"TestProtocol","types":[],"messages":{}}
        """
        try await cache.add(hash: clientHash, protocolString: json)
        let proto = await cache.avroProtocol(for: clientHash)
        #expect(proto != nil)
        #expect(proto?.name == "TestProtocol")
    }

    @Test("ClientSessionCache remove makes entry absent")
    func clientCacheRemove() async throws {
        let cache = ClientSessionCache()
        let json = """
        {"protocol":"ToRemove","types":[],"messages":{}}
        """
        try await cache.add(hash: clientHash, protocolString: json)
        #expect(await cache.avroProtocol(for: clientHash) != nil)
        await cache.remove(for: clientHash)
        #expect(await cache.avroProtocol(for: clientHash) == nil)
    }

    @Test("ClientSessionCache remove on absent key is a no-op")
    func clientCacheRemoveAbsent() async {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xFF, count: 16)
        await cache.remove(for: hash)
        #expect(await cache.avroProtocol(for: hash) == nil)
    }

    @Test("ClientSessionCache clear removes all entries")
    func clientCacheClear() async throws {
        let cache = ClientSessionCache()
        let hash1 = [UInt8](repeating: 5, count: 16)
        let hash2 = [UInt8](repeating: 6, count: 16)
        let json = """
        {"protocol":"Test","types":[],"messages":{}}
        """
        try await cache.add(hash: hash1, protocolString: json)
        try await cache.add(hash: hash2, protocolString: json)
        await cache.clear()
        #expect(await cache.avroProtocol(for: hash1) == nil)
        #expect(await cache.avroProtocol(for: hash2) == nil)
    }

    @Test("ClientSessionCache clear on empty cache is a no-op")
    func clientCacheClearEmpty() async {
        let cache = ClientSessionCache()
        await cache.clear()
        #expect(await cache.avroProtocol(for: [UInt8](repeating: 0, count: 16)) == nil)
    }

    // MARK: ServerSessionCache tests

    @Test("ServerSessionCache starts empty")
    func serverCacheEmpty() async {
        let cache = ServerSessionCache()
        let hash = [UInt8](repeating: 0, count: 16)
        #expect(await cache.contains(hash: hash) == false)
    }

    @Test("ServerSessionCache add then contains returns true")
    func serverCacheAddContains() async throws {
        let cache = ServerSessionCache()
        let hash = [UInt8](repeating: 2, count: 16)
        let json = """
        {"protocol":"ServerProtocol","types":[],"messages":{}}
        """
        try await cache.add(hash: hash, protocolString: json)
        #expect(await cache.contains(hash: hash))
    }

    @Test("ServerSessionCache remove makes entry absent")
    func serverCacheRemove() async throws {
        let cache = ServerSessionCache()
        let hash = [UInt8](repeating: 0xCC, count: 16)
        let json = """
        {"protocol":"RemovableServer","types":[],"messages":{}}
        """
        try await cache.add(hash: hash, protocolString: json)
        #expect(await cache.contains(hash: hash))
        await cache.remove(for: hash)
        #expect(await cache.contains(hash: hash) == false)
    }

    @Test("ServerSessionCache clear removes all entries")
    func serverCacheClear() async throws {
        let cache = ServerSessionCache()
        let hash1 = [UInt8](repeating: 7, count: 16)
        let hash2 = [UInt8](repeating: 8, count: 16)
        let json = """
        {"protocol":"Test","types":[],"messages":{}}
        """
        try await cache.add(hash: hash1, protocolString: json)
        try await cache.add(hash: hash2, protocolString: json)
        await cache.clear()
        #expect(await cache.contains(hash: hash1) == false)
        #expect(await cache.contains(hash: hash2) == false)
    }

    // MARK: SessionCache error paths

    @Test("add with invalid JSON throws")
    func cacheAddInvalidJSON() async {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 13, count: 16)
        await #expect(throws: (any Error).self) {
            try await cache.add(hash: hash, protocolString: "not valid json")
        }
    }

    @Test("add with empty string throws")
    func cacheAddEmptyString() async {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 12, count: 16)
        await #expect(throws: (any Error).self) {
            try await cache.add(hash: hash, protocolString: "")
        }
    }

    @Test("add with JSON missing protocol key throws")
    func cacheAddMissingProtocolKey() async {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xEE, count: 16)
        let badJSON = """
        {"notAProtocol": true}
        """
        await #expect(throws: (any Error).self) {
            try await cache.add(hash: hash, protocolString: badJSON)
        }
    }

    // MARK: Schema lookup: nil paths (unknown hash)

    @Test("requestSchemas returns nil for unknown hash")
    func cacheRequestSchemasNilUnknownHash() async {
        let cache = ClientSessionCache()
        #expect(await cache.requestSchemas(
            hash: [UInt8](repeating: 9, count: 16), messageName: "test") == nil)
    }

    @Test("responseSchema returns nil for unknown hash")
    func cacheResponseSchemaNilUnknownHash() async {
        let cache = ClientSessionCache()
        #expect(await cache.responseSchema(
            hash: [UInt8](repeating: 10, count: 16), messageName: "test") == nil)
    }

    @Test("errorSchemas returns nil for unknown hash")
    func cacheErrorSchemasNilUnknownHash() async {
        let cache = ClientSessionCache()
        #expect(await cache.errorSchemas(
            hash: [UInt8](repeating: 11, count: 16), messageName: "test") == nil)
    }

    // MARK: Schema lookup: nil paths (known hash, unknown message)

    @Test("requestSchemas returns nil for known hash but unknown message")
    func cacheRequestSchemasNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xA0, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.requestSchemas(hash: hash, messageName: "nonexistent") == nil)
    }

    @Test("responseSchema returns nil for known hash but unknown message")
    func cacheResponseSchemaNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xA1, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.responseSchema(hash: hash, messageName: "nonexistent") == nil)
    }

    @Test("errorSchemas returns nil for known hash but unknown message")
    func cacheErrorSchemasNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xA2, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.errorSchemas(hash: hash, messageName: "nonexistent") == nil)
    }

    // MARK: Schema lookup: happy paths

    @Test("requestSchemas returns one schema for known message")
    func cacheRequestSchemasHappy() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 14, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        let schemas = await cache.requestSchemas(hash: hash, messageName: "echo")
        #expect(schemas != nil)
        #expect(schemas?.count == 1)
    }

    @Test("responseSchema returns non-nil schema for known message")
    func cacheResponseSchemaHappy() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 15, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.responseSchema(hash: hash, messageName: "echo") != nil)
    }

    @Test("errorSchemas returns non-nil entry keyed by error type name")
    func cacheErrorSchemasHappy() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 16, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        let schemas = await cache.errorSchemas(hash: hash, messageName: "echo")
        #expect(schemas != nil)
        #expect(schemas?["ErrType"] != nil)
    }

    // MARK: Concurrent access

    @Test("ClientSessionCache handles concurrent adds")
    func clientCacheConcurrentAdds() async throws {
        let cache = ClientSessionCache()
        let json = """
        {"protocol":"ConcurrentProto","types":[],"messages":{}}
        """
        try await withThrowingTaskGroup(of: Void.self) { group in
            for byte in UInt8(0x20)..<UInt8(0x30) {
                let hash = [UInt8](repeating: byte, count: 16)
                group.addTask {
                    try await cache.add(hash: hash, protocolString: json)
                }
            }
            try await group.waitForAll()
        }
        for byte in UInt8(0x20)..<UInt8(0x30) {
            let hash = [UInt8](repeating: byte, count: 16)
            let proto = await cache.avroProtocol(for: hash)
            #expect(proto?.name == "ConcurrentProto")
        }
    }

    @Test("ServerSessionCache handles concurrent adds")
    func serverCacheConcurrentAdds() async throws {
        let cache = ServerSessionCache()
        let json = """
        {"protocol":"ConcurrentSvr","types":[],"messages":{}}
        """
        try await withThrowingTaskGroup(of: Void.self) { group in
            for byte in UInt8(0x40)..<UInt8(0x50) {
                let hash = [UInt8](repeating: byte, count: 16)
                group.addTask {
                    try await cache.add(hash: hash, protocolString: json)
                }
            }
            try await group.waitForAll()
        }
        for byte in UInt8(0x40)..<UInt8(0x50) {
            let hash = [UInt8](repeating: byte, count: 16)
            #expect(await cache.contains(hash: hash))
        }
    }
}

// ===========================================================================
// MARK: - AvroProtocol full coverage
// ===========================================================================

@Suite("AvroProtocol full coverage")
struct AvroProtocolFullCoverageTests {

    // MARK: Top-level fields

    @Test("Top-level protocol fields decode correctly")
    func topLevelFields() throws {
        let json = """
        {"namespace": "com.acme", "protocol": "HelloWorld", "doc": "Protocol Greetings",
         "types": [],
         "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(proto.type == "protocol")
        #expect(proto.name == "HelloWorld")
        #expect(proto.namespace == "com.acme")
        #expect(proto.doc == "Protocol Greetings")
    }

    // MARK: Message oneway field

    @Test("Message encodes and decodes oneway field")
    func messageOneWay() throws {
        let json = """
        {
          "protocol": "OneWayProto",
          "messages": {
            "fire": {
              "request": [{"name":"x","type":"int"}],
              "response": "int",
              "one-way": true
            }
          }
        }
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let fire = try #require(proto.messages?["fire"])
        #expect(fire.oneway == true)
    }

    @Test("Message oneway defaults to nil when absent")
    func messageOneWayDefaultNil() throws {
        let json = """
        {
          "protocol": "NoOneWay",
          "messages": {
            "add": {
              "request": [{"name":"x","type":"int"}],
              "response": "int"
            }
          }
        }
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let add = try #require(proto.messages?["add"])
        #expect(add.oneway == nil)
    }

    // MARK: RequestType

    @Test("RequestType stores name and type")
    func requestType() {
        let rt = RequestType(name: "param1", type: "MyRecord")
        #expect(rt.name == "param1")
        #expect(rt.type == "MyRecord")
    }

    @Test("RequestType equatable conformance")
    func requestTypeEquatable() {
        let a = RequestType(name: "x", type: "A")
        let b = RequestType(name: "x", type: "A")
        let c = RequestType(name: "y", type: "A")
        #expect(a == b)
        #expect(a != c)
    }

    // MARK: AvroProtocol with no messages

    @Test("AvroProtocol with no messages decodes with empty messages")
    func protocolNoMessages() throws {
        let json = """
        {"protocol": "NoMsgs", "types": [], "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(proto.messages?.isEmpty == true)
    }

    // MARK: getRequest with no request field

    @Test("getRequest returns nil when message has no request")
    func getRequestNoRequestField() throws {
        let json = """
        {
          "protocol": "Simple",
          "types": [{"name":"R","type":"record","fields":[{"name":"v","type":"int"}]}],
          "messages": {
            "notify": { "response": "R" }
          }
        }
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let schemas = proto.getRequest(messageName: "notify")
        #expect(schemas == nil)
    }

    // MARK: getResponse with string response matching type

    @Test("getResponse returns schema when response string matches type name")
    func getResponseMatchesTypeName() throws {
        let json = """
        {
          "protocol": "RespTest",
          "types": [{"name":"Answer","type":"record","fields":[{"name":"v","type":"string"}]}],
          "messages": {
            "ask": { "response": "Answer" }
          }
        }
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let schema = proto.getResponse(messageName: "ask")
        #expect(schema != nil)
        #expect(schema?.getName() == "Answer")
    }

    // MARK: getErrors with no errors

    @Test("getErrors returns nil when message has no errors")
    func getErrorsNoErrors() throws {
        let json = """
        {
          "protocol": "NoErr",
          "types": [{"name":"E","type":"error","fields":[{"name":"m","type":"string"}]}],
          "messages": {
            "safe": { "response": "int" }
          }
        }
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let errors = proto.getErrors(messageName: "safe")
        #expect(errors == nil)
    }

    // MARK: Message validate when types are invalid

    @Test("Message validate returns false for request with unknown type")
    func messageValidateBadRequestType() {
        let types: [AvroSchema] = []
        let msg = Message(
            doc: nil, request: [RequestType(name: "x", type: "Unknown")],
            response: "int", errors: nil, oneway: nil)
        #expect(msg.validate(types: types) == false)
    }

    @Test("Message validate returns false for response with unknown type")
    func messageValidateBadResponseType() {
        let types: [AvroSchema] = []
        let msg = Message(
            doc: nil, request: nil, response: "Unknown", errors: nil, oneway: nil)
        #expect(msg.validate(types: types) == false)
    }

    @Test("Message validate returns false for errors with unknown type")
    func messageValidateBadErrorType() {
        let types: [AvroSchema] = []
        let msg = Message(
            doc: nil, request: nil, response: nil, errors: ["Unknown"], oneway: nil)
        #expect(msg.validate(types: types) == false)
    }

    // MARK: Message addRequest

    @Test("Message addRequest does not duplicate existing entry")
    func messageAddRequestDedup() {
        let recSchema = Avro().decodeSchema(schema: """
            {"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}
        """)!
        var msg = Message(
            doc: nil, request: [RequestType(name: "x", type: "R")],
            response: nil, errors: nil, oneway: nil)
        msg.addRequest(types: [recSchema], name: "x", type: "R")
        #expect(msg.request?.count == 1)
    }

    @Test("Message addRequest ignores unknown type")
    func messageAddRequestIgnoresUnknown() {
        let recSchema = Avro().decodeSchema(schema: """
            {"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}
        """)!
        var msg = Message(doc: nil, request: [], response: nil, errors: nil, oneway: nil)
        msg.addRequest(types: [recSchema], name: "x", type: "Unknown")
        #expect(msg.request?.isEmpty == true)
    }

    // MARK: Message addError

    @Test("Message addError does not duplicate")
    func messageAddErrorDedup() {
        let errSchema = Avro().decodeSchema(schema: """
            {"type":"error","name":"E","fields":[{"name":"m","type":"string"}]}
        """)!
        var msg = Message(
            doc: nil, request: nil, response: nil, errors: ["E"], oneway: nil)
        msg.addError(types: [errSchema], errorName: "E")
        #expect(msg.errors?.count == 1)
    }

    @Test("Message addError ignores unknown type")
    func messageAddErrorIgnoresUnknown() {
        let errSchema = Avro().decodeSchema(schema: """
            {"type":"error","name":"E","fields":[{"name":"m","type":"string"}]}
        """)!
        var msg = Message(doc: nil, request: nil, response: nil, errors: [], oneway: nil)
        msg.addError(types: [errSchema], errorName: "Unknown")
        #expect(msg.errors?.isEmpty == true)
    }

    // MARK: AvroProtocol equality

    @Test("Two decodings of same JSON are equal")
    func equalitySameJson() throws {
        let json = """
        {"namespace": "com.acme", "protocol": "P", "types": [], "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        let p1 = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let p2 = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(p1 == p2)
    }

    @Test("equality: one types=nil, other has types -> false")
    func equalityOneTypesNil() throws {
        let json1 = #"{"protocol":"P1","messages":{}}"#
        let json2 = """
        {"protocol":"P2","types":[{"type":"record","name":"R","fields":[]}],"messages":{}}
        """
        let d1 = try #require(json1.data(using: .utf8))
        let d2 = try #require(json2.data(using: .utf8))
        var p1 = try JSONDecoder().decode(AvroProtocol.self, from: d1)
        let p2 = try JSONDecoder().decode(AvroProtocol.self, from: d2)
        p1.types = nil
        #expect(p1 != p2)
    }

    @Test("equality: both types nil -> true")
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

    // MARK: AvroProtocol addType

    @Test("addType adds new schema")
    func addType() throws {
        let json = """
        {"namespace": "com.acme", "protocol": "HelloWorld",
         "types": [
            {"name":"Greeting","type":"record","fields":[{"name":"message","type":"string"}]}
         ],
         "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let initialCount = proto.types?.count ?? 0
        let newSchema = Avro().decodeSchema(schema: """
            {"type":"record","name":"NewRecord","fields":[]}
        """)!
        proto.addType(schema: newSchema)
        #expect(proto.types?.count == initialCount + 1)
    }

    @Test("addType does not duplicate schema")
    func addTypeDuplicate() throws {
        let json = """
        {"namespace": "com.acme", "protocol": "HelloWorld",
         "types": [
            {"name":"Greeting","type":"record","fields":[{"name":"message","type":"string"}]}
         ],
         "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let greeting = try #require(proto.types?[0])
        let originalCount = proto.types?.count ?? 0
        proto.addType(schema: greeting)
        #expect(proto.types?.count == originalCount)
    }

    @Test("addType on existing types array appends correctly")
    func addTypeExistingArray() throws {
        let json = """
        {"protocol":"P","types":[{"type":"record","name":"A","fields":[]}],"messages":{}}
        """
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let newSchema = Avro().decodeSchema(schema: """
            {"type":"record","name":"B","fields":[]}
        """)!
        proto.addType(schema: newSchema)
        #expect(proto.types?.count == 2)
    }

    @Test("addType initialises types array on first call when nil")
    func addTypeInitialisesWhenNil() throws {
        let json = #"""
        {"namespace":"com.example","protocol":"Empty","messages":{}}
        """#
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        proto.types = nil
        let schema = Avro().decodeSchema(schema: """
            {"type":"record","name":"R","fields":[]}
        """)!
        proto.addType(schema: schema)
        #expect(proto.types?.count == 1)
    }

    // MARK: AvroProtocol addMessage

    @Test("addMessage adds new message")
    func addMessage() throws {
        let json = """
        {"namespace": "com.acme", "protocol": "HelloWorld",
         "types": [
            {"name":"Req","type":"record","fields":[]},
            {"name":"Resp","type":"record","fields":[]}
         ],
         "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let recSchema = try #require(Avro().decodeSchema(schema: """
            {"type":"record","name":"Req","fields":[]}
        """))
        proto.addType(schema: recSchema)
        let msg = Message(doc: nil, request: nil, response: "Resp", errors: nil, oneway: nil)
        proto.addMessage(name: "newMessage", message: msg)
        #expect(proto.messages?["newMessage"] != nil)
    }

    @Test("addMessage ignores invalid message")
    func addMessageInvalid() throws {
        let json = """
        {"namespace": "com.acme", "protocol": "HelloWorld",
         "types": [],
         "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let message = Message(doc: nil, request: nil, response: "NonExistent", errors: nil, oneway: nil)
        proto.addMessage(name: "badMessage", message: message)
        #expect(proto.messages?["badMessage"] == nil)
    }

    @Test("addMessage on existing messages dict adds correctly")
    func addMessageExistingDict() throws {
        let json = """
        {"protocol":"P","types":[{"type":"record","name":"R","fields":[]}],"messages":{"old":{}}}
        """
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        let msg = Message(doc: nil, request: nil, response: "R", errors: nil, oneway: nil)
        proto.addMessage(name: "new", message: msg)
        #expect(proto.messages?.count == 2)
        #expect(proto.messages?["new"] != nil)
    }

    @Test("addMessage does nothing when types is nil")
    func addMessageTypesNil() throws {
        let json = #"{"protocol":"P","messages":{}}"#
        let data = try #require(json.data(using: .utf8))
        var proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        proto.types = nil
        let msg = Message(doc: nil, request: nil, response: "R", errors: nil, oneway: nil)
        proto.addMessage(name: "x", message: msg)
        #expect(proto.messages == nil || proto.messages?.isEmpty == true)
    }

    // MARK: getRequest/Response/Errors with empty message name

    @Test("getRequest with empty string name returns nil")
    func getRequestEmptyName() throws {
        let proto = try JSONDecoder().decode(
            AvroProtocol.self, from: coverageProtocolJSON.data(using: .utf8)!)
        #expect(proto.getRequest(messageName: "") == nil)
    }

    @Test("getResponse with empty string name returns nil")
    func getResponseEmptyName() throws {
        let proto = try JSONDecoder().decode(
            AvroProtocol.self, from: coverageProtocolJSON.data(using: .utf8)!)
        #expect(proto.getResponse(messageName: "") == nil)
    }

    @Test("getErrors with empty string name returns nil")
    func getErrorsEmptyName() throws {
        let proto = try JSONDecoder().decode(
            AvroProtocol.self, from: coverageProtocolJSON.data(using: .utf8)!)
        #expect(proto.getErrors(messageName: "") == nil)
    }

    // MARK: getRequest/Response/Errors with known hash but unknown message

    @Test("getRequest returns nil for known hash but unknown message name")
    func getRequestNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xA0, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.requestSchemas(hash: hash, messageName: "nonexistent") == nil)
    }

    @Test("getResponse returns nil for known hash but unknown message name")
    func getResponseNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xA1, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.responseSchema(hash: hash, messageName: "nonexistent") == nil)
    }

    @Test("getErrors returns nil for known hash but unknown message name")
    func getErrorsNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash = [UInt8](repeating: 0xA2, count: 16)
        try await cache.add(hash: hash, protocolString: coverageProtocolJSON)
        #expect(await cache.errorSchemas(hash: hash, messageName: "nonexistent") == nil)
    }

    // MARK: Message struct initialization

    @Test("Message init stores all fields correctly")
    func messageInit() {
        let req = [RequestType(name: "x", type: "int")]
        let msg = Message(doc: "docs", request: req, response: "string", errors: ["Err"], oneway: true)
        #expect(msg.doc == "docs")
        #expect(msg.request?.count == 1)
        #expect(msg.response == "string")
        #expect(msg.errors?.count == 1)
        #expect(msg.oneway == true)
    }

    // MARK: AvroProtocol with namespace/aliases/doc

    @Test("AvroProtocol preserves namespace from JSON")
    func protocolNamespace() throws {
        let json = """
        {"namespace": "com.example", "protocol": "Namespaced", "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(proto.namespace == "com.example")
    }

    @Test("AvroProtocol preserves aliases from JSON")
    func protocolAliases() throws {
        let json = """
        {"protocol": "Aliased", "aliases": ["old.name"], "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(proto.aliases?.contains("old.name") == true)
    }

    @Test("AvroProtocol preserves doc from JSON")
    func protocolDoc() throws {
        let json = """
        {"protocol": "Docked", "doc": "Protocol docs", "messages": {}}
        """
        let data = try #require(json.data(using: .utf8))
        let proto = try JSONDecoder().decode(AvroProtocol.self, from: data)
        #expect(proto.doc == "Protocol docs")
    }

    // MARK: HandshakeRequest/Response decoding

    @Test("HandshakeRequest with no clientProtocol and no meta decodes correctly")
    func requestDecodeNullProtocolNoMeta() throws {
        let raw = Data([
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0,
            0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
            0
        ])
        let avro = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.requestSchema))
        let model = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
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
        let avro = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.requestSchema))
        let model = try AvroDecoder(schema: schema).decode(HandshakeRequest.self, from: raw)
        #expect(model.clientProtocol == "foo")
        #expect(model.meta == nil)
    }

    @Test("HandshakeResponse BOTH match encodes to four zero bytes")
    func responseEncodeBOTH() throws {
        let avro = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.responseSchema))
        let data = try AvroEncoder().encode(
            HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil),
            schema: schema)
        #expect(data == Data([0, 0, 0, 0]))
    }

    @Test("HandshakeResponse NONE first byte is zigzag 4")
    func responseEncodeNONE() throws {
        let avro = Avro()
        let schema = try #require(avro.newSchema(schema: MessageConstant.responseSchema))
        let resp = HandshakeResponse(
            match: .NONE, serverProtocol: "foo",
            serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf])
        let data = try AvroEncoder().encode(resp, schema: schema)
        #expect(data.first == 4)
    }
}
