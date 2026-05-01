//
//  AvroRequestResponseTest.swift
//  SwiftAvroCoreTests
//
//  Coverage suite for AvroIPCRequest (Request.swift) and AvroIPCResponse (Response.swift).
//
//  Redundant unit tests that were fully exercised by the end-to-end round-trip
//  tests have been removed. Each remaining test covers exactly one branch or
//  scenario not already proven by another test.
//

import Testing
import Foundation
@testable import SwiftAvroCore

// MARK: - Shared constants

private let supportProtocol = """
{
  "namespace": "com.acme", "protocol": "HelloWorld",
  "types": [
    {"name":"Greeting","type":"record","fields":[{"name":"message","type":"string"}]},
    {"name":"Curse","type":"error","fields":[{"name":"message","type":"string"}]}
  ],
  "messages": {
    "hello": {
      "request":  [{"name":"greeting","type":"Greeting"}],
      "response": "Greeting",
      "errors":   ["Curse"]
    }
  }
}
"""

private let clientHash: MD5Hash = [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10]
private let serverHash: MD5Hash = [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10]

private struct Greeting: Codable, Equatable { var message: String }
private struct Curse:    Codable, Equatable { var message: String }

// MARK: - Fixture helpers

/// Builds the three schemas required by AvroIPCContext.
/// HandshakeResponse uses NONE/BOTH/CLIENT per the Avro IPC spec.
private func makeSchemas(avro: Avro = Avro()) -> (req: AvroSchema, resp: AvroSchema, meta: AvroSchema) {
    let req = avro.newSchema(schema: """
        {"type":"record","name":"HandshakeRequest","fields":[
            {"name":"clientHash","type":{"type":"fixed","name":"MD5","size":16}},
            {"name":"clientProtocol","type":["null","string"]},
            {"name":"serverHash","type":{"type":"fixed","name":"MD5","size":16}},
            {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
        ]}
        """)!
    let resp = avro.newSchema(schema: """
        {"type":"record","name":"HandshakeResponse","fields":[
            {"name":"match","type":{"type":"enum","name":"HandshakeMatch",
                "symbols":["NONE","BOTH","CLIENT"]}},
            {"name":"serverProtocol","type":["null","string"]},
            {"name":"serverHash","type":["null",{"type":"fixed","name":"MD5","size":16}]},
            {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
        ]}
        """)!
    let meta = avro.newSchema(schema: """
    {"type":"map","values":"bytes"}
    """)!
    return (req, resp, meta)
}

/// Returns a (context, session, avro) triple.
private func makeSession(knownProtocols: Set<String>? = nil)
    -> (context: AvroIPCContext, session: AvroIPCSession, avro: Avro)
{
    let avro = Avro()
    let (req, resp, meta) = makeSchemas(avro: avro)
    let context = AvroIPCContext(requestSchema: req, responseSchema: resp, metaSchema: meta,
                                 requestMeta: [:], responseMeta: [:],
                                 knownProtocols: knownProtocols)
    return (context, AvroIPCSession(context: context), avro)
}

/// Runs the canonical NONE→BOTH handshake and returns everything needed for call-level tests.
private func performFullHandshake() async throws -> (
    avro: Avro, session: AvroIPCSession,
    client: AvroIPCRequest, server: AvroIPCResponse,
    handshakeReq: HandshakeRequest
) {
    let (_, session, avro) = makeSession()
    let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)
    let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)

// Leg 1: initial (nil clientProtocol) → NONE
        let initialData = try client.encodeInitialHandshake(avro: avro, session: session)
        let (_, initialResp, _) = try await server.resolveHandshake(avro: avro, from: initialData, session: session)
        _ = try await client.resolveHandshakeResponse(try client.decodeHandshakeResponse(avro: avro, from: initialResp, session: session).0, avro: avro, session: session)

        // Leg 2: retry with full protocol + matching serverHash → BOTH
        let retryData = try client.encodeHandshake(avro: avro, serverHash: serverHash, session: session)
        let (handshakeReq, retryResp, _) = try await server.resolveHandshake(avro: avro, from: retryData, session: session)
        _ = try await client.resolveHandshakeResponse(try client.decodeHandshakeResponse(avro: avro, from: retryResp, session: session).0, avro: avro, session: session)

    return (avro, session, client, server, handshakeReq)
}

// ============================================================================
// MARK: - AvroIPCRequest
// ============================================================================

@Suite("AvroIPCRequest")
struct AvroIPCRequestTests {

    // MARK: init

    @Test("init stores fields; knownProtocols=nil always passes")
    func initHappyPath() throws {
        let (_, session, _) = makeSession()
        let req = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)
        #expect(req.clientHash == clientHash)
        #expect(req.clientProtocol == supportProtocol)
    }

    @Test("init: allowed protocol passes, unknown protocol throws")
    func initKnownProtocols() throws {
        let (_, session, _) = makeSession(knownProtocols: ["HelloWorld"])
        // Allowed name must not throw.
        _ = try AvroIPCRequest(clientHash: clientHash, clientProtocol: "HelloWorld", session: session)
        // Unknown name must throw.
        #expect(throws: AvroHandshakeError.self) {
            _ = try AvroIPCRequest(clientHash: clientHash, clientProtocol: "Unknown", session: session)
        }
    }

    // MARK: encodeInitialHandshake / encodeHandshake

    @Test("encodeInitialHandshake sets clientProtocol=nil; encodeHandshake sets full protocol")
    func encodeHandshakeDifference() throws {
        let (context, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)

        // Initial: clientProtocol must be nil per Avro IPC spec.
        let initial = try client.encodeInitialHandshake(avro: avro, session: session)
        avro.setSchema(schema: context.requestSchema)
        let decodedInitial: HandshakeRequest = try avro.decode(from: initial)
        #expect(decodedInitial.clientProtocol == nil)
        #expect(decodedInitial.clientHash == clientHash)

        // Retry: clientProtocol must be the full string.
        let retry = try client.encodeHandshake(avro: avro, serverHash: serverHash, session: session)
        avro.setSchema(schema: context.requestSchema)
        let decodedRetry: HandshakeRequest = try avro.decode(from: retry)
        #expect(decodedRetry.clientProtocol == supportProtocol)
        #expect(decodedRetry.serverHash == serverHash)
    }

    // MARK: decodeHandshakeResponse

    @Test("decodeHandshakeResponse cleanly splits response bytes from trailing payload")
    func decodeHandshakeResponse() throws {
        let (context, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)

        let serverResp = HandshakeResponse(match: .NONE, serverProtocol: supportProtocol,
                                           serverHash: serverHash, meta: nil)
        avro.setSchema(schema: context.responseSchema)
        let payload = Data([0xDE, 0xAD, 0xBE, 0xEF])
        let combined = try avro.encode(serverResp) + payload

        let (decoded, remainder) = try client.decodeHandshakeResponse(avro: avro, from: combined, session: session)
        #expect(decoded.match == .NONE)
        #expect(remainder == payload)
    }

    // MARK: resolveHandshakeResponse

    @Test("resolveHandshakeResponse: BOTH → nil, NONE → retry data, NONE missing hash → throws")
    func resolveHandshakeResponseBothAndNone() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)

        // .BOTH must return nil (handshake complete).
        let both = HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil, meta: nil)
        #expect(try await client.resolveHandshakeResponse(both, avro: avro, session: session) == nil)

        // .NONE with serverHash must return non-empty retry data.
        let none = HandshakeResponse(match: .NONE, serverProtocol: supportProtocol,
                                     serverHash: serverHash, meta: nil)
        let retry = try await client.resolveHandshakeResponse(none, avro: avro, session: session)
        #expect(retry != nil && !retry!.isEmpty)

        // .NONE without serverHash must throw.
        let noneNoHash = HandshakeResponse(match: .NONE, serverProtocol: nil, serverHash: nil, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.resolveHandshakeResponse(noneNoHash, avro: avro, session: session)
        }
    }

    @Test("resolveHandshakeResponse: CLIENT populates cache; missing hash or protocol throws")
    func resolveHandshakeResponseClient() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)

        // Full CLIENT response must populate clientCache and return nil.
        let full = HandshakeResponse(match: .CLIENT, serverProtocol: supportProtocol,
                                     serverHash: serverHash, meta: nil)
        #expect(try await client.resolveHandshakeResponse(full, avro: avro, session: session) == nil)
        let proto = await session.clientCache.avroProtocol(for: serverHash)
        #expect(proto?.name == "HelloWorld")

        // Missing serverHash must throw.
        let noHash = HandshakeResponse(match: .CLIENT, serverProtocol: supportProtocol,
                                       serverHash: nil, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.resolveHandshakeResponse(noHash, avro: avro, session: session)
        }

        // Missing serverProtocol must throw.
        let noProto = HandshakeResponse(match: .CLIENT, serverProtocol: nil,
                                        serverHash: serverHash, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.resolveHandshakeResponse(noProto, avro: avro, session: session)
        }
    }

    // MARK: encodeCall

    @Test("encodeCall ping (empty name) produces exactly two zero bytes; server decodes empty params")
    func encodeCallPing() async throws {
        let (avro, session, client, server, handshakeReq) = try await performFullHandshake()
        struct Empty: Codable {}

        let callData = try await client.encodeCall(avro: avro, messageName: "",
                                                   parameters: [Empty()],
                                                   serverHash: serverHash, session: session)
        #expect(callData == Data([0x00, 0x00]))

        let (header, params): (RequestHeader, [Empty]) =
            try await server.decodeCall(avro: avro, header: handshakeReq, from: callData, session: session)
        #expect(header.name == "")
        #expect(params.isEmpty)
    }

    @Test("encodeCall throws missingSchema when serverHash absent from clientCache")
    func encodeCallMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await client.encodeCall(avro: avro, messageName: "hello",
                                            parameters: [Greeting(message: "x")],
                                            serverHash: serverHash, session: session)
        }
    }

    // MARK: decodeResponse — missing schema only
    // (success and error paths are covered by the end-to-end tests below)

    @Test("decodeResponse throws missingSchema when serverHash absent from clientCache")
    func decodeResponseMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)
        // Minimal success-response bytes: hasMeta=0, flag=false, then a dummy payload.
        let raw = Data([0x00, 0x00, 0x06]) + "foo".data(using: .utf8)!
        await #expect(throws: AvroHandshakeError.self) {
            let _: (ResponseHeader, [Greeting]) =
                try await client.decodeResponse(avro: avro, messageName: "hello",
                                                from: raw, serverHash: serverHash,
                                                session: session)
        }
    }
}

// ============================================================================
// MARK: - AvroIPCResponse
// ============================================================================

@Suite("AvroIPCResponse")
struct AvroIPCResponseTests {

    // MARK: init

    @Test("init stores serverHash and serverProtocol")
    func initStoresFields() {
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)
        #expect(server.serverHash == serverHash)
        #expect(server.serverProtocol == supportProtocol)
    }

    // MARK: resolveHandshake — all server-side match branches

    @Test("resolveHandshake NONE: unknown client without clientProtocol")
    func resolveHandshakeNone() async throws {
        let (context, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)

        let data = try client.encodeInitialHandshake(avro: avro, session: session)
        let (_, responseData, _) = try await server.resolveHandshake(avro: avro, from: data, session: session)

        avro.setSchema(schema: context.responseSchema)
        let response: HandshakeResponse = try avro.decode(from: responseData)
        #expect(response.match          == .NONE)
        #expect(response.serverHash     == serverHash)
        #expect(response.serverProtocol == supportProtocol)
    }

    @Test("resolveHandshake CLIENT: known client with wrong serverHash")
    func resolveHandshakeClientWrongHash() async throws {
        let (context, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)
        try await session.serverCache.add(hash: clientHash, protocolString: supportProtocol)

        let (reqSchema, _, _) = makeSchemas()
        let req = HandshakeRequest(clientHash: clientHash, clientProtocol: supportProtocol,
                                   serverHash: clientHash,  // deliberately wrong
                                   meta: [:])
        avro.setSchema(schema: reqSchema)
        let data = try avro.encode(req)

        let (_, responseData, _) = try await server.resolveHandshake(avro: avro, from: data, session: session)
        avro.setSchema(schema: context.responseSchema)
        let response: HandshakeResponse = try avro.decode(from: responseData)
        #expect(response.match          == .CLIENT)
        #expect(response.serverHash     == serverHash)
        #expect(response.serverProtocol == supportProtocol)
    }

    @Test("resolveHandshake throws invalidClientHashLength for a short clientHash")
    func resolveHandshakeInvalidHashLength() async throws {
        let (_, _, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)

        // Build a context whose requestSchema uses fixed(4) so we can encode a short hash.
        let shortHashSchema = avro.newSchema(schema: """
            {"type":"record","name":"HandshakeRequest","fields":[
                {"name":"clientHash","type":{"type":"fixed","name":"MD4","size":4}},
                {"name":"clientProtocol","type":["null","string"]},
                {"name":"serverHash","type":{"type":"fixed","name":"MD4","size":4}},
                {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
            ]}
            """)!
        let (_, respSchema, metaSchema) = makeSchemas()
        let shortSession = AvroIPCSession(context: AvroIPCContext(
            requestSchema: shortHashSchema, responseSchema: respSchema, metaSchema: metaSchema))

        let shortHash: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        avro.setSchema(schema: shortHashSchema)
        let data = try avro.encode(HandshakeRequest(clientHash: shortHash, clientProtocol: nil,
                                                    serverHash: shortHash, meta: [:]))
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await server.resolveHandshake(avro: avro, from: data, session: shortSession)
        }
    }

    // MARK: encodeResponse / encodeErrorResponse — missing schema only
    // (happy paths are covered by the end-to-end tests below)

    @Test("encodeResponse throws missingSchema when clientHash absent from serverCache")
    func encodeResponseMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)
        let dummy = HandshakeRequest(clientHash: clientHash, clientProtocol: nil,
                                     serverHash: serverHash, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await server.encodeResponse(avro: avro, header: dummy, messageName: "hello",
                                                parameter: Greeting(message: "x"), session: session)
        }
    }

    @Test("encodeErrorResponse throws missingSchema when clientHash absent from serverCache")
    func encodeErrorResponseMissingSchema() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)
        let dummy = HandshakeRequest(clientHash: clientHash, clientProtocol: nil,
                                     serverHash: serverHash, meta: nil)
        await #expect(throws: AvroHandshakeError.self) {
            _ = try await server.encodeErrorResponse(avro: avro, header: dummy, messageName: "hello",
                                                     errors: ["Curse": Curse(message: "x")], session: session)
        }
    }
}

// ============================================================================
// MARK: - End-to-end round-trip tests
// (one suite covers both sides together; avoids duplicating encode/decode logic)
// ============================================================================

@Suite("Avro IPC end-to-end")
struct AvroIPCEndToEndTests {

    @Test("Full NONE→BOTH handshake; clientHash registered in serverCache after leg 2")
    func fullHandshakeNoneThenBoth() async throws {
        let (context, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash, clientProtocol: supportProtocol, session: session)
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)

        // Leg 1: initial → NONE
        let initial = try client.encodeInitialHandshake(avro: avro, session: session)
        let (_, noneBytesRaw, _) = try await server.resolveHandshake(avro: avro, from: initial, session: session)
        let (noneResp, _) = try client.decodeHandshakeResponse(avro: avro, from: noneBytesRaw, session: session)
        #expect(noneResp.match == .NONE)

        let retryData = try await #require(
            client.resolveHandshakeResponse(noneResp, avro: avro, session: session))

        // Leg 2: retry → BOTH; clientHash must now be in serverCache
        let (_, bothBytesRaw, _) = try await server.resolveHandshake(avro: avro, from: retryData, session: session)
        avro.setSchema(schema: context.responseSchema)
        let bothResp: HandshakeResponse = try avro.decode(from: bothBytesRaw)
        #expect(bothResp.match          == .BOTH)
        #expect(bothResp.serverProtocol == nil)
        #expect(bothResp.serverHash     == nil)
        #expect(await session.serverCache.contains(hash: clientHash))
    }

    @Test("Normal request/response round-trip")
    func requestResponseNormal() async throws {
        let (avro, session, client, server, handshakeReq) = try await performFullHandshake()

        let callData = try await client.encodeCall(avro: avro, messageName: "hello",
                                                   parameters: [Greeting(message: "requestData")],
                                                   serverHash: serverHash, session: session)

        let (reqHeader, greetings): (RequestHeader, [Greeting]) =
            try await server.decodeCall(avro: avro, header: handshakeReq, from: callData, session: session)
        #expect(reqHeader.name       == "hello")
        #expect(greetings[0].message == "requestData")

        let resData = try await server.encodeResponse(avro: avro, header: handshakeReq,
                                                      messageName: reqHeader.name,
                                                      parameter: Greeting(message: "responseData"),
                                                      session: session)

        let (resHeader, responses): (ResponseHeader, [Greeting]) =
            try await client.decodeResponse(avro: avro, messageName: "hello",
                                            from: resData, serverHash: serverHash, session: session)
        #expect(!resHeader.flag)
        #expect(responses[0].message == "responseData")
    }

    @Test("Error response round-trip")
    func requestResponseError() async throws {
        let (avro, session, client, server, handshakeReq) = try await performFullHandshake()

        let resData = try await server.encodeErrorResponse(avro: avro, header: handshakeReq,
                                                           messageName: "hello",
                                                           errors: ["Curse": Curse(message: "responseError")],
                                                           session: session)

        let (resHeader, errors): (ResponseHeader, [Curse]) =
            try await client.decodeResponse(avro: avro, messageName: "hello",
                                            from: resData, serverHash: serverHash, session: session)
        #expect(resHeader.flag)
        #expect(errors[0].message == "responseError")
    }

    // MARK: - IPC gap tests from IPCGapTest.swift

    @Test("decodeCall throws missingSchema for an unknown message name")
    func decodeCallUnknownMessage() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)

        // Register clientHash so we get past the cache check.
        try await session.serverCache.add(hash: clientHash, protocolString: supportProtocol)

        // Build call payload manually: zero meta, then unknown message name.
        var data = Data()
        // zero meta map: 1 byte 0x00
        data.append(try avro.encodeFrom([String: [UInt8]](),
                                        schema: session.context.metaSchema))
        // message name "bogus" — Avro string
        data.append(try avro.encodeFrom("bogus", schema: AvroSchema(type: "string")))

        let header = HandshakeRequest(clientHash: clientHash, clientProtocol: nil,
                                      serverHash: serverHash, meta: [:])

        await #expect(throws: AvroHandshakeError.self) {
            let _: (RequestHeader, [Greeting]) =
                try await server.decodeCall(avro: avro, header: header, from: data, session: session)
        }
    }

    @Test("decodeCall ping returns empty parameters")
    func decodeCallPing() async throws {
        let (_, session, avro) = makeSession()
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)

        try await session.serverCache.add(hash: clientHash, protocolString: supportProtocol)

        var data = Data()
        data.append(try avro.encodeFrom([String: [UInt8]](),
                                        schema: session.context.metaSchema))
        data.append(try avro.encodeFrom("", schema: AvroSchema(type: "string")))

        let header = HandshakeRequest(clientHash: clientHash, clientProtocol: nil,
                                      serverHash: serverHash, meta: [:])

        let (h, p): (RequestHeader, [Greeting]) =
            try await server.decodeCall(avro: avro, header: header, from: data, session: session)
        #expect(h.name == "")
        #expect(p.isEmpty)
    }

    @Test("encodeErrorResponse with key not in protocol falls back to string union")
    func errorResponseUnknownKey() async throws {
        let (_, session, avro) = makeSession()
        let client = try AvroIPCRequest(clientHash: clientHash,
                                          clientProtocol: supportProtocol, session: session)
        let server = AvroIPCResponse(serverHash: serverHash, serverProtocol: supportProtocol)

        try await session.serverCache.add(hash: clientHash, protocolString: supportProtocol)
        try await session.clientCache.add(hash: serverHash, protocolString: supportProtocol)

        let header = HandshakeRequest(clientHash: clientHash, clientProtocol: supportProtocol,
                                      serverHash: serverHash, meta: [:])
        // The "Curse" key matches the schema; the "Mystery" key does NOT and
        // should hit the string-fallback branch.
        struct Wrapper: Codable, Equatable { var message: String }
        let errors = ["Mystery": Wrapper(message: "fallback string")]
        let data = try await server.encodeErrorResponse(
            avro: avro, header: header, messageName: "hello",
            errors: errors, session: session
        )
        #expect(data.count > 0)
        // Decode the response and verify a string came back through the union
        // index-0 branch.
        let (resHeader, parts): (ResponseHeader, [String]) =
            try await client.decodeResponse(avro: avro, messageName: "hello",
                                              from: data, serverHash: serverHash, session: session)
        #expect(resHeader.flag)
        // The fallback path encodes a string; after decode it's a String.
        #expect(parts.first?.contains("fallback") == true)
    }
}
