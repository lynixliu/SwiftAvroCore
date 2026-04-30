import Foundation
import Testing
@testable import SwiftAvroCore

// MARK: - AvroProtocol mutation coverage

@Suite("AvroProtocol – mutation paths")
struct AvroProtocolMutationTests {

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

    @Test("addType is no-op for already-present schema")
    func addTypeDuplicate() throws {
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

    @Test("addMessage with valid types stores message")
    func addMessageStores() throws {
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

    @Test("Message.validate true when types match; false otherwise")
    func messageValidate() throws {
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
}

// MARK: - Response.swift / Request.swift gap coverage

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

private func makeSchemas(avro: Avro = Avro())
    -> (req: AvroSchema, resp: AvroSchema, meta: AvroSchema)
{
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
    let meta = avro.newSchema(schema: #"{"type":"map","values":"bytes"}"#)!
    return (req, resp, meta)
}

private func makeContext(requestMeta: [String: [UInt8]] = [:])
    -> (context: AvroIPCContext, session: AvroIPCSession, avro: Avro)
{
    let avro = Avro()
    let (req, resp, meta) = makeSchemas(avro: avro)
    let ctx = AvroIPCContext(requestSchema: req, responseSchema: resp, metaSchema: meta,
                             requestMeta: requestMeta, responseMeta: [:],
                             knownProtocols: nil)
    return (ctx, AvroIPCSession(context: ctx), avro)
}

@Suite("Avro IPC gap – Response & Request paths")
struct IPCResponseRequestGapTests {

    @Test("decodeCall throws missingSchema for an unknown message name")
    func decodeCallUnknownMessage() async throws {
        let (_, session, avro) = makeContext()
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
        let (_, session, avro) = makeContext()
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
        let (_, session, avro) = makeContext()
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
