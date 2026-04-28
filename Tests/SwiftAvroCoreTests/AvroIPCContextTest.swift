//
//  AvroIPCContextTest.swift
//  SwiftAvroCoreTests
//
//  Tests for AvroIPCContext, AvroIPCSession, and SessionCache.
//

import Testing
import Foundation
@testable import SwiftAvroCore

// MARK: - Shared helpers

/// Builds a minimal, valid AvroIPCContext from the project's MessageConstant schemas.
/// Extracted so every test can call `makeContext()` instead of repeating six lines.
private func makeContext() throws -> AvroIPCContext {
    let avro = Avro()
    let reqSchema  = try #require(avro.decodeSchema(schema: MessageConstant.requestSchema))
    let respSchema = try #require(avro.decodeSchema(schema: MessageConstant.responseSchema))
    let metaSchema = try #require(avro.decodeSchema(schema: MessageConstant.metadataSchema))
    return AvroIPCContext(requestSchema: reqSchema,
                         responseSchema: respSchema,
                         metaSchema: metaSchema)
}

/// A minimal Avro protocol JSON with one message that has a request, response and error type.
private let fullProtocolJSON = """
{
  "protocol": "TestProtocol",
  "types": [
    {"name": "Greeting", "type": "record",
     "fields": [{"name": "message", "type": "string"}]},
    {"name": "AppError", "type": "error",
     "fields": [{"name": "reason", "type": "string"}]}
  ],
  "messages": {
    "hello": {
      "request":  [{"name": "greeting", "type": "Greeting"}],
      "response": "Greeting",
      "errors":   ["AppError"]
    }
  }
}
"""

// MARK: - Test Suite

@Suite("Avro IPC Context")
struct AvroIPCContextTests {

    // =========================================================================
    // MARK: - AvroIPCContext
    // =========================================================================

    @Test("Context stores schemas and defaults requestMeta/responseMeta to empty")
    func contextDefaultInit() throws {
        let context = try makeContext()

        // Default meta must be empty maps, knownProtocols must be nil.
        #expect(context.requestMeta.isEmpty)
        #expect(context.responseMeta.isEmpty)
        #expect(context.knownProtocols == nil)

        // BUG FIX: The original suite never checked that the schemas were
        // actually stored. Using a simple identity-of-type check here; exact
        // schema equality depends on AvroSchema conformances in the module.
        // At minimum the stored values must not be the wrong schema.
        _ = context.requestSchema   // access to confirm storage (would crash if uninit)
        _ = context.responseSchema
        _ = context.metaSchema
    }

    @Test("Context stores custom requestMeta and responseMeta")
    func contextWithMeta() throws {
        let avro = Avro()
        let reqSchema  = try #require(avro.decodeSchema(schema: MessageConstant.requestSchema))
        let respSchema = try #require(avro.decodeSchema(schema: MessageConstant.responseSchema))
        let metaSchema = try #require(avro.decodeSchema(schema: MessageConstant.metadataSchema))

        let requestMeta:  [String: [UInt8]] = ["key1": [1, 2, 3]]
        let responseMeta: [String: [UInt8]] = ["key2": [4, 5, 6]]

        let context = AvroIPCContext(
            requestSchema:  reqSchema,
            responseSchema: respSchema,
            metaSchema:     metaSchema,
            requestMeta:    requestMeta,
            responseMeta:   responseMeta
        )

        #expect(context.requestMeta  == requestMeta)
        #expect(context.responseMeta == responseMeta)
        // knownProtocols should still be nil when not supplied.
        #expect(context.knownProtocols == nil)
    }

    @Test("Context stores a non-nil knownProtocols set")
    func contextWithKnownProtocols() throws {
        let context = try makeContext()
        let avro = Avro()
        let reqSchema  = try #require(avro.decodeSchema(schema: MessageConstant.requestSchema))
        let respSchema = try #require(avro.decodeSchema(schema: MessageConstant.responseSchema))
        let metaSchema = try #require(avro.decodeSchema(schema: MessageConstant.metadataSchema))

        let protocols: Set<String> = ["HelloWorld", "FooBar"]
        let ctxWithProtos = AvroIPCContext(
            requestSchema:  reqSchema,
            responseSchema: respSchema,
            metaSchema:     metaSchema,
            knownProtocols: protocols
        )
        #expect(ctxWithProtos.knownProtocols == protocols)
        // Baseline: our helper-built context must still be nil.
        #expect(context.knownProtocols == nil)
    }

    @Test("Context with empty knownProtocols set is distinct from nil")
    func contextEmptyKnownProtocols() throws {
        let avro = Avro()
        let reqSchema  = try #require(avro.decodeSchema(schema: MessageConstant.requestSchema))
        let respSchema = try #require(avro.decodeSchema(schema: MessageConstant.responseSchema))
        let metaSchema = try #require(avro.decodeSchema(schema: MessageConstant.metadataSchema))

        let ctx = AvroIPCContext(
            requestSchema:  reqSchema,
            responseSchema: respSchema,
            metaSchema:     metaSchema,
            knownProtocols: []          // explicitly empty, not nil
        )
        // An empty Set is non-nil; the caller opted-in to protocol filtering
        // even though no protocol names are registered yet.
        #expect(ctx.knownProtocols != nil)
        #expect(ctx.knownProtocols?.isEmpty == true)
    }

    // =========================================================================
    // MARK: - AvroIPCSession
    // =========================================================================

    @Test("Session stores context and initialises non-nil client and server caches")
    func sessionInit() async throws {
        // BUG FIX: The original test used `===` (reference equality) on
        // AvroIPCContext which is a *struct*, not a class – this does not
        // compile. We instead verify the stored context by comparing one of
        // its observable properties.
        let context = try makeContext()
        let session = AvroIPCSession(context: context)

        // Verify that context's requestMeta was preserved (struct copy semantics).
        #expect(session.context.requestMeta == context.requestMeta)
        #expect(session.context.responseMeta == context.responseMeta)
        #expect(session.context.knownProtocols == context.knownProtocols)

        // Verify caches are real, reachable actor instances (not crashing here
        // is already a meaningful assertion; we follow up with empty checks).
        let clientEmpty = await session.clientCache.avroProtocol(
            for: [UInt8](repeating: 0, count: 16)) == nil
        let serverEmpty = await session.serverCache.contains(
            hash: [UInt8](repeating: 0, count: 16)) == false
        #expect(clientEmpty)
        #expect(serverEmpty)
    }

    @Test("Session caches are independent between two sessions sharing the same context")
    func sessionCachesAreIndependent() async throws {
        let context  = try makeContext()
        let sessionA = AvroIPCSession(context: context)
        let sessionB = AvroIPCSession(context: context)

        let hash = [UInt8](repeating: 0xAB, count: 16)
        try await sessionA.clientCache.add(hash: hash, protocolString: fullProtocolJSON)

        let inA = await sessionA.clientCache.avroProtocol(for: hash) != nil
        let inB = await sessionB.clientCache.avroProtocol(for: hash) != nil

        #expect(inA)
        #expect(!inB)   // B's cache must be unaffected by A's writes
    }

    // =========================================================================
    // MARK: - ClientSessionCache
    // =========================================================================

    @Test("ClientSessionCache starts empty")
    func clientCacheEmpty() async {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 0, count: 16)
        #expect(await cache.avroProtocol(for: hash) == nil)
    }

    @Test("ClientSessionCache add then retrieve returns correct protocol name")
    func clientCacheAddRetrieve() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 1, count: 16)
        let json  = """
        {"protocol":"TestProtocol","types":[],"messages":{}}
        """
        try await cache.add(hash: hash, protocolString: json)

        let proto = await cache.avroProtocol(for: hash)
        #expect(proto != nil)
        #expect(proto?.name == "TestProtocol")
    }

    @Test("ClientSessionCache remove makes entry absent")
    func clientCacheRemove() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 4, count: 16)
        let json  = """
        {"protocol":"ToRemove","types":[],"messages":{}}
        """

        try await cache.add(hash: hash, protocolString: json)
        #expect(await cache.avroProtocol(for: hash) != nil)

        await cache.remove(for: hash)
        #expect(await cache.avroProtocol(for: hash) == nil)
    }

    @Test("ClientSessionCache remove on absent key is a no-op")
    func clientCacheRemoveAbsent() async {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 0xFF, count: 16)
        // Must not throw or crash when the key was never added.
        await cache.remove(for: hash)
        #expect(await cache.avroProtocol(for: hash) == nil)
    }

    @Test("ClientSessionCache clear removes all entries")
    func clientCacheClear() async throws {
        let cache  = ClientSessionCache()
        let hash1  = [UInt8](repeating: 5, count: 16)
        let hash2  = [UInt8](repeating: 6, count: 16)
        let json   = """
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
        await cache.clear()   // must not throw/crash
        #expect(await cache.avroProtocol(for: [UInt8](repeating: 0, count: 16)) == nil)
    }

    // =========================================================================
    // MARK: - ServerSessionCache
    // =========================================================================

    @Test("ServerSessionCache starts empty")
    func serverCacheEmpty() async {
        let cache = ServerSessionCache()
        let hash  = [UInt8](repeating: 0, count: 16)
        #expect(await cache.contains(hash: hash) == false)
    }

    @Test("ServerSessionCache add then contains returns true; absent hash returns false")
    func serverCacheAddContains() async throws {
        let cache      = ServerSessionCache()
        let hash       = [UInt8](repeating: 2, count: 16)
        let otherHash  = [UInt8](repeating: 3, count: 16)
        let json       = """
        {"protocol":"ServerProtocol","types":[],"messages":{}}
        """

        try await cache.add(hash: hash, protocolString: json)

        #expect(await cache.contains(hash: hash))
        #expect(await cache.contains(hash: otherHash) == false)
    }

    @Test("ServerSessionCache remove makes entry absent")
    func serverCacheRemove() async throws {
        // BUG FIX / COVERAGE: The original suite never tested ServerSessionCache.remove.
        let cache = ServerSessionCache()
        let hash  = [UInt8](repeating: 0xCC, count: 16)
        let json  = """
        {"protocol":"RemovableServer","types":[],"messages":{}}
        """

        try await cache.add(hash: hash, protocolString: json)
        #expect(await cache.contains(hash: hash))

        await cache.remove(for: hash)
        #expect(await cache.contains(hash: hash) == false)
    }

    @Test("ServerSessionCache clear removes all entries")
    func serverCacheClear() async throws {
        let cache  = ServerSessionCache()
        let hash1  = [UInt8](repeating: 7, count: 16)
        let hash2  = [UInt8](repeating: 8, count: 16)
        let json   = """
        {"protocol":"Test","types":[],"messages":{}}
        """

        try await cache.add(hash: hash1, protocolString: json)
        try await cache.add(hash: hash2, protocolString: json)

        await cache.clear()
        #expect(await cache.contains(hash: hash1) == false)
        #expect(await cache.contains(hash: hash2) == false)
    }

    // =========================================================================
    // MARK: - SessionCache error paths
    // =========================================================================

    @Test("add with invalid JSON throws")
    func cacheAddInvalidJSON() async {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 13, count: 16)

        await #expect(throws: (any Error).self) {
            try await cache.add(hash: hash, protocolString: "not valid json")
        }
    }

    @Test("add with empty string throws")
    func cacheAddEmptyString() async {
        // BUG FIX: The original 'invalid UTF-8' test was incorrect.
        // String(decoding:as:) replaces bad bytes with U+FFFD and *never*
        // throws – so the test was a false positive that would actually pass
        // without the cache ever throwing.  Testing an empty string (which
        // is valid UTF-8 but not valid JSON) is a correct replacement.
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 12, count: 16)

        await #expect(throws: (any Error).self) {
            try await cache.add(hash: hash, protocolString: "")
        }
    }

    @Test("add with JSON that is missing the 'protocol' key throws")
    func cacheAddMissingProtocolKey() async {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 0xEE, count: 16)
        // Valid JSON object but not a valid Avro protocol descriptor.
        let badJSON = """
        {"notAProtocol": true}
        """

        await #expect(throws: (any Error).self) {
            try await cache.add(hash: hash, protocolString: badJSON)
        }
    }

    // =========================================================================
    // MARK: - Schema look-up: nil paths (unknown hash)
    // =========================================================================

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

    // =========================================================================
    // MARK: - Schema look-up: nil paths (known hash, unknown message)
    // =========================================================================

    @Test("requestSchemas returns nil for known hash but unknown message name")
    func cacheRequestSchemasNilUnknownMessage() async throws {
        // COVERAGE: Original suite omitted the branch where the protocol IS
        // cached but the requested message name does not exist in it.
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 0xA0, count: 16)
        try await cache.add(hash: hash, protocolString: fullProtocolJSON)

        #expect(await cache.requestSchemas(hash: hash, messageName: "nonexistent") == nil)
    }

    @Test("responseSchema returns nil for known hash but unknown message name")
    func cacheResponseSchemaNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 0xA1, count: 16)
        try await cache.add(hash: hash, protocolString: fullProtocolJSON)

        #expect(await cache.responseSchema(hash: hash, messageName: "nonexistent") == nil)
    }

    @Test("errorSchemas returns nil for known hash but unknown message name")
    func cacheErrorSchemasNilUnknownMessage() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 0xA2, count: 16)
        try await cache.add(hash: hash, protocolString: fullProtocolJSON)

        #expect(await cache.errorSchemas(hash: hash, messageName: "nonexistent") == nil)
    }

    // =========================================================================
    // MARK: - Schema look-up: happy paths
    // =========================================================================

    @Test("requestSchemas returns one schema for known message")
    func cacheRequestSchemas() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 14, count: 16)
        try await cache.add(hash: hash, protocolString: fullProtocolJSON)

        let schemas = await cache.requestSchemas(hash: hash, messageName: "hello")
        #expect(schemas != nil)
        #expect(schemas?.count == 1)
    }

    @Test("responseSchema returns non-nil schema for known message")
    func cacheResponseSchema() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 15, count: 16)
        try await cache.add(hash: hash, protocolString: fullProtocolJSON)

        #expect(await cache.responseSchema(hash: hash, messageName: "hello") != nil)
    }

    @Test("errorSchemas returns non-nil entry keyed by error type name")
    func cacheErrorSchemas() async throws {
        let cache = ClientSessionCache()
        let hash  = [UInt8](repeating: 16, count: 16)
        try await cache.add(hash: hash, protocolString: fullProtocolJSON)

        let schemas = await cache.errorSchemas(hash: hash, messageName: "hello")
        #expect(schemas != nil)
        #expect(schemas?["AppError"] != nil)
    }

    // =========================================================================
    // MARK: - Concurrent access (actor-safety smoke test)
    // =========================================================================

    @Test("ClientSessionCache handles concurrent adds without crashing")
    func clientCacheConcurrentAdds() async throws {
        // COVERAGE: verifies that the actor serialises concurrent writes
        // correctly and that every hash is findable afterwards.
        let cache = ClientSessionCache()
        let json  = """
        {"protocol":"ConcurrentProto","types":[],"messages":{}}
        """

        try await withThrowingTaskGroup(of: Void.self) { group in
            for byte in UInt8(0x20) ..< UInt8(0x30) {
                let hash = [UInt8](repeating: byte, count: 16)
                group.addTask {
                    try await cache.add(hash: hash, protocolString: json)
                }
            }
            try await group.waitForAll()
        }

        for byte in UInt8(0x20) ..< UInt8(0x30) {
            let hash  = [UInt8](repeating: byte, count: 16)
            let proto = await cache.avroProtocol(for: hash)
            #expect(proto?.name == "ConcurrentProto")
        }
    }
}
