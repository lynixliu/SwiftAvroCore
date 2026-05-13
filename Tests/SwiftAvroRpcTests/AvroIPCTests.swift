//
//  SwiftAvroRpcTests.swift
//  SwiftAvroRpcTests
//
//  Created by Yang Liu on 06/04/2026.
//

import Testing
import Foundation
@testable import SwiftAvroCore
@testable import SwiftAvroRpc

// MARK: - V2 protocol (schema evolution tests only)

/// V2 server protocol: Greeting gains `language` (default "en") for request evolution tests.
private let helloProtocolV2 = """
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "types": [
    {"name": "Greeting", "type": "record", "fields": [
      {"name": "message",  "type": "string"},
      {"name": "language", "type": "string", "default": "en"}
    ]},
    {"name": "Curse", "type": "error", "fields": [{"name": "message", "type": "string"}]}
  ],
  "messages": {
    "hello": {
      "request":  [{"name": "greeting", "type": "Greeting"}],
      "response": "Greeting",
      "errors":   ["Curse"]
    }
  }
}
"""

private struct GreetingV2: Codable, Sendable, Equatable {
    var message:  String
    var language: String
}

// MARK: - SwiftAvroRpc lifecycle tests

@Suite("SwiftAvroRpc lifecycle")
struct SwiftAvroRpcLifecycleTests {

    @Test("Init creates a running instance")
    func initCreatesRunningInstance() async throws {
        let rpc = SwiftAvroRpc(threads: 1)
        _ = try await rpc.makeContainer(schema: """
            {"type":"record","name":"X","fields":[{"name":"a","type":"int"}]}
            """)
        try await rpc.stop()
    }

    @Test("stop() is idempotent")
    func stopIsIdempotent() async throws {
        let rpc = SwiftAvroRpc(threads: 1)
        try await rpc.stop()
        try await rpc.stop()
    }

    @Test("Factory methods throw after stop()")
    func factoryThrowsAfterStop() async throws {
        let rpc = SwiftAvroRpc(threads: 1)
        try await rpc.stop()
        await #expect(throws: (any Error).self) {
            try await rpc.makeContainer(schema: """
                {"type":"record","name":"X","fields":[{"name":"a","type":"int"}]}
                """)
        }
    }

    @Test("makeIPCContext returns context with correct schema names")
    func makeIPCContextSchemaNames() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        #expect(context.requestSchema.getName()  == "HandshakeRequest")
        #expect(context.responseSchema.getName() == "HandshakeResponse")
        #expect(context.metaSchema.getTypeName() == "map")
        try await rpc.stop()
    }
}

// MARK: - SwiftAvroRpc server tests

@Suite("SwiftAvroRpc server")
struct SwiftAvroRpcServerTests {

    @Test("makeServer binds and reports a local address")
    func makeServerBinds() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()

        let config = AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        )

        let server = try await rpc.makeServer(config)
        #expect(server.localAddress != nil)

        try await server.close()
        try await rpc.stop()
    }

    @Test("makeServer throws after stop()")
    func makeServerThrowsAfterStop() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        try await rpc.stop()

        let config = AvroIPCServerConfig(
            transport: TCPTransport(port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        )
        await #expect(throws: (any Error).self) {
            try await rpc.makeServer(config)
        }
    }

    @Test("Two servers can bind on different ports simultaneously")
    func twoServersOnDifferentPorts() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let s1 = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        ))
        let s2 = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        ))

        #expect(s1.localAddress != s2.localAddress)

        try await s1.close()
        try await s2.close()
        try await rpc.stop()
    }
}

// MARK: - SwiftAvroRpc client tests

@Suite("SwiftAvroRpc client")
struct SwiftAvroRpcClientTests {

    @Test("makeClient connects successfully")
    func makeClientConnects() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: port),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        ))

        try await client.disconnect()
        try await server.close()
        try await rpc.stop()
    }

    @Test("makeClient throws after stop()")
    func makeClientThrowsAfterStop() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        try await rpc.stop()

        await #expect(throws: (any Error).self) {
            try await rpc.makeClient(AvroIPCClientConfig(
                transport: TCPTransport(host: "127.0.0.1", port: 9999),
                context: context,
                clientHash: testClientHash,
                clientProtocol: helloProtocol,
                serverHash: testServerHash
            ))
        }
    }

    @Test("disconnect is safe to call multiple times")
    func disconnectIsIdempotent() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: port),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        ))

        try await client.disconnect()
        try await client.disconnect()

        try await server.close()
        try await rpc.stop()
    }
}

// MARK: - SwiftAvroRpc end-to-end tests

@Suite("SwiftAvroRpc end-to-end")
struct SwiftAvroRpcEndToEndTests {

    @Test("Client and server complete handshake")
    func handshakeCompletes() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: GreetingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: port),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        ))

        let response: Greeting = try await client.call(
            messageName: "hello",
            parameters: [Greeting(message: "hi")],
            as: Greeting.self
        )
        #expect(response.message == "hello back")

        try await client.disconnect()
        try await server.close()
        try await rpc.stop()
    }

    @Test("Multiple sequential calls on the same connection succeed")
    func multipleSequentialCalls() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: GreetingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: port),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        ))

        for i in 0..<5 {
            let response: Greeting = try await client.call(
                messageName: "hello",
                parameters: [Greeting(message: "call-\(i)")],
                as: Greeting.self
            )
            #expect(response.message == "hello back")
        }

        try await client.disconnect()
        try await server.close()
        try await rpc.stop()
    }

    @Test("Multiple clients connect to the same server")
    func multipleClientsOneServer() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: GreetingHandler()
        ))

        let port = extractPort(from: server.localAddress)

        let clients = try await withThrowingTaskGroup(of: AvroIPCClient.self) { group in
            for _ in 0..<3 {
                group.addTask {
                    try await rpc.makeClient(AvroIPCClientConfig(
                        transport: TCPTransport(host: "127.0.0.1", port: port),
                        context: context,
                        clientHash: testClientHash,
                        clientProtocol: helloProtocol,
                        serverHash: testServerHash
                    ))
                }
            }
            var result: [AvroIPCClient] = []
            for try await client in group { result.append(client) }
            return result
        }

        for client in clients {
            let response: Greeting = try await client.call(
                messageName: "hello",
                parameters: [Greeting(message: "ping")],
                as: Greeting.self
            )
            #expect(response.message == "hello back")
            try await client.disconnect()
        }

        try await server.close()
        try await rpc.stop()
    }

    @Test("Server handler error closes connection gracefully")
    func serverHandlerErrorClosesConnection() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: FailingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: port),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        ))

        await #expect(throws: (any Error).self) {
            try await client.call(
                messageName: "hello",
                parameters: [Greeting(message: "will fail")],
                as: Greeting.self
            )
        }

        try await server.close()
        try await rpc.stop()
    }
}

// MARK: - AvroIPCServerConfig tests

@Suite("AvroIPCServerConfig")
struct AvroIPCServerConfigTests {

    @Test("Default host is 0.0.0.0")
    func defaultHost() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCServerConfig(
            transport: TCPTransport(port: 9090),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        )
        let tcp = config.transport as? TCPTransport
        #expect(tcp?.host == "0.0.0.0")
        try await rpc.stop()
    }

    @Test("All properties are stored correctly")
    func propertiesStoredCorrectly() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 9090),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocol,
            handler: EchoHandler()
        )
        let tcp = config.transport as? TCPTransport
        #expect(tcp?.host           == "127.0.0.1")
        #expect(tcp?.port           == 9090)
        #expect(config.serverHash     == testServerHash)
        #expect(config.serverProtocol == helloProtocol)
        try await rpc.stop()
    }
}

// MARK: - AvroIPCClientConfig tests

@Suite("AvroIPCClientConfig")
struct AvroIPCClientConfigTests {

    @Test("All properties are stored correctly")
    func propertiesStoredCorrectly() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 9090),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        )
        let tcp = config.transport as? TCPTransport
        #expect(tcp?.host           == "127.0.0.1")
        #expect(tcp?.port           == 9090)
        #expect(config.clientHash     == testClientHash)
        #expect(config.clientProtocol == helloProtocol)
        #expect(config.serverHash     == testServerHash)
        try await rpc.stop()
    }
}

// MARK: - IPC request schema evolution tests

/// Handler that requires schema evolution: decodes with writer+reader schemas and
/// echoes the language field back in the response so the test can verify evolution.
private struct EvolvingGreetingHandler: AvroIPCHandler {
    func handle(messageName: String, requestData: Data) async throws -> Data {
        throw AvroIPCError.noHandler("EvolvingGreetingHandler requires schema context")
    }

    func handle(
        messageName:   String,
        requestData:   Data,
        writerSchemas: [AvroSchema],
        readerSchemas: [AvroSchema]
    ) async throws -> Data {
        let avro   = Avro()
        let reader = avro.makeDataReader(data: requestData)
        let g: GreetingV2 = try reader.decode(
            writerSchema: writerSchemas[0],
            readerSchema: readerSchemas[0]
        )
        let v2ResponseSchema = avro.newSchema(schema: """
            {"type":"record","name":"Greeting","namespace":"com.acme",
             "fields":[{"name":"message","type":"string"},
                       {"name":"language","type":"string","default":"en"}]}
            """)!
        return try avro.encodeFrom(
            GreetingV2(message: "\(g.message):\(g.language)", language: g.language),
            schema: v2ResponseSchema
        )
    }
}

@Suite("SwiftAvroRpc IPC request evolution")
struct SwiftAvroRpcIPCRequestEvolutionTests {

    @Test("Client V1 schema, server V2 schema — language default filled via evolution")
    func requestEvolution() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport: TCPTransport(host: "127.0.0.1", port: 0),
            context: context,
            serverHash: testServerHash,
            serverProtocol: helloProtocolV2,
            handler: EvolvingGreetingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport: TCPTransport(host: "127.0.0.1", port: port),
            context: context,
            clientHash: testClientHash,
            clientProtocol: helloProtocol,
            serverHash: testServerHash
        ))

        let response: Greeting = try await client.call(
            messageName: "hello",
            parameters:  [Greeting(message: "hi")],
            as:          Greeting.self
        )
        #expect(response.message == "hi:en")

        try await client.disconnect()
        try await server.close()
        try await rpc.stop()
    }
}
