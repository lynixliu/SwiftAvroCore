//
//  AvroIPCHTTPTests.swift
//  SwiftAvroRpcTests
//
//  Created by Yang Liu on 09/05/2026.
//

import Testing
import Foundation
@testable import SwiftAvroCore
@testable import SwiftAvroRpc

// MARK: - AvroIPCHTTPServerConfig tests

@Suite("AvroIPCHTTPServerConfig")
struct AvroIPCHTTPServerConfigTests {

    @Test("Default host is 0.0.0.0 and path is /")
    func defaultHostAndPath() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCHTTPServerConfig(
            port:           9090,
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        EchoHandler()
        )
        #expect(config.host == "0.0.0.0")
        #expect(config.path == "/")
        #expect(config.tls  == nil)
        try await rpc.stop()
    }

    @Test("All properties are stored correctly")
    func propertiesStoredCorrectly() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCHTTPServerConfig(
            host:           "127.0.0.1",
            port:           8080,
            path:           "/avro",
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        EchoHandler()
        )
        #expect(config.host           == "127.0.0.1")
        #expect(config.port           == 8080)
        #expect(config.path           == "/avro")
        #expect(config.serverHash     == testServerHash)
        #expect(config.serverProtocol == helloProtocol)
        try await rpc.stop()
    }
}

// MARK: - AvroIPCHTTPClientConfig tests

@Suite("AvroIPCHTTPClientConfig")
struct AvroIPCHTTPClientConfigTests {

    @Test("Default path is /")
    func defaultPath() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCHTTPClientConfig(
            host:           "127.0.0.1",
            port:           8080,
            context:        context,
            clientHash:     testClientHash,
            clientProtocol: helloProtocol,
            serverHash:     testServerHash
        )
        #expect(config.path == "/")
        #expect(config.tls  == nil)
        try await rpc.stop()
    }

    @Test("All properties are stored correctly")
    func propertiesStoredCorrectly() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        let config  = AvroIPCHTTPClientConfig(
            host:           "127.0.0.1",
            port:           8080,
            path:           "/avro",
            context:        context,
            clientHash:     testClientHash,
            clientProtocol: helloProtocol,
            serverHash:     testServerHash
        )
        #expect(config.host           == "127.0.0.1")
        #expect(config.port           == 8080)
        #expect(config.path           == "/avro")
        #expect(config.clientHash     == testClientHash)
        #expect(config.clientProtocol == helloProtocol)
        #expect(config.serverHash     == testServerHash)
        try await rpc.stop()
    }
}

// MARK: - HTTP server lifecycle tests

@Suite("HTTP server lifecycle")
struct HTTPServerLifecycleTests {

    @Test("makeHTTPServer binds and reports a local address")
    func makeHTTPServerBinds() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeHTTPServer(AvroIPCHTTPServerConfig(
            host:           "127.0.0.1",
            port:           0,
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        EchoHandler()
        ))
        #expect(server.localAddress != nil)

        try await server.close()
        try await rpc.stop()
    }

    @Test("makeHTTPServer throws after stop()")
    func makeHTTPServerThrowsAfterStop() async throws {
        let rpc     = SwiftAvroRpc(threads: 1)
        let context = try await rpc.makeIPCContext()
        try await rpc.stop()

        await #expect(throws: (any Error).self) {
            try await rpc.makeHTTPServer(AvroIPCHTTPServerConfig(
                port:           0,
                context:        context,
                serverHash:     testServerHash,
                serverProtocol: helloProtocol,
                handler:        EchoHandler()
            ))
        }
    }
}

// MARK: - HTTP end-to-end tests

@Suite("HTTP IPC end-to-end")
struct HTTPIPCEndToEndTests {

    @Test("HTTP client and server complete handshake and call")
    func handshakeAndCall() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeHTTPServer(AvroIPCHTTPServerConfig(
            host:           "127.0.0.1",
            port:           0,
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        GreetingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeHTTPClient(AvroIPCHTTPClientConfig(
            host:           "127.0.0.1",
            port:           port,
            context:        context,
            clientHash:     testClientHash,
            clientProtocol: helloProtocol,
            serverHash:     testServerHash
        ))

        let response: Greeting = try await client.call(
            messageName: "hello",
            parameters:  [Greeting(message: "hi")],
            as:          Greeting.self
        )
        #expect(response.message == "hello back")

        try await server.close()
        try await rpc.stop()
    }

    @Test("Multiple sequential HTTP calls succeed")
    func multipleSequentialCalls() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeHTTPServer(AvroIPCHTTPServerConfig(
            host:           "127.0.0.1",
            port:           0,
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        GreetingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeHTTPClient(AvroIPCHTTPClientConfig(
            host:           "127.0.0.1",
            port:           port,
            context:        context,
            clientHash:     testClientHash,
            clientProtocol: helloProtocol,
            serverHash:     testServerHash
        ))

        for i in 0..<5 {
            let response: Greeting = try await client.call(
                messageName: "hello",
                parameters:  [Greeting(message: "call-\(i)")],
                as:          Greeting.self
            )
            #expect(response.message == "hello back")
        }

        try await server.close()
        try await rpc.stop()
    }

    @Test("Multiple HTTP clients share the server session cache")
    func multipleClientsShareServerSessionCache() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeHTTPServer(AvroIPCHTTPServerConfig(
            host:           "127.0.0.1",
            port:           0,
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        GreetingHandler()
        ))
        let port = extractPort(from: server.localAddress)

        // Three independent HTTP clients — each performs its own handshake.
        // The shared AvroIPCServer actor accumulates the session cache across them.
        let clients = try await withThrowingTaskGroup(of: AvroIPCHTTPClient.self) { group in
            for _ in 0..<3 {
                group.addTask {
                    try await rpc.makeHTTPClient(AvroIPCHTTPClientConfig(
                        host:           "127.0.0.1",
                        port:           port,
                        context:        context,
                        clientHash:     testClientHash,
                        clientProtocol: helloProtocol,
                        serverHash:     testServerHash
                    ))
                }
            }
            var result: [AvroIPCHTTPClient] = []
            for try await client in group { result.append(client) }
            return result
        }

        for client in clients {
            let response: Greeting = try await client.call(
                messageName: "hello",
                parameters:  [Greeting(message: "ping")],
                as:          Greeting.self
            )
            #expect(response.message == "hello back")
        }

        try await server.close()
        try await rpc.stop()
    }

    @Test("HTTP server handler error returns server-error to client")
    func serverHandlerErrorReturnsError() async throws {
        let rpc     = SwiftAvroRpc(threads: 2)
        let context = try await rpc.makeIPCContext()

        let server = try await rpc.makeHTTPServer(AvroIPCHTTPServerConfig(
            host:           "127.0.0.1",
            port:           0,
            context:        context,
            serverHash:     testServerHash,
            serverProtocol: helloProtocol,
            handler:        FailingHandler()
        ))

        let port   = extractPort(from: server.localAddress)
        let client = try await rpc.makeHTTPClient(AvroIPCHTTPClientConfig(
            host:           "127.0.0.1",
            port:           port,
            context:        context,
            clientHash:     testClientHash,
            clientProtocol: helloProtocol,
            serverHash:     testServerHash
        ))

        await #expect(throws: (any Error).self) {
            try await client.call(
                messageName: "hello",
                parameters:  [Greeting(message: "will fail")],
                as:          Greeting.self
            )
        }

        try await server.close()
        try await rpc.stop()
    }
}
