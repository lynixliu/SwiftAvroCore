//
//  AvroIPCTLSTests.swift
//  SwiftAvroRpcTests
//
//

import Testing
import Foundation
import NIOSSL
@testable import SwiftAvroCore
@testable import SwiftAvroRpc

// MARK: - Certificate generation

/// Generates temporary PEM files for a self-signed CA and a server cert
/// signed by that CA.  The temporary directory is removed when the closure
/// returns.
///
/// - Parameters passed to `body`: `(caPEM, serverCertPEM, serverKeyPEM)`.
private func withTemporaryCerts<T>(
    _ body: (String, String, String) async throws -> T
) async throws -> T {
    let tmp = FileManager.default.temporaryDirectory
        .appendingPathComponent("SwiftAvroRpcTLS-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: tmp) }

    let caKey  = tmp.appendingPathComponent("ca-key.pem").path
    let caCert = tmp.appendingPathComponent("ca-cert.pem").path
    let srvKey = tmp.appendingPathComponent("server-key.pem").path
    let srvCrt = tmp.appendingPathComponent("server-cert.pem").path
    let srvCSR = tmp.appendingPathComponent("server.csr").path

    // Runs openssl to completion; throws if it exits non-zero.
    func openssl(_ arguments: [String]) throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/openssl")
        process.arguments = arguments
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw NSError(domain: "openssl", code: Int(process.terminationStatus),
                          userInfo: [NSLocalizedDescriptionKey:
                            "openssl \(arguments.first ?? "") exited \(process.terminationStatus)"])
        }
    }

    // Generate self-signed CA.
    try openssl([
        "req", "-x509", "-new", "-newkey", "rsa:2048",
        "-keyout", caKey, "-out", caCert, "-days", "365",
        "-nodes", "-subj", "/CN=SwiftAvroRPCTestCA"
    ])

    // Generate server CSR + key.
    try openssl([
        "req", "-new", "-newkey", "rsa:2048",
        "-keyout", srvKey, "-out", srvCSR,
        "-nodes", "-subj", "/CN=127.0.0.1"
    ])

    // Sign server cert with the CA.
    try openssl([
        "x509", "-req", "-in", srvCSR,
        "-CA", caCert, "-CAkey", caKey, "-CAcreateserial",
        "-out", srvCrt, "-days", "365"
    ])

    return try await body(caCert, srvCrt, srvKey)
}

private func withSwiftAvroRpc<T>(
    threads: Int,
    _ body: (SwiftAvroRpc) async throws -> T
) async throws -> T {
    let rpc = SwiftAvroRpc(threads: threads)
    do {
        let result = try await body(rpc)
        try await rpc.stop()
        return result
    } catch {
        try? await rpc.stop()
        throw error
    }
}

/// Builds an `AvroTLSConfig` that trusts the given CA PEM file for
/// client-side use.
private func clientTLS(caPEM: String) throws -> AvroTLSConfig {
    let ca     = try NIOSSLCertificate.fromPEMFile(caPEM)
    var cfg    = TLSConfiguration.makeClientConfiguration()
    cfg.trustRoots              = .certificates(ca)
    cfg.certificateVerification = .none
    return AvroTLSConfig(sslContext: try NIOSSLContext(configuration: cfg))
}

// MARK: - AvroTLSConfig tests

@Suite("AvroTLSConfig")
struct AvroTLSConfigSuite {

    @Test("Server TLS from PEM files succeeds")
    func serverConfig() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            let tls = try AvroTLSConfig.server(
                certificateFile: srvCert,
                privateKeyFile:  srvKey
            )
            _ = tls.sslContext
        }
    }

    @Test("Client TLS succeeds")
    func clientConfig() throws {
        _ = try AvroTLSConfig.client().sslContext
    }
}

// MARK: - Secure server tests

@Suite("TLS IPC server")
struct TLSServerSuite {

    @Test("makeSecureServer binds and reports a local address")
    func secureServerBinds() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 1) { rpc in
                let context = try await rpc.makeIPCContext()
                let tls     = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let server  = try await rpc.makeSecureServer(
                    host: "127.0.0.1", port: 0, tls: tls,
                    context: context, serverHash: testServerHash,
                    serverProtocol: helloProtocol, handler: GreetingHandler()
                )
                #expect(server.localAddress != nil)
                try await server.close()
            }
        }
    }

    @Test("TLS on AvroIPCServerConfig stored correctly")
    func tlsInServerConfig() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 1) { rpc in
                let context = try await rpc.makeIPCContext()
                let tls     = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let config  = AvroIPCServerConfig(
                    transport: TCPTransport(host: "127.0.0.1", port: 0),
                    context: context, serverHash: testServerHash,
                    serverProtocol: helloProtocol, handler: EchoHandler(),
                    tls: tls
                )
                let tcp = config.transport as? TCPTransport
                #expect(tcp?.host == "127.0.0.1")
                #expect(config.tls  != nil)
                let server = try await rpc.makeServer(config)
                #expect(server.localAddress != nil)
                try await server.close()
            }
        }
    }
}

// MARK: - TLS IPC client tests

@Suite("TLS IPC client")
struct TLSClientSuite {

    @Test("makeSecureClient connects to TLS server")
    func secureClientConnects() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 2) { rpc in
                let context = try await rpc.makeIPCContext()

                let serverTLS = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let server = try await rpc.makeSecureServer(
                    host: "127.0.0.1", port: 0, tls: serverTLS,
                    context: context, serverHash: testServerHash,
                    serverProtocol: helloProtocol, handler: GreetingHandler()
                )
                let port = extractPort(from: server.localAddress)

                let clientTLS = try clientTLS(caPEM: caPEM)
                let client    = try await rpc.makeSecureClient(
                    host: "127.0.0.1", port: port, tls: clientTLS,
                    context: context, clientHash: testClientHash,
                    clientProtocol: helloProtocol, serverHash: testServerHash
                )

                try await client.disconnect()
                try await server.close()
            }
        }
    }

    @Test("TLS on AvroIPCClientConfig stored correctly")
    func tlsInClientConfig() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 1) { rpc in
                let context = try await rpc.makeIPCContext()
                let tls     = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let config  = AvroIPCClientConfig(
                    transport: TCPTransport(host: "127.0.0.1", port: 9999),
                    context: context, clientHash: testClientHash,
                    clientProtocol: helloProtocol, serverHash: testServerHash,
                    tls: tls
                )
                let tcp = config.transport as? TCPTransport
                #expect(tcp?.host == "127.0.0.1")
                #expect(config.tls  != nil)
            }
        }
    }
}

// MARK: - TLS end-to-end tests

@Suite("TLS IPC end-to-end")
struct TLSEndToEndSuite {

    @Test("Handshake and RPC call succeed over TLS")
    func handshakeAndCall() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 2) { rpc in
                let context = try await rpc.makeIPCContext()

                let serverTLS = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let server = try await rpc.makeSecureServer(
                    host: "127.0.0.1", port: 0, tls: serverTLS,
                    context: context, serverHash: testServerHash,
                    serverProtocol: helloProtocol, handler: GreetingHandler()
                )
                let port = extractPort(from: server.localAddress)

                let clientTLS = try clientTLS(caPEM: caPEM)
                let client    = try await rpc.makeSecureClient(
                    host: "127.0.0.1", port: port, tls: clientTLS,
                    context: context, clientHash: testClientHash,
                    clientProtocol: helloProtocol, serverHash: testServerHash
                )

                let response: Greeting = try await client.call(
                    messageName: "hello",
                    parameters: [Greeting(message: "secure hi")],
                    as: Greeting.self
                )
                #expect(response.message == "hello back")

                try await client.disconnect()
                try await server.close()
            }
        }
    }

    @Test("Multiple sequential calls over TLS succeed")
    func multipleSequentialCalls() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 2) { rpc in
                let context = try await rpc.makeIPCContext()

                let serverTLS = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let server = try await rpc.makeSecureServer(
                    host: "127.0.0.1", port: 0, tls: serverTLS,
                    context: context, serverHash: testServerHash,
                    serverProtocol: helloProtocol, handler: GreetingHandler()
                )
                let port = extractPort(from: server.localAddress)

                let clientTLS = try clientTLS(caPEM: caPEM)
                let client    = try await rpc.makeSecureClient(
                    host: "127.0.0.1", port: port, tls: clientTLS,
                    context: context, clientHash: testClientHash,
                    clientProtocol: helloProtocol, serverHash: testServerHash
                )

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
            }
        }
    }

    @Test("Server handler error over TLS closes connection gracefully")
    func serverHandlerError() async throws {
        try await withTemporaryCerts { caPEM, srvCert, srvKey in
            try await withSwiftAvroRpc(threads: 2) { rpc in
                let context = try await rpc.makeIPCContext()

                let serverTLS = try AvroTLSConfig.server(
                    certificateFile: srvCert,
                    privateKeyFile:  srvKey
                )
                let server = try await rpc.makeSecureServer(
                    host: "127.0.0.1", port: 0, tls: serverTLS,
                    context: context, serverHash: testServerHash,
                    serverProtocol: helloProtocol, handler: FailingHandler()
                )
                let port = extractPort(from: server.localAddress)

                let clientTLS = try clientTLS(caPEM: caPEM)
                let client    = try await rpc.makeSecureClient(
                    host: "127.0.0.1", port: port, tls: clientTLS,
                    context: context, clientHash: testClientHash,
                    clientProtocol: helloProtocol, serverHash: testServerHash
                )

                await #expect(throws: (any Error).self) {
                    try await client.call(
                        messageName: "hello",
                        parameters: [Greeting(message: "will fail")],
                        as: Greeting.self
                    )
                }

                try await server.close()
            }
        }
    }
}
