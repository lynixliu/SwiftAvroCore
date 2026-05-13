//
//  Service/ServiceClient.swift
//  SwiftAvroRpc
//
//  Copyright © 2026 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation

/// Discovers a service via a ``ServiceCatalog`` and calls it via Avro IPC.
///
/// `ServiceClient` owns the connection pool keyed by endpoint, delegates
/// endpoint selection to ``LoadBalancer``, and handles both TCP and Unix
/// domain socket connections transparently through the transport abstraction.
public actor ServiceClient {

    private let rpc:          SwiftAvroRpc
    private let catalogue:    any ServiceCatalog
    private let loadBalancer: LoadBalancer
    private var pool:         [Endpoint: AvroIPCClient] = [:]

    public init(catalogue: any ServiceCatalog) {
        self.rpc          = SwiftAvroRpc(threads: 1)
        self.catalogue    = catalogue
        self.loadBalancer = LoadBalancer()
    }

    /// Calls a named message on the best available endpoint for the service.
    ///
    /// - Parameters:
    ///   - serviceName: Logical name used at registration (e.g. `"greeter"`).
    ///   - clientProtocol: Avro protocol JSON string this client speaks.
    ///   - messageName: Message name as declared in the protocol.
    ///   - parameters: Encoded request parameters.
    ///   - responseType: Expected `Codable` response type.
    public func call<Req: Codable & Sendable, Res: Codable & Sendable>(
        serviceName:    String,
        clientProtocol: String,
        messageName:    String,
        parameters:     [Req],
        as responseType: Res.Type
    ) async throws -> Res {
        let candidates = try await catalogue.discover(serviceName: serviceName)
        guard let info = await loadBalancer.select(serviceName: serviceName, from: candidates) else {
            throw ServiceClientError.noEndpointAvailable(serviceName)
        }
        let client = try await connection(to: info.endpoint, clientProtocol: clientProtocol)
        return try await client.call(
            messageName: messageName,
            parameters:  parameters,
            as:          responseType
        )
    }

    /// Disconnects all pooled connections and stops the event loop group.
    public func shutdown() async throws {
        for client in pool.values { try await client.disconnect() }
        try await rpc.stop()
    }

    // MARK: - Private

    private func connection(to endpoint: Endpoint, clientProtocol: String) async throws -> AvroIPCClient {
        if let existing = pool[endpoint] { return existing }
        let context   = try await rpc.makeIPCContext()
        let hash      = SwiftAvroRpc.md5Hash(of: clientProtocol)
        let transport: any AvroIPCClientTransport
        switch endpoint {
        case .tcp(let host, let port): transport = TCPTransport(host: host, port: port)
        case .unix(let path):          transport = UnixDomainTransport(path: path)
        case .inProcess:
            throw ServiceClientError.inProcessEndpointNotSupported
        }
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport:      transport,
            context:        context,
            clientHash:     hash,
            clientProtocol: clientProtocol,
            serverHash:     hash
        ))
        pool[endpoint] = client
        return client
    }
}

/// Errors thrown by ``ServiceClient``.
public enum ServiceClientError: Error, Sendable, Equatable {
    /// No live endpoint is registered for the named service.
    case noEndpointAvailable(String)
    /// In-process endpoints must be called via ``InProcessServiceProvider/callRaw(serviceName:messageName:requestData:)``.
    case inProcessEndpointNotSupported
}
