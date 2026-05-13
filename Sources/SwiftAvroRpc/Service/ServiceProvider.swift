//
//  Service/ServiceProvider.swift
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

/// Hosts one or more ``AvroService`` implementations within a single process.
///
/// For each hosted service, `ServiceProvider` starts an Avro IPC server on the
/// declared endpoint and registers the service with the cluster catalogue. Supports
/// both TCP (inter-node) and Unix domain socket (intra-node) transports through
/// the pluggable ``AvroIPCServerTransport`` interface.
public actor ServiceProvider {

    private let rpc:    SwiftAvroRpc
    private let nodeID: String
    private var servers: [String: AvroServerChannel] = [:]

    public init(nodeID: String, threads: Int = 1) {
        self.nodeID = nodeID
        self.rpc    = SwiftAvroRpc(threads: threads)
    }

    /// Starts the service on the given endpoint and registers it with the catalogue.
    ///
    /// - Parameters:
    ///   - service: The service implementation to host.
    ///   - endpoint: Where to bind — `.tcp` for inter-node, `.unix` for intra-node.
    ///   - catalogue: The service catalogue to register with.
    public func host(
        service:   any AvroService,
        endpoint:  Endpoint,
        catalogue: any ServiceCatalog
    ) async throws {
        let context = try await rpc.makeIPCContext()
        let hash    = SwiftAvroRpc.md5Hash(of: service.avroProtocol)
        let transport: any AvroIPCServerTransport
        switch endpoint {
        case .tcp(let host, let port): transport = TCPTransport(host: host, port: port)
        case .unix(let path):          transport = UnixDomainTransport(path: path)
        case .inProcess:
            throw ServiceProviderError.unsupportedEndpoint(endpoint)
        }
        let server = try await rpc.makeServer(AvroIPCServerConfig(
            transport:      transport,
            context:        context,
            serverHash:     hash,
            serverProtocol: service.avroProtocol,
            handler:        service.handler
        ))
        servers[service.serviceName] = server

        let info = ServiceInfo(
            name:     service.serviceName,
            version:  service.serviceVersion,
            endpoint: endpoint,
            nodeID:   nodeID
        )
        try await catalogue.register(info)
    }

    /// Closes all hosted servers and stops the event loop group.
    public func shutdown() async throws {
        for server in servers.values { try await server.close() }
        try await rpc.stop()
    }
}

/// Errors thrown by ``ServiceProvider``.
public enum ServiceProviderError: Error, Sendable, Equatable {
    /// `.inProcess` endpoints must be hosted via ``InProcessServiceProvider``, not ``ServiceProvider``.
    case unsupportedEndpoint(Endpoint)
}
