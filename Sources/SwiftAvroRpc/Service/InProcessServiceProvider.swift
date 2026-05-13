//
//  Service/InProcessServiceProvider.swift
//  SwiftAvroRpc
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

/// Hosts services as in-process actors — no sockets, no IPC overhead.
///
/// Use this on iOS (or any single-process node) where all services run as
/// Swift actor threads within one app process. Callers dispatch via
/// ``callRaw(serviceName:messageName:requestData:)`` instead of going through
/// a network stack.
///
/// The provider still registers each service with the ``ServiceCatalog`` so
/// the cluster can discover which services live on this node. Other nodes
/// reach this node over TCP; actors within the same process call it directly.
///
/// **Usage:**
/// ```swift
/// let local = InProcessServiceProvider(nodeID: node.nodeID)
/// try await local.host(service: try GreeterService(), catalogue: registry)
///
/// let responseData = try await local.callRaw(
///     serviceName: "greeter",
///     messageName: "hello",
///     requestData: encodedRequest
/// )
/// ```
public actor InProcessServiceProvider {

    private var handlers: [String: any AvroIPCHandler] = [:]
    private let nodeID:   String

    public init(nodeID: String) {
        self.nodeID = nodeID
    }

    // MARK: - Registration

    /// Registers a service and makes it available for in-process dispatch.
    ///
    /// The service is also registered in the ``ServiceCatalog`` with an
    /// ``Endpoint/inProcess(id:)`` endpoint so other nodes know this service
    /// exists on this node (reachable via TCP for cross-node calls).
    public func host(service: any AvroService, catalogue: any ServiceCatalog) async throws {
        let id = "\(nodeID)/\(service.serviceName)"
        handlers[service.serviceName] = service.handler
        let info = ServiceInfo(
            name:     service.serviceName,
            version:  service.serviceVersion,
            endpoint: .inProcess(id: id),
            nodeID:   nodeID
        )
        try await catalogue.register(info)
    }

    // MARK: - Dispatch

    /// Dispatches a call directly to the named service handler.
    ///
    /// Bypasses all networking — the handler runs in the current process.
    /// Request and response data must be Avro-encoded by the caller.
    ///
    /// - Throws: ``InProcessServiceError/serviceNotFound(_:)`` if no handler
    ///   is registered for `serviceName`.
    public func callRaw(
        serviceName: String,
        messageName: String,
        requestData: Data
    ) async throws -> Data {
        guard let handler = handlers[serviceName] else {
            throw InProcessServiceError.serviceNotFound(serviceName)
        }
        return try await handler.handle(messageName: messageName, requestData: requestData)
    }

    /// Returns the handler for the named service, or `nil` if not registered.
    public func handler(for serviceName: String) -> (any AvroIPCHandler)? {
        handlers[serviceName]
    }
}

/// Errors thrown by ``InProcessServiceProvider``.
public enum InProcessServiceError: Error, Sendable, Equatable {
    /// No handler has been registered for the named service.
    case serviceNotFound(String)
}
