//
//  Service/ServiceCatalog.swift
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

/// Abstracts the service registry so the service layer does not depend on
/// `DistributedCluster`.  The concrete ``ServiceRegistry`` distributed actor
/// (in `SwiftAvroCluster`) conforms to this protocol.
///
/// For testing, supply a simple in-memory implementation.
public protocol ServiceCatalog: Sendable {
    /// Registers a service endpoint. Called by ``ServiceProvider`` on startup.
    func register(_ info: ServiceInfo) async throws

    /// Returns all live endpoints for the named service.
    func discover(serviceName: String) async throws -> [ServiceInfo]

    /// Removes all endpoints belonging to a node. Called on node failure.
    func deregister(nodeID: String) async throws
}
