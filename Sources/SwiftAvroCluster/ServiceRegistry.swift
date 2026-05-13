//
//  ServiceRegistry.swift
//  SwiftAvroCluster
//
//  Copyright © 2026 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import DistributedCluster
import Foundation
import SwiftAvroRpc

/// Distributed actor that acts as the service catalogue for the cluster.
///
/// Any node can register service endpoints or query for live providers.
/// The registry is backed by the `ClusterSystem` and accessible from all nodes
/// via `ServiceRegistry.resolve(id:using:)`.
///
/// Node liveness is maintained externally by ``HealthMonitor``, which calls
/// ``deregister(nodeID:)`` whenever SWIM marks a node as down.
///
/// `ServiceRegistry` conforms to ``ServiceCatalog`` so it can be passed directly
/// to ``ServiceProvider`` and ``ServiceClient``.
public distributed actor ServiceRegistry: ServiceCatalog {

    public typealias ActorSystem = ClusterSystem

    private var services: [String: [ServiceInfo]] = [:]

    public init(actorSystem: ClusterSystem) {
        self.actorSystem = actorSystem
    }

    /// Registers a service endpoint. Called by ``ServiceProvider`` on startup.
    public distributed func register(_ info: ServiceInfo) {
        services[info.name, default: []].append(info)
    }

    /// Removes all endpoints belonging to a node. Called by ``HealthMonitor`` on node death.
    public distributed func deregister(nodeID: String) {
        for key in services.keys {
            services[key]?.removeAll { $0.nodeID == nodeID }
        }
    }

    /// Returns all live endpoints for the named service.
    public distributed func discover(serviceName: String) -> [ServiceInfo] {
        services[serviceName] ?? []
    }
}
