//
//  ClusterNode.swift
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

/// Manages the distributed actor cluster on this process.
///
/// Every process in the system creates exactly one `ClusterNode`. It owns the
/// `ClusterSystem` that backs all `distributed actor` types and provides
/// SWIM-based failure detection.
public actor ClusterNode {

    private let system: ClusterSystem

    /// Stable identifier for this node: `"\(host):\(clusterPort)"`.
    public let nodeID: String

    /// Initialises the cluster system and binds it to the given address.
    public init(host: String, port: Int) async throws {
        self.nodeID = "\(host):\(port)"
        self.system = await ClusterSystem("SwiftAvroCluster") { settings in
            settings.bindHost = host
            settings.bindPort = port
        }
    }

    /// The underlying actor system — pass to `DistributedActor.resolve` calls.
    public var actorSystem: ClusterSystem { system }

    /// Contacts a seed node to join an existing cluster.
    public func join(seedHost: String, seedPort: Int) {
        system.cluster.join(host: seedHost, port: seedPort)
    }

    /// Suspends until this node has successfully joined the cluster.
    public func waitUntilUp() async throws {
        _ = try await system.cluster.joined(within: .seconds(30))
    }

    /// Shuts down the cluster system and releases all resources.
    public func shutdown() async throws {
        try await system.shutdown()
    }
}
