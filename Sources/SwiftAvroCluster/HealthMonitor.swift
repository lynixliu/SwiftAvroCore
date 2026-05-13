//
//  HealthMonitor.swift
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

/// Subscribes to cluster membership events and removes dead nodes from the catalogue.
///
/// Runs as a long-lived task on each node. When SWIM marks a peer as `.down`,
/// `HealthMonitor` calls ``ServiceCatalog/deregister(nodeID:)`` so stale
/// endpoints are no longer returned to clients.
public actor HealthMonitor {

    private let system:    ClusterSystem
    private let catalogue: any ServiceCatalog

    public init(system: ClusterSystem, catalogue: any ServiceCatalog) {
        self.system    = system
        self.catalogue = catalogue
    }

    /// Starts watching for cluster membership events. Suspends indefinitely — run inside a `Task`.
    public func watch() async {
        for await event in system.cluster.events {
            guard case .membershipChange(let change) = event,
                  change.member.status.isDown else { continue }
            let nodeID = "\(change.member.node.host):\(change.member.node.port)"
            try? await catalogue.deregister(nodeID: nodeID)
        }
    }
}
