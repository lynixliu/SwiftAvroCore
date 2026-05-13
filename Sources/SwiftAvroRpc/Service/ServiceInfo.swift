//
//  Service/ServiceInfo.swift
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

/// Metadata for a registered service instance.
/// Stored in the catalogue and used by the load balancer to select endpoints.
public struct ServiceInfo: Sendable, Hashable, Codable {

    /// Logical service name (e.g. `"greeter"`, `"auth"`).
    public let name: String

    /// Semantic version string (e.g. `"1.0.0"`).
    public let version: String

    /// The endpoint clients should connect to in order to call this service.
    public let endpoint: Endpoint

    /// Identifier of the cluster node hosting this service instance.
    /// Format: `"\(host):\(clusterPort)"`.
    public let nodeID: String

    public init(name: String, version: String, endpoint: Endpoint, nodeID: String) {
        self.name     = name
        self.version  = version
        self.endpoint = endpoint
        self.nodeID   = nodeID
    }
}
