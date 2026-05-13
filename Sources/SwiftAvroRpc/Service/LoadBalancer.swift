//
//  Service/LoadBalancer.swift
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

/// Selects a provider endpoint from the candidates returned by a ``ServiceCatalog``.
///
/// Default strategy: round-robin across all live endpoints. Replace or extend
/// this actor to implement weighted, least-connections, or locality-aware routing.
public actor LoadBalancer {

    private var cursors: [String: Int] = [:]

    public init() {}

    /// Returns the next endpoint for the named service, or `nil` if none are registered.
    public func select(
        serviceName: String,
        from candidates: [ServiceInfo]
    ) -> ServiceInfo? {
        guard !candidates.isEmpty else { return nil }
        let index = (cursors[serviceName] ?? 0) % candidates.count
        cursors[serviceName] = index + 1
        return candidates[index]
    }
}
