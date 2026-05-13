//
//  Service/Endpoint.swift
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

/// The transport address of a service endpoint.
///
/// | Case | When to use |
/// |---|---|
/// | `.tcp` | Inter-node communication across machines |
/// | `.unix` | Intra-node IPC on the same host (macOS / Linux only) |
/// | `.inProcess` | Intra-process calls within a single app (iOS or single-process server) |
public enum Endpoint: Sendable, Hashable, Codable {
    /// Standard TCP socket — used for inter-node (cross-machine) calls.
    case tcp(host: String, port: Int)

    /// Unix domain socket — intra-node IPC on macOS / Linux.
    /// Not available on iOS due to sandbox restrictions.
    case unix(path: String)

    /// In-process dispatch — no socket at all.
    /// The service handler is called directly within the same process.
    /// Used on iOS where all services run as actors in one app process.
    case inProcess(id: String)
}
