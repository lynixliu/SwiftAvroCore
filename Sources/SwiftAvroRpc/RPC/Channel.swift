//
//  RPC/Channel.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 04/04/2026.
//  Copyright © 2026 Yang Liu.
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
import NIO

// MARK: - AvroServerChannel

/// A bound Avro IPC server channel. Returned by ``SwiftAvroRpc/makeServer(host:port:context:serverHash:serverProtocol:handler:)``.
/// Call ``close()`` to stop accepting connections and release the port.
public final class AvroServerChannel: Sendable {

    private let channel: any Channel

    init(channel: any Channel) {
        self.channel = channel
    }

    /// Closes the server and stops accepting new connections.
    public func close() async throws {
        try await channel.close()
    }

    /// The local address the server is bound to, or nil if unavailable.
    public var localAddress: String? {
        channel.localAddress?.description
    }
}
