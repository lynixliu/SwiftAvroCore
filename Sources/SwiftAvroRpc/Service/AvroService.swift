//
//  Service/AvroService.swift
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

/// Conform to this protocol to expose a service via Avro IPC.
///
/// The service descriptor (protocol JSON + handler) is consumed by
/// ``ServiceProvider`` to bind an Avro IPC server on the declared endpoint.
///
/// **Recommended pattern** — load the Avro protocol from a bundled `.avpr` file
/// using ``AvroServiceDescriptor`` rather than embedding JSON in source code:
///
/// ```swift
/// struct GreeterService: AvroService {
///     var avroProtocol: String
///     var serviceName:  String { "greeter" }
///     var serviceVersion: String { "1.0.0" }
///     var handler: any AvroIPCHandler { GreeterHandler() }
///
///     init() throws {
///         self.avroProtocol = try AvroServiceDescriptor(resource: "greeter", in: .module).json
///     }
/// }
/// ```
///
/// Place `greeter.avpr` under `Sources/<Target>/Resources/` and declare
/// `.process("Resources")` in the target's `Package.swift` resources list.
public protocol AvroService: Sendable {

    /// Avro protocol JSON string describing all messages this service handles.
    var avroProtocol: String { get }

    /// The handler that processes incoming RPC calls for this service.
    var handler: any AvroIPCHandler { get }

    /// Logical name used for registration and discovery (e.g. `"greeter"`).
    var serviceName: String { get }

    /// Semantic version of this service implementation (e.g. `"1.0.0"`).
    var serviceVersion: String { get }
}
