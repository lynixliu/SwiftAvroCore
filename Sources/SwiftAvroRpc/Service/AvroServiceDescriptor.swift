//
//  Service/AvroServiceDescriptor.swift
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

/// Loads an Avro protocol definition from a `.avpr` bundle resource.
///
/// Use this in your ``AvroService`` conformance to keep the protocol JSON
/// out of source code and in a versioned resource file instead.
///
/// **Usage:**
/// ```swift
/// struct GreeterService: AvroService {
///     var avroProtocol: String
///
///     init() throws {
///         self.avroProtocol = try AvroServiceDescriptor(resource: "greeter", in: .module).json
///     }
/// }
/// ```
///
/// The resource file must be named `<resource>.avpr` and included in the
/// target's resource bundle (declared in `Package.swift` as `.process("Resources")`).
public struct AvroServiceDescriptor: Sendable {

    /// The raw Avro protocol JSON loaded from the resource file.
    public let json: String

    /// Loads `<resource>.avpr` from the given bundle.
    ///
    /// - Parameters:
    ///   - resource: File name without extension (e.g. `"greeter"` loads `greeter.avpr`).
    ///   - bundle: The bundle that owns the resource file. Pass `Bundle.module` from the
    ///     **service's own target** — not from `SwiftAvroRpc`, which has no resources.
    /// - Throws: ``AvroServiceDescriptorError/resourceNotFound(_:)`` if the file is absent.
    public init(resource: String, in bundle: Bundle) throws {
        guard let url = bundle.url(forResource: resource, withExtension: "avpr") else {
            throw AvroServiceDescriptorError.resourceNotFound("\(resource).avpr")
        }
        self.json = try String(contentsOf: url, encoding: .utf8)
    }
}

/// Errors thrown by ``AvroServiceDescriptor``.
public enum AvroServiceDescriptorError: Error, Sendable, Equatable, CustomStringConvertible {
    /// The named `.avpr` file was not found in the bundle.
    case resourceNotFound(String)

    public var description: String {
        switch self {
        case .resourceNotFound(let name):
            return "AvroServiceDescriptor: '\(name)' not found in bundle. "
                + "Ensure the file is listed under .process(\"Resources\") in Package.swift."
        }
    }
}
