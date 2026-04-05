//
//  AvroClient/NullCodec.swift
//
//  Created by Yang Liu on 22/09/18.
//  Copyright © 2018 柳洋 and the project authors.
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
// MARK: - CodecProtocol

public protocol CodecProtocol {
    /// Compresses the given data, returning the compressed result.
    func compress(data: Data) throws -> Data

    /// Decompresses the given data, returning the original result.
    func decompress(data: Data) throws -> Data

    /// The canonical name of this codec (e.g. `"null"`, `"deflate"`).
    var name: String { get }
}

// MARK: - NullCodec

/// A no-op codec that passes data through unchanged.
public struct NullCodec: CodecProtocol {
    public let name: String

    public init(name: String = AvroReservedConstants.nullCodec) {
        self.name = name
    }

    public func compress(data: Data) throws -> Data   { data }
    public func decompress(data: Data) throws -> Data { data }
}

// MARK: - Codec

/// A type-erasing wrapper around any `CodecProtocol` implementation.
public struct Codec: CodecProtocol {
    private let wrapped: any CodecProtocol

    public init(_ codec: any CodecProtocol) {
        self.wrapped = codec
    }

    /// Defaults to the null (pass-through) codec.
    public init() {
        self.wrapped = NullCodec()
    }

    public var name: String { wrapped.name }

    public func compress(data: Data) throws -> Data {
        try wrapped.compress(data: data)
    }

    public func decompress(data: Data) throws -> Data {
        try wrapped.decompress(data: data)
    }
}

