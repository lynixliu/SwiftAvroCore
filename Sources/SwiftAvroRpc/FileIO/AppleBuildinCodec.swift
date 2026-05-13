//
//  FileIO/AppleBuiltinCodec.swift
//
//  Created by Yang Liu on  04/04/2026.
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
#if canImport(Compression)
import Compression
#endif
import SwiftAvroCore

// MARK: - Errors

/// Errors thrown by codec operations.
/// Used as the typed-throw error for ``BuiltinCodec`` and ``CodecFactory``.
enum AvroCodecError: Error, Sendable, Equatable {
    /// The requested codec name is not supported.
    case unsupportedCodec(String)
    /// The input data was empty.
    case emptySourceData
    /// The compression algorithm failed to produce output.
    case compressionFailed
    /// The decompression algorithm failed to produce output.
    case decompressionFailed
}

// MARK: - Codecs

#if canImport(Compression)
/// A codec backed by Apple's `Compression` framework.
/// Supports deflate (ZLIB), LZ4, LZMA (xz), and LZFSE algorithms.
struct BuiltinCodec: CodecProtocol, Sendable {
    let name: String
    private let decompressBufferSize: Int

    private var algorithm: compression_algorithm {
        switch name {
        case AvroReservedConstants.deflateCodec: return COMPRESSION_ZLIB
        case AvroReservedConstants.xzCodec:      return COMPRESSION_LZMA
        case AvroReservedConstants.lz4Codec:     return COMPRESSION_LZ4
        case AvroReservedConstants.lzfseCodec:   return COMPRESSION_LZFSE
        default:                                  return COMPRESSION_BROTLI
        }
    }

    /// Creates a codec for the given Avro codec name.
    init(codecName: String, decompressBufferSize: Int = 1024 << 3) {
        self.name = codecName
        self.decompressBufferSize = decompressBufferSize
    }

    /// - Throws: ``AvroCodecError/emptySourceData`` or ``AvroCodecError/compressionFailed``.
    func compress(data: Data) throws(AvroCodecError) -> Data {
        guard !data.isEmpty else { throw .emptySourceData }
        let src = [UInt8](data)
        let capacity = max(src.count * 2, 1024)
        let dst = UnsafeMutablePointer<UInt8>.allocate(capacity: capacity)
        defer { dst.deallocate() }
        let written = compression_encode_buffer(
            dst, capacity, src, src.count, nil, algorithm
        )
        guard written > 0 else { throw .compressionFailed }
        return Data(bytes: dst, count: written)
    }

    /// - Throws: ``AvroCodecError/emptySourceData`` or ``AvroCodecError/decompressionFailed``.
    func decompress(data: Data) throws(AvroCodecError) -> Data {
        guard !data.isEmpty else { throw .emptySourceData }
        let src = [UInt8](data)
        var dstSize = max(data.count * 4, 65536)
        while dstSize <= 16 * 1024 * 1024 {
            let dst = UnsafeMutablePointer<UInt8>.allocate(capacity: dstSize)
            defer { dst.deallocate() }
            let written = compression_decode_buffer(
                dst, dstSize, src, src.count, nil, algorithm
            )
            if written > 0 { return Data(bytes: dst, count: written) }
            if written == 0 { break }
            dstSize *= 2
        }
        throw .decompressionFailed
    }
}
#endif

// MARK: - AvroCodecRegistry

/// A global registry that maps codec names to user-supplied ``CodecProtocol`` implementations.
///
/// Register custom codecs at startup before creating any container that uses their names.
/// Built-in codecs (`null`, `deflate`, `lz4`, `xz`, `lzfse`) are always available
/// without registration and cannot be overridden.
///
/// **Name-based usage:**
/// ```swift
/// AvroCodecRegistry.register(ZstdCodec())
/// let container = try AvroFileObjectContainer(schema: mySchema, codec: ZstdCodec(), syncMarker: marker)
/// ```
public enum AvroCodecRegistry {
    private static let lock = NSLock()
    private nonisolated(unsafe) static var registered: [String: any CodecProtocol] = [:]

    /// Registers a codec. Replaces any previous registration under the same name.
    public static func register(_ codec: any CodecProtocol) {
        lock.lock(); defer { lock.unlock() }
        registered[codec.name] = codec
    }

    /// Looks up a previously registered codec by name. Returns `nil` if not found.
    public static func codec(named name: String) -> (any CodecProtocol)? {
        lock.lock(); defer { lock.unlock() }
        return registered[name]
    }
}

// MARK: - CodecFactory

/// Factory that creates ``CodecProtocol`` instances by Avro codec name.
/// Checks built-in codecs first, then falls back to ``AvroCodecRegistry``.
enum CodecFactory: Sendable {
    /// Returns a codec for the given Avro codec name.
    /// - Throws: ``AvroCodecError/unsupportedCodec(_:)`` if the name is not recognised.
    static func make(named name: String) throws(AvroCodecError) -> any CodecProtocol {
        switch name {
        case AvroReservedConstants.nullCodec:
            return NullCodec()
        #if canImport(Compression)
        case AvroReservedConstants.deflateCodec,
             AvroReservedConstants.xzCodec,
             AvroReservedConstants.lz4Codec,
             AvroReservedConstants.lzfseCodec:
            return BuiltinCodec(codecName: name)
        #endif
        default:
            if let custom = AvroCodecRegistry.codec(named: name) { return custom }
            throw .unsupportedCodec(name)
        }
    }
}
