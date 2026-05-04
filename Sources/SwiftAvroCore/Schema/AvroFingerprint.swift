//
//  AvroClient/AvroFingerprint.swift
//
//  Created by Yang Liu on 21/09/18.
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

/// A function that maps raw bytes to a fingerprint byte sequence.
///
/// Pass any algorithm where the library accepts a fingerprint function:
/// ```swift
/// import CryptoKit
/// let sha256: FingerprintFunction = { data in
///     Array(SHA256.hash(data: Data(data)))
/// }
/// ```
public typealias FingerprintFunction = ([UInt8]) -> [UInt8]

/// Avro-standard 64-bit Rabin fingerprint.
///
/// All members are static — no instance is needed.
public enum AvroFingerprint {

    private static let empty: Int64 = -4513414715797952619

    // Computed once at program start; shared across all callers.
    private static let table: [Int64] = (0..<256).map { i in
        var fp = Int64(i)
        for _ in 0..<8 { fp = (fp >> 1) ^ (Self.empty & -(fp & 1)) }
        return fp
    }

    /// Returns the 64-bit Rabin fingerprint of `data`.
    public static func fingerprint64(_ data: [UInt8]) -> Int64 {
        data.reduce(Self.empty) { fp, byte in
            (fp >> 8) ^ table[Int((fp ^ Int64(byte)) & 0xff)]
        }
    }
}
