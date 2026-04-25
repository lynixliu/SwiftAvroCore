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

/// Avro 64-bit Rabin fingerprint.
///
/// This is a standalone implementation with no external dependencies.
/// For SHA-256 or MD5 fingerprints, consider using CommonCrypto or
/// swift-crypto.
public class AvroFingerPrint {

    private static let empty: Int64 = -4513414715797952619

    private let table: [Int64]

    public convenience init() { self.init(size: 256) }

    public init(size: Int) {
        table = (0..<size).map { i in
            var fp = Int64(i)
            for _ in 0..<8 {
                fp = (fp >> 1) ^ (Self.empty & -(fp & 1))
            }
            return fp
        }
    }

    /// Returns the 64-bit Rabin fingerprint of `data`.
    func fingerPrint64(_ data: [UInt8]) -> Int64 {
        data.reduce(Self.empty) { fp, byte in
            (fp >> 8) ^ table[Int((fp ^ Int64(byte)) & 0xff)]
        }
    }
}
