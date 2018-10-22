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

/// Implementate the Avro 64-bit Rabin fingerprint
/// This package is designed as a cross platform standalong library which do not depend on other libaray.
/// To use the the SHA and MD5 as finger print please refer to the implementation at:
/// https://github.com/apple/swift-package-manager/tree/master/Sources/SHA.swift
/// or use the CommonCrypto.
/// For osx 10.11+ platform, there is a referene implementation list below:
/// func sha256Hash(data: Data) -> Data {
///     let transform = SecDigestTransformCreate(kSecDigestSHA2, 256, nil)
///     SecTransformSetAttribute(transform, kSecTransformInputAttributeName, data as CFTypeRef, nil)
///     return SecTransformExecute(transform, nil) as! Data
/// }

class AvroFingerPrint {
    private let EMPTY: Int64 = -4513414715797952619;
    private let TABLE_SIZE = 256
    private var FingerPrintTable: [Int64]
    
    /// Avro 64-bit Rabin fingerprint
    func fingerPrint64(_ data: [UInt8]) -> Int64 {
        var fingerPrint = EMPTY;
        for value in data {
            fingerPrint = (fingerPrint >> 8) ^ FingerPrintTable[Int((fingerPrint ^ Int64(value)) & 0xff)]
        }
        return fingerPrint;
    }
    
    init() {
        self.FingerPrintTable = [Int64]()
        self.FingerPrintTable.reserveCapacity(TABLE_SIZE)
        for i in 0..<TABLE_SIZE {
            var fingerPrint = Int64(i)
            for _ in 0..<8 {
                let mask = -(fingerPrint & 0x1);
                fingerPrint = (fingerPrint >> 1) ^ (EMPTY & mask);
                self.FingerPrintTable[i] = fingerPrint;
            }
        }
    }
}

