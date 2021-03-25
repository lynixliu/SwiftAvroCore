//
//  AvroDatum.swift
//
//  Created by Kacper Kawecki on 25/03/2021.
//  Copyright © 2021 by Kacper Kawecki and the project authors.
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

internal enum AvroDatum {
    case primitive(AvroPrimitiveValue)
    case logical(AvroLogicalValue)
    case array([AvroDatum])
    case keyed([String: AvroDatum])

    func bytesToArray() throws -> [AvroDatum] {
        if case .primitive(.bytes(let bytes)) = self {
            var parsedBytes: [AvroDatum] = []
            for byte in bytes {
                parsedBytes.append(.primitive(.byte(byte)))
            }
            return parsedBytes
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    func durationToArray() throws -> [AvroDatum] {
        switch self {
        case .logical(.duration(let bytes)):
            guard bytes.count == 12 else {
                throw BinaryDecodingError.malformedAvro
            }
            let months = UInt32(littleEndian: bytes[0..<4].withUnsafeBytes { $0.load(as: UInt32.self) })
            let days = UInt32(littleEndian: bytes[4..<8].withUnsafeBytes { $0.load(as: UInt32.self) })
            let milliseconds = UInt32(littleEndian: bytes[8...].withUnsafeBytes { $0.load(as: UInt32.self) })
            return [
                .primitive(.durationElement(months)),
                .primitive(.durationElement(days)),
                .primitive(.durationElement(milliseconds))
            ]
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
}
