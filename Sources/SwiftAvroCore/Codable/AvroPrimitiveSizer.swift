//
//  AvroClient/AvroPrimitiveSizer.swift
//
//  Created by Yang Liu on 19/09/18.
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
/// pre-allocate the size of encoding data before serialization
/// use for skip field or block when applying schema resolution

/// Computes the encoded byte size of Avro primitives without allocating a buffer.
final class AvroPrimitiveSizer: AvroPrimitiveEncodeProtocol {

    // Satisfies the protocol but is never populated — the sizer only tracks byte count.
    var buffer: [UInt8] { get { [] } set {} }

    private(set) var size: Int = 0

    // Null encodes to zero bytes in Avro binary format
    func encodeNull() {}

    func append(_ other: any AvroPrimitiveEncodeProtocol) {
        size += other.size
    }

    func encode(_ value: Bool)   { size += 1 }
    func encode(_ value: UInt8)  { size += 1 }
    func encode(_ value: Float)  { size += MemoryLayout<Float>.size }
    func encode(_ value: Double) { size += MemoryLayout<Double>.size }

    func encode(_ value: Int)    { sizeVarInt(Int64(value).zigZagEncoded) }
    func encode(_ value: Int8)   { sizeVarInt(UInt64(Int32(value).zigZagEncoded)) }
    func encode(_ value: Int16)  { sizeVarInt(UInt64(Int32(value).zigZagEncoded)) }
    func encode(_ value: Int32)  { sizeVarInt(UInt64(value.zigZagEncoded)) }
    func encode(_ value: Int64)  { sizeVarInt(value.zigZagEncoded) }
    func encode(_ value: UInt)   { sizeVarInt(Int64(bitPattern: UInt64(value)).zigZagEncoded) }
    func encode(_ value: UInt16) { sizeVarInt(UInt64(Int32(value).zigZagEncoded)) }
    func encode(_ value: UInt32) { size += 4 } // UInt32 is always 4 bytes (little-endian fixed)
    func encode(_ value: UInt64) { sizeVarInt(Int64(bitPattern: value).zigZagEncoded) }

    func encode(_ value: String) {
        let utf8Count = value.utf8.count
        encode(Int64(utf8Count))
        size += utf8Count
    }

    func encode(_ value: [UInt8]) {
        encode(Int64(value.count))
        size += value.count
    }

    func encode(fixed: [UInt8])  { size += fixed.count }
    func encode(fixed: [UInt32]) { size += fixed.count * MemoryLayout<UInt32>.size }

    // MARK: - Private helpers

    /// Accumulates the varint byte size of a `UInt64` ZigZag-encoded value.
    private func sizeVarInt(_ value: UInt64) {
        // Negative Int64 values (high bit set) always encode to 10 varint bytes.
        if Int64(bitPattern: value) < 0 {
            size += 10
            return
        }
        // Divide-and-conquer to find the minimal byte count for the remaining 63 bits.
        var v = value
        var n = 1
        if v & (~UInt64(0) << 35) != 0 { n += 4; v >>= 28 }
        if v & (~UInt64(0) << 21) != 0 { n += 2; v >>= 14 }
        if v & (~UInt64(0) << 14) != 0 { n += 1; v >>= 7  }
        if v & (~UInt64(0) <<  7) != 0 { n += 1           }
        size += n
    }
}