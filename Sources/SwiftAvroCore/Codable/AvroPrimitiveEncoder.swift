//
//  AvroPrimitiveEncoder.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 28/08/18.
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

final class AvroPrimitiveEncoder: AvroPrimitiveEncodeProtocol {

    private(set) var buffer: [UInt8] = []

    var size: Int { buffer.count }

    func encodeNull() {}

    func append(_ other: any AvroPrimitiveEncodeProtocol) {
        buffer.append(contentsOf: other.buffer)
    }

    func encode(_ value: Bool) {
        buffer.append(value ? 1 : 0)
    }

    func encode(_ value: Int) {
        putVarInt(Int64(value).zigZagEncoded)
    }

    func encode(_ value: Int8) {
        putVarInt(UInt64(Int32(value).zigZagEncoded))
    }

    func encode(_ value: Int16) {
        putVarInt(UInt64(Int32(value).zigZagEncoded))
    }

    func encode(_ value: Int32) {
        putVarInt(UInt64(value.zigZagEncoded))
    }

    func encode(_ value: Int64) {
        putVarInt(value.zigZagEncoded)
    }

    /// Encodes UInt as a zigzag varint. Throws if value exceeds Int64.max
    /// since Avro long is a signed 64-bit type.
    func encode(_ value: UInt) throws {
        guard value <= UInt(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        putVarInt(Int64(value).zigZagEncoded)
    }

    func encode(_ value: UInt8) {
        buffer.append(value)
    }

    func encode(_ value: UInt16) {
        putVarInt(UInt64(Int32(value).zigZagEncoded))
    }

    /// UInt32 in Avro duration fixed fields is always little-endian, not varint.
    func encode(_ value: UInt32) {
        appendLittleEndian(value)
    }

    /// Encodes UInt64 as a zigzag varint. Throws if value exceeds Int64.max
    /// since Avro long is a signed 64-bit type.
    func encode(_ value: UInt64) throws {
        guard value <= UInt64(Int64.max) else { throw BinaryEncodingError.uintOverflow }
        putVarInt(Int64(value).zigZagEncoded)
    }

    func encode(_ value: Float) {
        appendLittleEndian(value.bitPattern)
    }

    func encode(_ value: Double) {
        appendLittleEndian(value.bitPattern)
    }

    func encode(_ value: String) {
        encode(Int64(value.utf8.count))
        buffer.append(contentsOf: value.utf8)
    }

    func encode(_ value: [UInt8]) {
        encode(Int64(value.count))
        buffer.append(contentsOf: value)
    }

    func encode(fixed: [UInt8]) {
        buffer.append(contentsOf: fixed)
    }

    func encode(fixed: [UInt32]) {
        fixed.forEach { encode($0) }
    }

    // MARK: - Private helpers

    private func putVarInt(_ value: UInt64) {
        var v = value
        while v > 0x7F {
            buffer.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        buffer.append(UInt8(v))
    }

    private func appendLittleEndian<T: FixedWidthInteger>(_ value: T) {
        var v = value.littleEndian
        withUnsafeBytes(of: &v) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - ZigZag encoding

extension Int32 {
    var zigZagEncoded: UInt32 {
        UInt32(bitPattern: (self << 1) ^ (self >> 31))
    }
}

extension Int64 {
    var zigZagEncoded: UInt64 {
        UInt64(bitPattern: (self << 1) ^ (self >> 63))
    }
}
