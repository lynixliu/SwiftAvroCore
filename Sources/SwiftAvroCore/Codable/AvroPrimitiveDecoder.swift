//
//  AvroClient/AvroBinaryDecoder.swift
//
//  Created by Yang Liu on 28/08/18.
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

/// Decodes Avro primitives from binary (Avro binary encoding) format.
final class AvroPrimitiveDecoder: AvroBinaryDecodableProtocol {
    private var pointer: UnsafePointer<UInt8>
    private(set) var available: Int
    private let size: Int

    var read: Int { size - available }

    init(pointer: UnsafePointer<UInt8>, size: Int) {
        self.pointer = pointer
        self.available = size
        self.size = size
    }

    func advance(_ count: Int) {
        pointer += count
        available -= count
    }

    private func update(pointer: UnsafePointer<UInt8>, available: Int) {
        self.pointer = pointer
        self.available = available
    }

    func decodeNull() {}

    func decode() throws -> Bool {
        guard available >= 1 else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let result = pointer[0] != 0
        advance(1)
        return result
    }

    func decode() throws -> Int32 {
        UInt32(truncatingIfNeeded: try decodeVarint()).decodeZigZag
    }

    func decode() throws -> Int64 {
        try decodeVarint().decodeZigZag
    }

    func decode() throws -> Int {
        Int(try decode() as Int64)
    }

    func decode() throws -> Int8 {
        Int8(try decode() as Int64)
    }

    func decode() throws -> Int16 {
        Int16(try decode() as Int64)
    }

    func decode() throws -> UInt {
        UInt(bitPattern: Int(try decode() as Int64))
    }

    func decode() throws -> UInt8 {
        guard available >= 1 else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let result = pointer[0]
        advance(1)
        return result
    }

    func decode() throws -> UInt16 {
        UInt16(try decode() as Int64)
    }

    func decode() throws -> UInt32 {
        var result: UInt32 = 0
        try decodeFixedWidth(into: &result)
        return result
    }

    func decode() throws -> UInt64 {
        UInt64(bitPattern: try decode() as Int64)
    }

    func decode() throws -> Float {
        var result: Float = 0
        try decodeFixedWidth(into: &result)
        return result
    }

    func decode() throws -> Double {
        var result: Double = 0
        try decodeFixedWidth(into: &result)
        return result
    }

    func decode() throws -> String {
        let bytes: [UInt8] = try decode()
        guard let string = String(bytes: bytes, encoding: .utf8) else {
            throw BinaryDecodingError.malformedAvro
        }
        return string
    }

    func decode() throws -> [UInt8] {
        let length = Int(try decode() as Int64)
        guard available >= length else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let result = Array(UnsafeBufferPointer(start: pointer, count: length))
        advance(length)
        return result
    }

    func decode(fixedSize: Int) throws -> [UInt8] {
        guard available >= fixedSize else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let result = Array(UnsafeBufferPointer(start: pointer, count: fixedSize))
        advance(fixedSize)
        return result
    }

    func decode(fixedSize: Int) throws -> [UInt32] {
        guard available >= fixedSize else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let elementCount = fixedSize / MemoryLayout<UInt32>.size
        var result: [UInt32] = []
        result.reserveCapacity(elementCount)
        for _ in 0..<elementCount {
            var value: UInt32 = 0
            try decodeFixedWidth(into: &value)
            result.append(value)
        }
        return result
    }

    // MARK: - Private helpers

    /// Reads exactly `MemoryLayout<T>.size` bytes from the buffer into `value`.
    private func decodeFixedWidth<T>(into value: inout T) throws {
        let byteCount = MemoryLayout<T>.size
        guard available >= byteCount else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        withUnsafeMutableBytes(of: &value) { dest in
            dest.copyBytes(from: UnsafeRawBufferPointer(start: pointer, count: byteCount))
        }
        advance(byteCount)
    }

    /// Reads and returns a base-128 little-endian varint from the buffer.
    private func decodeVarint() throws -> UInt64 {
        guard available >= 1 else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        var cursor = pointer
        var remaining = available

        let firstByte = cursor[0]
        cursor += 1
        remaining -= 1

        if firstByte & 0x80 == 0 {
            update(pointer: cursor, available: remaining)
            return UInt64(firstByte)
        }

        var value = UInt64(firstByte & 0x7F)
        var shift: UInt64 = 7

        while true {
            guard remaining >= 1, shift < 64 else {
                throw BinaryDecodingError.malformedAvro
            }
            let byte = cursor[0]
            cursor += 1
            remaining -= 1
            value |= UInt64(byte & 0x7F) << shift

            if byte & 0x80 == 0 {
                update(pointer: cursor, available: remaining)
                return value
            }
            shift += 7
        }
    }
}

extension UInt32 {
    /// ZigZag-decodes this value to a signed `Int32`.
    var decodeZigZag: Int32 {
        Int32(bitPattern: self >> 1) ^ -Int32(bitPattern: self & 1)
    }
}

extension UInt64 {
    /// ZigZag-decodes this value to a signed `Int64`.
    var decodeZigZag: Int64 {
        Int64(bitPattern: self >> 1) ^ -Int64(bitPattern: self & 1)
    }
}
