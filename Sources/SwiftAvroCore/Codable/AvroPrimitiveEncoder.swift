//
// AvroClient/AvroBinaryEncoder.swift
//
//  Created by Yang Liu on 24/08/18.
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

/*
 * Encoder for convert Avro primitives to binary format
 */
internal class AvroPrimitiveEncoder: AvroPrimitiveEncodeProtocol {
    var buffer: [UInt8] = []
    var size: Int {
        return buffer.count
    }
    init() {
        buffer = []
    }
    func append(_ other: AvroPrimitiveEncodeProtocol) {
        buffer.append(contentsOf: other.buffer)
    }
    
    func encodeNull() {
        return
    }
    
    func encode(_ value: Bool) {
        buffer.append(value ? 1 : 0)
    }
    
    func encode(_ value: Int) {
        putVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: Int8) {
        putVarInt(value: UInt64(Int32(value).encodeToZigZag()))
    }
    
    func encode(_ value: Int16) {
        putVarInt(value: UInt64(Int32(value).encodeToZigZag()))
    }
    
    func encode(_ value: Int32) {
        putVarInt(value: UInt64(value.encodeToZigZag()))
    }

    func encode(_ value: Int64) {
        putVarInt(value: value.encodeToZigZag())
    }
    func encode(_ value: UInt) {
        putVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: UInt8) {
        buffer.append(value)
    }
    
    func encode(_ value: UInt16) {
        putVarInt(value: UInt64(Int32(value).encodeToZigZag()))
    }
    
    func encode(_ value: UInt32) {
        var shiftValue = value
        for _ in 0..<4 {
            buffer.append(UInt8(0xFF & shiftValue))
            shiftValue >>= 8
        }
    }
    
    func encode(_ value: UInt64) {
        putVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: Float) {
        let size = MemoryLayout<Float>.size
        var shiftValue = value.bitPattern
        for _ in 0..<size {
            buffer.append(UInt8(0xFF & shiftValue))
            shiftValue >>= 8
        }
    }
    
    func encode(_ value: Double) {
        let size = MemoryLayout<Double>.size
        var shiftValue = value.bitPattern
        for _ in 0..<size {
            buffer.append(UInt8(0xFF & shiftValue))
            shiftValue >>= 8
        }
    }
    
    func encode(_ value: String) {
        encode(Int64(value.utf8.count))
        buffer.append(contentsOf: [UInt8](value.utf8))
    }
    
    func encode(_ value: [UInt8]) {
        encode(Int64(value.count))
        buffer.append(contentsOf: value)
    }
    
    func encode(fixed: [UInt8]) {
        buffer.append(contentsOf: fixed)
        return
    }
    
    func encode(fixed: [UInt32]) {
        for v in fixed {
            encode(v)
        }
        return
    }
    
    private func putVarInt(value: UInt64) {
        var v = value
        while v > 127 {
            buffer.append(UInt8(v & 0x7f | 0x80))
            v >>= 7
        }
        buffer.append(UInt8(v))
    }
} 

extension Int32 {
    public func encodeToZigZag() -> UInt32 {
        return UInt32(bitPattern: (self << 1) ^ (self >> 31))
    }
}

extension Int64 {
    public func encodeToZigZag() -> UInt64 {
        return UInt64(bitPattern: (self << 1) ^ (self >> 63))
    }
}
