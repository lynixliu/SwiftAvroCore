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
internal class AvroPrimitiveSizer: AvroPrimitiveEncodeProtocol {
    var buffer: [UInt8] = []
    var size: Int
    
    init() {
        size = 0
    }
    func append(_ other: AvroPrimitiveEncodeProtocol) {
        size += other.size
    }
    
    func encodeNull() {
        return
    }
    
    func encode(_ value: Bool) {
        size += 1
    }
    
    func encode(_ value: Int) {
        sizeOfVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: Int8) {
        sizeOfVarInt(value: UInt64(Int32(value).encodeToZigZag()))
    }
    
    func encode(_ value: Int16) {
        sizeOfVarInt(value: UInt64(Int32(value).encodeToZigZag()))
    }
    
    func encode(_ value: Int32) {
        sizeOfVarInt(value: UInt64(value.encodeToZigZag()))
    }
    
    func encode(_ value: Int64) {
        sizeOfVarInt(value: value.encodeToZigZag())
    }
    func encode(_ value: UInt) {
        sizeOfVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: UInt8) {
        size += 1
    }
    
    func encode(_ value: UInt16) {
        sizeOfVarInt(value: UInt64(Int32(value).encodeToZigZag()))
    }
    
    func encode(_ value: UInt32) {
        sizeOfVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: UInt64) {
        sizeOfVarInt(value: UInt64(Int64(value).encodeToZigZag()))
    }
    
    func encode(_ value: Float) {
        size += MemoryLayout<Float>.size
    }
    
    func encode(_ value: Double) {
        size += MemoryLayout<Double>.size
    }
    
    func encode(_ value: String) {
        encode(Int64(value.utf8.count))
        size += value.utf8.count
    }
    
    func encode(_ value: [UInt8]) {
        encode(Int64(value.count))
        size += value.count
    }
    
    func encode(fixed: [UInt8]) {
        size += fixed.count
        return
    }
    func encode(fixed: [UInt32]) {
        size += (fixed.count<<2)
        return
    }
    private func sizeOfVarInt(value: UInt32) {
        if (value & (~0 << 7)) == 0 {
            size += 1
        } else if (value & (~0 << 14)) == 0 {
            size += 2
        } else if (value & (~0 << 21)) == 0 {
            size += 3
        } else if (value & (~0 << 28)) == 0 {
            size += 4
        } else {
            size += 5
        }
    }
    private func sizeOfVarInt(value: UInt64) {
        let v = Int64(bitPattern: value)
        // Handle two common special cases up front.
        if (v & (~0 << 7)) == 0 {
            size += 1
            return
        }
        if v < 0 {
            size += 10
            return
        }
        
        // Divide and conquer the remaining eight cases.
        var value = v
        var n = 2
        
        if (value & (~0 << 35)) != 0 {
            n += 4
            value >>= 28
        }
        if (value & (~0 << 21)) != 0 {
            n += 2
            value >>= 14
        }
        if (value & (~0 << 14)) != 0 {
            n += 1
        }
        size += n
    }
}
