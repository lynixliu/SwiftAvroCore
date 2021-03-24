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

class AvroPrimitiveDecoder : AvroBinaryDecodableProtocol {
    fileprivate var pointer: UnsafePointer<UInt8>
    var available: Int
    fileprivate let size: Int
    
    init(pointer: UnsafePointer<UInt8>, size: Int) {
        self.pointer = pointer
        self.available = size
        self.size = size
    }
    
    public var read: Int {
        return (size - available)
    }
    
    internal func byte() throws -> UInt8 {
        if available < 1 {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let result = pointer[0]
        advance(1)
        return result
    }
    
    internal func advance(_ size: Int) {
        pointer += size
        available -= size
    }
    
    fileprivate func update(pointer: UnsafePointer<UInt8>, available: Int) {
        self.pointer = pointer
        self.available = available
    }

    /// do nothing for Null
    func decodeNull() {
        return
    }
    
    func decode() throws -> Bool {
        return try byte() > 0
    }
    
    func decode() throws -> UInt8 {
        return try byte()
    }
    
    func decode() throws -> Int32 {
        let varint = try decodeVarint()
        let t = UInt32(truncatingIfNeeded: varint)
        return t.decodeZigZag()
    }
    
    func decode() throws -> Int64 {
        let varint = try decodeVarint()
        return varint.decodeZigZag()
    }
    
    func decode() throws -> Int {
        return Int(try decode() as Int64)
    }
    
    func decode() throws -> Int8 {
        return Int8(try decode() as Int64)
    }
    
    func decode() throws -> Int16 {
        return Int16(try decode() as Int64)
    }
    
    func decode() throws -> UInt {
        return UInt(try decode() as Int64)
    }
    
    func decode() throws -> UInt16 {
        return UInt16(try decode() as Int64)
    }
    
    func decode() throws -> UInt32 {
        var result: UInt32 = 0
        try decodeByteNumber(value: &result)
        return result
    }
    
    func decode() throws -> UInt64 {
        return UInt64(try decode() as Int64)
    }
    
    func decode() throws -> Float {
        var result: Float = 0
        try decodeByteNumber(value: &result)
        return result
    }
    
    func decode() throws -> Double {
        var result: Double = 0
        try decodeByteNumber(value: &result)
        return result
    }
    
    func decode() throws -> String {
        let bytes = try decode() as [UInt8]
        guard let string = String(bytes: bytes, encoding: String.Encoding.utf8) else {
            throw BinaryDecodingError.malformedAvro
        }
        return string
    }
    
    func decode() throws -> [UInt8] {
        let length = try decode() as Int64
        guard available >= length else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let buffer = UnsafeBufferPointer(start: pointer, count: Int(length));
        advance(Int(length))
        return Array(buffer)
    }
    
    func decode(fixedSize: Int) throws -> [UInt8] {
        if available < fixedSize {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        let buffer = UnsafeBufferPointer(start: pointer, count: fixedSize);
        advance(fixedSize)
        return Array(buffer)
    }
    
    func decode(fixedSize: Int) throws -> [UInt32] {
        if available < fixedSize {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        var result: [UInt32] = []
        let duration = fixedSize << 2
        for _ in 0..<duration {
            var value: UInt32 = 0
            try decodeByteNumber(value: &value)
            result.append(value)
        }
        return result
    }
    
    /// decode a fixed-length <value>-byte number.  This generic
    /// helper handles all four/geight-byte number types.
    private func decodeByteNumber<T>(value: inout T) throws {
        let size = MemoryLayout<T>.size
        if available < size {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        withUnsafeMutablePointer(to: &value) { ip -> Void in
            let dest = UnsafeMutableRawPointer(ip).assumingMemoryBound(to: UInt8.self)
            let src = UnsafeRawPointer(pointer).assumingMemoryBound(to: UInt8.self)
            dest.initialize(from: src, count: size)
        }
        advance(size)
    }
    
    /// Parse the next raw varint from the input.
    private func decodeVarint() throws -> UInt64 {
        var c = try byte()
        var value = UInt64(c & 0x7f)
        var shift = UInt64(7)
        while (c & 0x80) != 0  {
            c = try byte()
            value |= UInt64(c & 0x7f) << shift
            shift += 7
        }
        return value
    }
}
