//
//  AvroBinaryReader.swift
//
//  Created by Kacper Kawecki on 24/03/2021.
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

class AvroBinaryReader {
    let data: Data
    var iterator: Data.Iterator

    init(data: Data) {
        self.data = data
        self.iterator = data.makeIterator()
    }

    convenience init(bytes: [UInt8]) {
        self.init(data: Data(bytes))
    }

    private func byte() throws -> UInt8 {
        guard let byte = self.iterator.next() else {
            throw BinaryDecodingError.outOfBufferBoundary
        }
        return byte
    }

    func read(_ count: Int) throws -> [UInt8] {
        return try read(Int64(count))
    }

    func read(_ count: Int64) throws -> [UInt8] {
        guard count >= 0 else {
            throw BinaryDecodingError.malformedAvro
        }
        var bytes: [UInt8] = []
        for _ in 0..<count {
            bytes.append(try byte())
        }
        return bytes
    }

    func readNull() {
        return
    }

    func readBoolean() throws -> Bool {
        return try byte() == 1
    }

    func readInt() throws -> Int64 {
        return try readLong()
    }

    func readLong() throws -> Int64 {
        return try readVarInt().decodeZigZag()
    }

    func readFloat() throws -> Float {
        let data = Data(try read(4))
        return Float(bitPattern: UInt32(littleEndian: data.withUnsafeBytes { $0.load(as: UInt32.self) }))
    }

    func readDouble() throws -> Double {
        let data = Data(try read(8))
        return Double(bitPattern: UInt64(littleEndian: data.withUnsafeBytes { $0.load(as: UInt64.self) }))
    }

    func readBytes() throws -> [UInt8] {
        try read(readLong())
    }

    func readString() throws -> String {
        let bytes = try readBytes()
        guard let string = String(bytes: bytes, encoding: String.Encoding.utf8) else {
            throw BinaryDecodingError.malformedAvro
        }
        return string
    }

    /// Parse the next raw varInt from the input.
    private func readVarInt() throws -> UInt64 {
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

    func skip(_ count: Int) throws {
        return try skip(Int64(count))
    }

    func skip(_ count: Int64) throws {
        guard count >= 0 else {
            throw BinaryDecodingError.malformedAvro
        }
        for _ in 0..<count {
            let _ = try byte()
        }
    }

    func skipNull() {}

    func skipBoolean() throws {
        try skip(1)
    }

    func skipInt() throws {
        try skipLong()
    }

    func skipLong() throws {
        var c = try byte()
        while (c & 0x80) != 0  {
            c = try byte()
        }
    }

    func skipFloat() throws {
        try skip(4)
    }

    func skipDouble() throws {
        try skip(8)
    }

    func skipBytes() throws {
        try skip(readLong())
    }

    func skipString() throws {
        try skipBytes()
    }
}