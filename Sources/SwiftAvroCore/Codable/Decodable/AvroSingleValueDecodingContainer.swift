//
//  AvroKeyedDecodingContainer.swift
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

internal struct AvroSingleValueDecodingContainer: SingleValueDecodingContainer {
    var codingPath: [CodingKey]
    var decoder: AvroBinaryDecoder
    var datum: AvroDatum

    init(decoder: AvroBinaryDecoder, datum: AvroDatum, codingPath: [CodingKey]) throws {
        self.decoder = decoder
        self.codingPath = codingPath
        self.datum = datum
    }

    func decodeNil() -> Bool {
        (try? datum.decodeNil()) ?? false
    }

    @inlinable func decode(_ type: Bool.Type) throws -> Bool {
        try datum.decode()
    }

    @inlinable func decode(_ type: Int.Type) throws -> Int {
        try datum.decode()
    }
    @inlinable func decode(_ type: Int8.Type) throws -> Int8 {
        try datum.decode()
    }
    @inlinable func decode(_ type: Int16.Type) throws -> Int16 {
        try datum.decode()
    }
    @inlinable func decode(_ type: Int32.Type) throws -> Int32 {
        try datum.decode()
    }
    @inlinable func decode(_ type: Int64.Type) throws -> Int64 {
        try datum.decode()
    }
    @inlinable func decode(_ type: UInt.Type) throws -> UInt {
        try datum.decode()
    }
    @inlinable func decode(_ type: UInt8.Type) throws -> UInt8 {
        try datum.decode()
    }

    @inlinable func decode(_ type: UInt16.Type) throws -> UInt16 {
        try datum.decode()
    }
    @inlinable func decode(_ type: UInt32.Type) throws -> UInt32 {
        try datum.decode()
    }
    @inlinable func decode(_ type: UInt64.Type) throws -> UInt64 {
        try datum.decode()
    }
    @inlinable func decode(_ type: Float.Type) throws -> Float {
        try datum.decode()
    }
    @inlinable func decode(_ type: Double.Type) throws -> Double {
        try datum.decode()
    }

    @inlinable func decode(_ type: String.Type) throws -> String {
        try datum.decode()
    }

    @inlinable func decode(_ type: String.Type) throws -> Data {
        try datum.decode()
    }

    @inlinable func decode(_ type: String.Type) throws -> UUID {
        try datum.decode()
    }

    @inlinable func decode(_ type: String.Type) throws -> Decimal {
        try datum.decode()
    }

    @inlinable func decode(_ type: String.Type) throws -> Date {
        try datum.decode()
    }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        try T(from: decoder)
    }
}