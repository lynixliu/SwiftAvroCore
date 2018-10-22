//
//  swift-avro-core/AvroPrimitiveProtocol.swift
//
//  Created by Yang Liu on 29/09/18.
//  Copyright © 2018 ___ORGANIZATIONNAME___ and the project authors.
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

protocol AvroPrimitiveEncodeProtocol {
    var buffer: [UInt8] {get set}
    var size: Int {get}
    func encodeNull()
    func append(_ other: AvroPrimitiveEncodeProtocol)
    func encode(_ value: Bool)
    func encode(_ value: Int)
    func encode(_ value: Int8)
    func encode(_ value: Int16)
    func encode(_ value: Int32)
    func encode(_ value: Int64)
    func encode(_ value: UInt)
    func encode(_ value: UInt8)
    func encode(_ value: UInt16)
    func encode(_ value: UInt32)
    func encode(_ value: UInt64)
    func encode(_ value: Float)
    func encode(_ value: Double)
    func encode(_ value: String)
    func encode(_ value: [UInt8])
    func encode(fixed: [UInt8])
    func encode(fixed: [UInt32])
}
protocol AvroBinaryDecodableProtocol {
    var available: Int {get set}
    func advance(_ size: Int)
    func decodeNull()
    func decode() throws -> Bool
    func decode() throws -> Int32
    func decode() throws -> Int64
    func decode() throws -> Int
    func decode() throws -> Int8
    func decode() throws -> Int16
    func decode() throws -> UInt
    func decode() throws -> UInt8
    func decode() throws -> UInt16
    func decode() throws -> UInt32
    func decode() throws -> UInt64
    func decode() throws -> Float
    func decode() throws -> Double
    func decode() throws -> String
    func decode() throws -> [UInt8]
    func decode(fixedSize: Int) throws -> [UInt8]
    func decode(fixedSize: Int) throws -> [UInt32]
}
