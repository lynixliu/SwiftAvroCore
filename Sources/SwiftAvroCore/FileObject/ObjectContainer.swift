//
//  AvroClient/AvroObjectFile.swift
//
//  Created by Yang Liu on 21/09/18.
//  Copyright © 2018 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

public struct AvroReservedConstants {
    public static let MetaDataSync:String = "avro.sync"
    public static let MetaDataCodec:String = "avro.codec"
    public static let MetaDataSchema:String = "avro.schema"
    public static let MetaDataReserved:String = "avro"
    
    public static let NullCodec = "null"
    public static let DeflateCodec = "deflate"
    public static let XZCodec = "xz"
    public static let LZFSECodec = "lzfse"
    public static let LZ4Codec = "lz4"
    
    public static let SyncSize = 16
    public static let DefaultSyncInterval = 4000 * 16
    
    public static let longScheme = """
{"type" : "long"}
"""
    public static let markerScheme = """
{"type": "fixed", "name": "Sync", "size": 16}
"""
    public static let headerScheme = """
{"type": "record", "name": "org.apache.avro.file.Header",
"fields" : [
{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
{"name": "meta", "type": {"type": "map", "values": "bytes"}},
{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
]
}
"""
    public static let blockScheme = """
{"type": "record", "name": "org.apache.avro.file.Block",
"fields" : [
{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
{"name": "meta", "type": {"type": "map", "values": "bytes"}},
{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
]
}
"""
    public static let dummyRecordScheme = """
{
"type": "record",
"name": "xx",
"fields" : [
]
}
"""
}

public struct ObjectContainer {
    public var header: Header
    public var blocks: [Block]
    private let core: Avro
    private let headerSchema: AvroSchema
    private let longSchema: AvroSchema
    private let markerSchema: AvroSchema
    
    public init(schema: String? = nil, codec: CodecProtocol) throws {
        header = Header()
        header.setSchema(jsonSchema:schema ?? AvroReservedConstants.dummyRecordScheme)
        header.setCodec(codec:codec.getName())
        blocks = [Block]()
        core = Avro()
        headerSchema = core.newSchema(schema: AvroReservedConstants.headerScheme)!
        longSchema = core.newSchema(schema: AvroReservedConstants.longScheme)!
        markerSchema = core.newSchema(schema: AvroReservedConstants.markerScheme)!
        guard core.decodeSchema(schema: header.schema) != nil else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }

    var headerSize:Int {
        let sizer = AvroEncoder()
        if let sz = try? sizer.sizeOf(header, schema: headerSchema) {
            return sz
        }
        return 0
    }
    
    mutating func setMetaItem(key: String, value: [UInt8]) {
        header.addMetaData(key:key, value:value)
    }

    public mutating func addObject<T: Codable>(_ type: T) throws {
        if let d = try? core.encode(type) {
            var block = Block()
            block.addObject(d)
            blocks.append(block)
        }
    }
    
    public mutating func addObjects<T: Codable>(_ types: [T]) throws {
        var block = Block()
        for type in types {
            let d = try core.encode(type)
            block.addObject(d)
        }
        blocks.append(block)
    }
    
    mutating func addObjectsToBlocks<T: Codable>(_ types: [T], objectsInBlock: Int) throws {
        var block = Block()
        var i = 0
        for type in types {
            if i < objectsInBlock {
                if let d = try? core.encode(type) {
                    block.addObject(d)
                }
                i += 1
                continue
            }
            blocks.append(block)
            i = 0
        }
    }
    
    public func encodeHeader() throws -> Data? {
       return try? core.encodeFrom(header, schema: headerSchema)
    }
    
    
    public func decodeObjects<T: Decodable>() throws -> [T] {
        return try decodeObjectsHelper() { (remainingData, objectSchema) in
            let (obj, decodedBytes) = try core.decodeFromContinue(from: remainingData, schema: objectSchema) as (T, Int)
            return (obj, decodedBytes)
        }
    }

    public func decodeObjects() throws -> [Any?] {
        return try decodeObjectsHelper() { (remainingData, objectSchema) in
            let (obj, decodedBytes) = try core.decodeFromContinue(from: remainingData, schema: objectSchema)
            return (obj, decodedBytes)
        }
    }
    private func decodeObjectsHelper<T>(objectDecoder: ((Data, AvroSchema) throws -> (T?, Int))) throws -> [T] {
        let objectSchemaFromHeader = header.schema
        let objectSchema = core.decodeSchema(schema: objectSchemaFromHeader)!
        var result: [T] = []
        for block in blocks {
            var remainingData = block.data
            var objectsDecoded = 0
            while remainingData.count > 0 && objectsDecoded < Int(block.objectCount) {
                let (obj, decodedBytes) = try objectDecoder(remainingData, objectSchema)
                remainingData = remainingData.subdata(in: decodedBytes..<remainingData.count)
                if let decodedObj = obj {
                    result.append(decodedObj)
                    objectsDecoded += 1
                }
            }
            guard objectsDecoded == Int(block.objectCount) else {
                throw AvroError.decodingError("Expected to decode \(block.objectCount) objects in block, but decoded \(objectsDecoded) objects")
            }
        }
        return result
    }

    
    
    public func encodeObject() throws -> Data {
        var d: Data
        d = try core.encodeFrom(header, schema: headerSchema)
        for block in blocks {
            if let objectCount = try? core.encodeFrom(block.objectCount, schema:longSchema) {
                d.append(objectCount)
            }
            if let size = try? core.encodeFrom(block.size, schema:longSchema) {
                d.append(size)
            }
            d.append(block.data)
            d.append(contentsOf: header.marker)
        }
        return d
    }
    
    public mutating func decodeFromData(from: Data) throws {
        try decodeHeader(from: from)
        let start = findMarker(from: from)
        try decodeBlock(from: from.subdata(in: start..<from.count))
    }
    
    public mutating func decodeHeader(from: Data) throws {
        try decodeHeader(from: from)
        let start = findMarker(from: from)
        try decodeBlock(from: from.subdata(in: start..<from.count))
    }
    
    public mutating func decodeHeader(from: Data) throws {
        if let hdr = try core.decodeFrom(from: from, schema: headerSchema) as Header? {
            self.header = hdr
        }
    }
    
    public func findMarker(from: Data) -> Int {
        for loc in 0..<from.count {
            let sub = from.subdata(in: loc..<loc+header.marker.count)
            if sub.elementsEqual(header.marker) {
                return loc+header.marker.count
            }
        }
        return 0
    }

    public mutating func decodeBlock(from: Data) throws {
        from.withUnsafeBytes{ (pointer: UnsafePointer<UInt8>) in
            let decoder = AvroPrimitiveDecoder(pointer:pointer, size:from.count)
            var block = Block()
            if let objectCount = try? decoder.decode()as UInt64 {
                block.objectCount = objectCount
            }
            if let value = try? decoder.decode() as [UInt8] {
                block.data.append(contentsOf: value)
                block.size = UInt64(block.data.count)
                blocks.append(block)
            }
        }
    }

}

struct Header:Codable {
    private var magic: [UInt8]
    private var meta: [String : [UInt8]]
    private var sync: [UInt8]
    
    init(){
        let version: UInt8 = 1
        self.magic = "Obj".utf8.map{UInt8($0)} + [version]
        self.meta = Dictionary<String, [UInt8]>()
        self.sync = withUnsafeBytes(of: UUID().uuid) {buf in [UInt8](buf)}
    }
    var magicValue: [UInt8] {
        return magic
    }
    var marker: [UInt8] {
        return sync
    }
    var codec: String {
        return String(decoding:meta[AvroReservedConstants.MetaDataCodec]!, as:UTF8.self)
    }
    var schema:String {
        return String(decoding:meta[AvroReservedConstants.MetaDataSchema]!, as:UTF8.self)
    }
    
    mutating func addMetaData(key: String, value: [UInt8]) {
        self.meta[key] = value
    }
    
    mutating func setSchema(jsonSchema: String) {
        addMetaData(key:AvroReservedConstants.MetaDataSchema, value:Array(jsonSchema.utf8))
    }
    mutating func setCodec(codec: String) {
        addMetaData(key:AvroReservedConstants.MetaDataCodec, value:Array(codec.utf8))
    }
    
}

public struct Block {
    public var objectCount: UInt64
    public var size: UInt64
    public var data: Data
    
    init(){
        objectCount = 0
        size = 0
        data = Data()
    }
    
    mutating func addObject(_ other: Data) {
        objectCount += 1
        size += UInt64(other.count)
        data.append(other)
    }
}
