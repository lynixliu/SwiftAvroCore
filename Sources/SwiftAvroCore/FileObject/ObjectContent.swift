//
//  Header.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
import Foundation

// MARK: - Header

public struct Header: Codable {
    private var magic: [UInt8]
    private var meta:  [String: [UInt8]]
    private var sync:  [UInt8]

    init() {
        magic = Array("Obj".utf8) + [1]           // "Obj" + version byte
        meta  = [:]
        sync  = withUnsafeBytes(of: UUID().uuid) { Array($0) }
    }

    var magicValue: [UInt8] { magic }
    var marker:     [UInt8] { sync  }

    var codec: String {
        get throws {
            guard let raw = meta[AvroReservedConstants.metaDataCodec] else {
                throw AvroCodingError.decodingFailed("Missing codec metadata")
            }
            return String(decoding: raw, as: UTF8.self)
        }
    }

    var schema: String {
        get throws {
            guard let raw = meta[AvroReservedConstants.metaDataSchema] else {
                throw AvroCodingError.decodingFailed("Missing schema metadata")
            }
            return String(decoding: raw, as: UTF8.self)
        }
    }

    mutating func addMetaData(key: String, value: [UInt8]) {
        meta[key] = value
    }

    mutating func setSchema(jsonSchema: String) {
        addMetaData(key: AvroReservedConstants.metaDataSchema, value: Array(jsonSchema.utf8))
    }

    mutating func setCodec(codec: String) {
        addMetaData(key: AvroReservedConstants.metaDataCodec, value: Array(codec.utf8))
    }
}

// MARK: - Block

public struct Block {
    public var objectCount: UInt64
    public var size:        UInt64
    public var data:        Data

    init() {
        objectCount = 0
        size        = 0
        data        = Data()
    }

    mutating func addObject(_ other: Data) {
        objectCount += 1
        size        += UInt64(other.count)
        data.append(other)
    }
}
