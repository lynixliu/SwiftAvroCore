//
//  AvroReservedConstants.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//


// MARK: - AvroReservedConstants

public enum AvroReservedConstants {
    public static let metaDataSync      = "avro.sync"
    public static let metaDataCodec     = "avro.codec"
    public static let metaDataSchema    = "avro.schema"
    public static let metaDataReserved  = "avro"

    public static let nullCodec    = "null"
    public static let deflateCodec = "deflate"
    public static let xzCodec      = "xz"
    public static let lzfseCodec   = "lzfse"
    public static let lz4Codec     = "lz4"

    public static let syncSize            = 16
    public static let defaultSyncInterval = 4000 * 16

    public static let longScheme   = #"{"type":"long"}"#
    public static let markerScheme = #"{"type":"fixed","name":"Sync","size":16}"#

    public static let headerScheme = """
        {
          "type": "record",
          "name": "org.apache.avro.file.Header",
          "fields": [
            {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
            {"name": "meta",  "type": {"type": "map",   "values": "bytes"}},
            {"name": "sync",  "type": {"type": "fixed", "name": "Sync",  "size": 16}}
          ]
        }
        """

    public static let blockScheme = """
        {
          "type": "record",
          "name": "org.apache.avro.file.Block",
          "fields": [
            {"name": "count", "type": "long"},
            {"name": "data",  "type": "bytes"},
            {"name": "sync",  "type": {"type": "fixed", "name": "Sync", "size": 16}}
          ]
        }
        """

    public static let dummyRecordScheme = """
        {"type":"record","name":"xx","fields":[]}
        """
}

// MARK: - ObjectContainerContext

/// Shared, immutable configuration passed to both writer and reader.
/// Owns schema resolution and codec — neither writer nor reader need
/// to know how these were constructed.
public struct ObjectContainerContext {
    public let schema:    String
    public let codecName: String
    public let codec:     any CodecProtocol

    // Pre-parsed schemas used by writer and reader — parsed once here,
    // never duplicated into the structs that consume them.
    let headerSchema: AvroSchema
    let longSchema:   AvroSchema
    let markerSchema: AvroSchema

    /// Creates a context for the given Avro schema string and codec.
    /// Parses all required internal schemas eagerly so writer and reader
    /// can be constructed without further schema work.
    public init(schema: String, codec: any CodecProtocol = NullCodec()) throws {
        self.schema    = schema
        self.codecName = codec.name
        self.codec     = codec

        let core = SwiftAvroCore()
        headerSchema = try Self.requiredSchema(core, AvroReservedConstants.headerScheme)
        longSchema   = try Self.requiredSchema(core, AvroReservedConstants.longScheme)
        markerSchema = try Self.requiredSchema(core, AvroReservedConstants.markerScheme)
    }

    private static func requiredSchema(_ core: SwiftAvroCore, _ json: String) throws -> AvroSchema {
        guard let s = core.newSchema(schema: json) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        return s
    }
}

