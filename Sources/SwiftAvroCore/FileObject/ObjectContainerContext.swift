//
//  ObjectContainerContext.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
//  Shared, immutable configuration for Avro object container file I/O.
//  Parsed once and handed to both ObjectContainerWriter and ObjectContainerReader
//  so neither type needs to own schema-parsing logic.

import Foundation

// MARK: - ObjectContainerContext

/// Immutable configuration shared between a writer and a reader.
///
/// Owns the codec and all pre-parsed internal schemas (header, long, marker).
/// Constructing this once and reusing it across multiple writers/readers avoids
/// redundant schema parsing.
struct ObjectContainerContext {
    internal let schema:    String
    internal let codecName: String
    internal let codec:     any CodecProtocol

    // Pre-parsed schemas — built once here, shared read-only with writer/reader.
    let headerSchema: AvroSchema
    let longSchema:   AvroSchema
    let markerSchema: AvroSchema

    /// Creates a context for the given Avro schema string and codec.
    ///
    /// All required internal schemas are parsed eagerly so that writer and
    /// reader construction is guaranteed to succeed once a valid context exists.
    public init(schema: String, codec: any CodecProtocol = NullCodec()) throws {
        self.schema    = schema
        self.codecName = codec.name
        self.codec     = codec

        let avro = Avro()
        headerSchema = try Self.requiredSchema(avro, AvroReservedConstants.headerScheme)
        longSchema   = try Self.requiredSchema(avro, AvroReservedConstants.longScheme)
        markerSchema = try Self.requiredSchema(avro, AvroReservedConstants.markerScheme)

        // Validate the user-supplied schema eagerly.
        guard avro.decodeSchema(schema: schema) != nil else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }

    private static func requiredSchema(_ avro: Avro, _ json: String) throws -> AvroSchema {
        guard let s = avro.newSchema(schema: json) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        return s
    }
}
