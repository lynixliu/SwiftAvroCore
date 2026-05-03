//
// AvroClient/AvroError.swift - Error constants
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

/// Describes errors that can occur when decoding a schema.
public enum AvroSchemaDecodingError: Error {
    case unknownSchemaJsonFormat
    case unnamedSchema
    case emptyType
    case typeDuplicateBranchInUnion
}

/// Describes errors that can occur when encoding a schema.
public enum AvroSchemaEncodingError: Error {
    /// The definition of the encoding data invalid, typically, the data type is not
    /// defined in Avro Schema
    case invalidSchemaType
}

/// Describes errors that can occur when encoding a value to Avro binary or JSON.
public enum BinaryEncodingError: Error, Equatable {
    case noSchemaSpecified
    case noEncoderSpecified
    case anyTranscodeFailure
    case typeMismatchWithSchema
    case notFoundInUnionBranches
    case invalidUnionIndex
    case invalidSchema
    case invalidDecimal

    case typeMismatchWithSchemaBool
    case typeMismatchWithSchemaInt
    case typeMismatchWithSchemaInt8
    case typeMismatchWithSchemaInt16
    case typeMismatchWithSchemaInt32
    case typeMismatchWithSchemaInt64
    case typeMismatchWithSchemaUInt
    case typeMismatchWithSchemaUInt8
    case typeMismatchWithSchemaUInt16
    case typeMismatchWithSchemaUInt32
    case typeMismatchWithSchemaUInt64
    case typeMismatchWithSchemaFloat
    case typeMismatchWithSchemaDouble
    case typeMismatchWithSchemaString
    case typeMismatchWithSchemaNil

    /// UInt value exceeds Int64.max — Avro long is signed 64-bit
    case uintOverflow
    case missingRequiredFields
}

/// Describes errors that can occur when decoding a value from Avro binary.
public enum BinaryDecodingError: Error, Equatable {
    case outOfBufferBoundary
    case malformedAvro
    case indexOutofBoundary

    case typeMismatchWithSchemaBool
    case typeMismatchWithSchemaInt
    case typeMismatchWithSchemaInt8
    case typeMismatchWithSchemaInt16
    case typeMismatchWithSchemaInt32
    case typeMismatchWithSchemaInt64
    case typeMismatchWithSchemaUInt
    case typeMismatchWithSchemaUInt8
    case typeMismatchWithSchemaUInt16
    case typeMismatchWithSchemaUInt32
    case typeMismatchWithSchemaUInt64
    case typeMismatchWithSchemaFloat
    case typeMismatchWithSchemaDouble
    case typeMismatchWithSchemaString
}

/// Describes errors that can occur in Schema Resolution.
public enum AvroSchemaResolutionError: Error {
    case WriterFieldMissingWithoutDefaultValue
    case SchemaMismatch
}

public enum AvroDeflateCodexError: Error {
    case SourceDataSizeInvalid
    case InitDecodeStreamFailed
    case CompressionStatusError
}

public enum AvroHandshakeError: Error {
    case noClientHash
    case noServerHash
    case invalidClientHashLength
    case sessionNotFound
    /// Thrown when a required schema for the named message cannot be found in
    /// the session cache, preventing safe encode or decode of that message.
    case missingSchema(String)
    /// Thrown when the `clientProtocol` name supplied to ``AvroIPCRequest`` or
    /// ``MessageRequest`` is not a member of the ``AvroIPCContext/knownProtocols``
    /// set. Only raised when `knownProtocols` is non-nil (closed deployments).
    case unknownProtocol(String)
}

public enum AvroMessageError: Error {
    case requestParamterCountError
    case errorIdOutofRangeError
}

public enum AvroCodingError: Error {
    case encodingFailed(String)
    case decodingFailed(String)
}
