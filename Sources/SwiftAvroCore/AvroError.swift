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
///
/// Enum constants that identify the particular error.
///
// -----------------------------------------------------------------------------
//extension AvroSchema {
/// Describes errors that can occur when decoding a message from binary format.
public enum AvroSchemaDecodingError: Error {
    /// The definition of the message or one of its nested messages has required
    /// fields but the message being encoded did not include values for them. You
    /// must pass `partial: true` during encoding if you wish to explicitly ignore
    /// missing required fields.
    case unknownSchemaJsonFormat
    case unnamedSchema
    case emptyType
    case typeDuplicateBranchInUnion
}
//}
/// Describes errors that can occur when decoding a message from binary format.
public enum AvroSchemaEncodingError: Error {
    /// The definition of the encoding data invalid, typically, the data type is not
    /// defined in Avro Schema
    case invalidSchemaType
}
/// Describes errors that can occur when decoding a message from binary format.
public enum BinaryEncodingError: Error {
  /// `Any` fields that were decoded from JSON cannot be re-encoded to binary
  /// unless the object they hold is a well-known type or a type registered via
  /// `Google_Protobuf_Any.register()`.
    case noSchemaSpecified
    case noEncoderSpecified
    case anyTranscodeFailure
    case typeMismatchWithSchema
    case invalidUnionIndex
    case invalidSchema
    case invalidDecimal
  /// The definition of the message or one of its nested messages has required
  /// fields but the message being encoded did not include values for them. You
  /// must pass `partial: true` during encoding if you wish to explicitly ignore
  /// missing required fields.
  case missingRequiredFields
}

public enum BinaryDecodingError: Error {
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
    /// The definition of the encoding data invalid, typically, the data type is not
    /// defined in Avro Schema
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
}
