import Foundation

internal final class AvroBinaryDecoder: Decoder {
    
    /// required by Decoder
    internal var codingPath = [CodingKey]()
    internal var userInfo = [CodingUserInfoKey : Any]()
    
    // schema related
    var datum: AvroDatum
    let schema: AvroSchema
    
    internal init(schema: AvroSchema, data: Data) throws {
        self.schema = schema
        let decoder = AvroBinaryReader(data: data)
        let reader = AvroDatumReader(writerSchema: schema)
        self.datum = try reader.read(decoder: decoder)
    }

    internal init(other: AvroBinaryDecoder, datum: AvroDatum, codingPath: [CodingKey]? = nil) {
        self.schema = other.schema
        self.datum = datum
        self.codingPath = codingPath ?? other.codingPath
    }

    internal func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        if case .keyed(let keyedContext) = datum {
            return KeyedDecodingContainer(AvroKeyedDecodingContainer<Key>(decoder: self, datum: keyedContext, codingPath: self.codingPath))
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    internal func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        if case .array(let arrayDatum) = datum {
            return try AvroUnkeyedDecodingContainer(decoder: self, datum: arrayDatum, codingPath: codingPath)
        } else if case .primitive(.bytes(_)) = datum {
            return try AvroUnkeyedDecodingContainer(decoder: self, datum: datum.bytesToArray(), codingPath: codingPath)
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    internal func singleValueContainer() throws -> SingleValueDecodingContainer {
        if case .primitive(_) = datum {
            return try AvroSingleValueDecodingContainer(decoder: self, datum: datum, codingPath: codingPath)
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    internal func decode<T: Decodable>(_ type: T.Type) throws -> T {
        return try T(from: self)
    }
}
