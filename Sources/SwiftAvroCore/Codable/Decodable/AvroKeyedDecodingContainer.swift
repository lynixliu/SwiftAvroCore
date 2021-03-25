//
// Created by Kacper Kawecki on 24/03/2021.
//

import Foundation

internal struct AvroKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {

    var allKeys: [K] {
        return datum.keys.reduce(into: [K]()) { keys, key in
            guard let key = K.init(stringValue: key) else {
                return
            }
            keys.append(key)
        }
    }

    var codingPath: [CodingKey] = []
    let datum: [String: AvroDatum]
    fileprivate var decoder: AvroBinaryDecoder

    init(decoder: AvroBinaryDecoder, datum: [String: AvroDatum], codingPath: [CodingKey]) {
        self.decoder = decoder
        self.datum = datum
        self.codingPath = codingPath
    }

    func contains(_ key: K) -> Bool {
        return datum.keys.contains(key.stringValue)
    }

    func datumFor(_ key: K) throws -> AvroDatum {
        if let value = datum[key.stringValue] {
            return value
        } else {
            throw BinaryDecodingError.unknownKey(key.stringValue)
        }
    }

    func decodeNil(forKey key: K) throws -> Bool {
        try datumFor(key).decodeNil()
    }

    @inlinable func decode(_ type: Bool.Type, forKey key: K) throws -> Bool {
        try datumFor(key).decode()
    }

    @inlinable func decode(_ type: Int.Type, forKey key: K) throws -> Int {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: Int8.Type, forKey key: K) throws -> Int8 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: Int16.Type, forKey key: K) throws -> Int16 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: Int32.Type, forKey key: K) throws -> Int32 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: Int64.Type, forKey key: K) throws -> Int64 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: UInt.Type, forKey key: K) throws -> UInt {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8 {
        try datumFor(key).decode()
    }

    @inlinable func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64 {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: Float.Type, forKey key: K) throws -> Float {
        try datumFor(key).decode()
    }
    @inlinable func decode(_ type: Double.Type, forKey key: K) throws -> Double {
        try datumFor(key).decode()
    }

//    @inlinable func decode(_ type: [UInt8].Type, forKey key: K) throws -> [UInt8] {
//        try datumFor(key).decode()
//    }

    @inlinable func decode(_ type: String.Type, forKey key: K) throws -> String {
        try datumFor(key).decode()
    }

    //    @inlinable mutating func decode(_ type: [UInt32].Type, forKey key: K) throws -> [UInt32] {
//        let sch = self.schema(key)
//        switch sch {
//        case .fixedSchema(let fixed)
//            return try decoder.primitive.decode(fixedSize: fixed.size)
//        default:
//            throw BinaryDecodingError.typeMismatchWithSchema
//        }
//    }

    @inlinable func decode(_ type: String.Type, forKey key: K) throws -> Data {
        try datumFor(key).decode()
    }

    @inlinable func decode(_ type: String.Type, forKey key: K) throws -> UUID {
        try datumFor(key).decode()
    }

    @inlinable func decode(_ type: String.Type, forKey key: K) throws -> Decimal {
        try datumFor(key).decode()
    }

    @inlinable func decode(_ type: String.Type, forKey key: K) throws -> Date {
        try datumFor(key).decode()
    }

    @inlinable func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T : Decodable {
        return try T(from: superDecoder(forKey: key))
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        let datum = try datumFor(key)
        if case .keyed(let keyedDatum) = datum {
            let newDecoder = AvroBinaryDecoder(other: decoder, datum: datum)
            var newCodingPath = codingPath
            newCodingPath.append(key)
            return KeyedDecodingContainer(AvroKeyedDecodingContainer<NestedKey>(decoder: newDecoder, datum: keyedDatum, codingPath: newCodingPath))
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        let datum = try datumFor(key)
        let newDecoder = AvroBinaryDecoder(other: decoder, datum: datum)
        var newCodingPath = codingPath
        switch datum {
        case .array(let arrayDatum):
            newCodingPath.append(key)
            return try AvroUnkeyedDecodingContainer(decoder: newDecoder, datum: arrayDatum, codingPath: newCodingPath)
        case .primitive(.bytes(_)):
            return try AvroUnkeyedDecodingContainer(decoder: newDecoder, datum: datum.bytesToArray(), codingPath: newCodingPath)
        case .logical(.duration(_)):
            return try AvroUnkeyedDecodingContainer(decoder: newDecoder, datum: datum.durationToArray(), codingPath: newCodingPath)
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    func superDecoder() throws -> Decoder {
        return decoder
    }

    func superDecoder(forKey key: K) throws -> Decoder {
        let datum = try datumFor(key)
        return AvroBinaryDecoder(other: decoder, datum: datum)
    }
}