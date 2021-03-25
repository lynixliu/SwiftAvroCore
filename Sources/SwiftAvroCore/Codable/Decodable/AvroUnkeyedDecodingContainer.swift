//
// Created by Kacper Kawecki on 25/03/2021.
//

import Foundation


internal struct AvroUnkeyedDecodingContainer: UnkeyedDecodingContainer {
    var isAtEnd: Bool {
        currentIndex == count
    }

    var codingPath: [CodingKey]
    var datum: [AvroDatum]
    var decoder: AvroBinaryDecoder
    private(set) var count: Int? = nil
    var currentIndex: Int

    init(decoder: AvroBinaryDecoder, datum: [AvroDatum], codingPath: [CodingKey]) throws {
        self.decoder = decoder
        self.datum = datum
        self.codingPath = codingPath
        self.count = datum.count
        self.currentIndex = 0
    }

    private mutating func increasingIndex<T>(callback: (AvroDatum) throws ->  T ) throws -> T{
        guard currentIndex < count ?? datum.count else {
            throw BinaryDecodingError.indexOutOfRange
        }

        let result = try callback(datum[currentIndex])
        currentIndex += 1
        return result
    }

    private mutating func increasingIndex<T>(callback: (AvroDatum, AvroBinaryDecoder) throws ->  T ) throws -> T{
        guard currentIndex < count ?? datum.count else {
            throw BinaryDecodingError.indexOutOfRange
        }

        let result = try callback(datum[currentIndex], decoder)
        currentIndex += 1
        return result
    }

    mutating func decodeNil() throws -> Bool {
        try increasingIndex() { currentDatum in try currentDatum.decodeNil()  }
    }

    @inlinable mutating func decode(_ type: Bool.Type) throws -> Bool {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }

    @inlinable mutating func decode(_ type: Int.Type) throws -> Int {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: Int8.Type) throws -> Int8 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: Int16.Type) throws -> Int16 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: Int32.Type) throws -> Int32 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: Int64.Type) throws -> Int64 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: UInt.Type) throws -> UInt {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: UInt8.Type) throws -> UInt8 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }

    @inlinable mutating func decode(_ type: UInt16.Type) throws -> UInt16 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: UInt32.Type) throws -> UInt32 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: UInt64.Type) throws -> UInt64 {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: Float.Type) throws -> Float {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }
    @inlinable mutating func decode(_ type: Double.Type) throws -> Double {
        try increasingIndex() { currentDatum in try currentDatum.decode() }
    }

    mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        if case .keyed(let keyedDatum) = datum[currentIndex] {
            return KeyedDecodingContainer(AvroKeyedDecodingContainer<NestedKey>(decoder: decoder, datum: keyedDatum, codingPath: codingPath))
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        if case .array(let arrayDatum) = datum[currentIndex] {
            return try AvroUnkeyedDecodingContainer(decoder: decoder, datum: arrayDatum, codingPath: codingPath)
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
    @inlinable mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        try increasingIndex { currentDatum, decoder in
            return try type.init(from: AvroBinaryDecoder(other: decoder, datum: currentDatum))
        }
    }

    mutating func superDecoder() throws -> Decoder {
        return AvroBinaryDecoder(other: decoder, datum: datum[currentIndex])
    }

}