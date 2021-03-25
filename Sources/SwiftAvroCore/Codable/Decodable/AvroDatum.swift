//
// Created by Kacper Kawecki on 25/03/2021.
//

import Foundation

internal enum AvroDatum {
    case primitive(AvroPrimitiveValue)
    case array([AvroDatum])
    case keyed([String: AvroDatum])

    func bytesToArray() throws -> [AvroDatum] {
        if case .primitive(.bytes(let bytes)) = self {
            var parsedBytes: [AvroDatum] = []
            for byte in bytes {
                parsedBytes.append(.primitive(.byte(byte)))
            }
            return parsedBytes
        } else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
}

extension AvroDatum {
    func decodeNil() throws -> Bool {
        if case .primitive(.null) = self {
            return true
        }
        return false
    }

    @inlinable func decode() throws -> Bool {
        guard case .primitive(.boolean(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return value
    }

    @inlinable func decode() throws -> Int {
        guard case .primitive(.int(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return Int(value)
    }
    @inlinable func decode() throws -> Int8 {
        guard case .primitive(.int(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return Int8(value)
    }
    @inlinable func decode() throws -> Int16 {
        guard case .primitive(.int(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return Int16(value)
    }
    @inlinable func decode() throws -> Int32 {
        guard case .primitive(.int(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return Int32(value)
    }
    @inlinable func decode() throws -> Int64 {
        guard case .primitive(.long(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return value
    }
    @inlinable func decode() throws -> UInt {
        guard case .primitive(.int(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return UInt(value)
    }
    @inlinable func decode() throws -> UInt8 {
        switch self {
        case .primitive(.byte(let value)):
            return value
        case .primitive(.int(let value)):
            return UInt8(value)
        case .primitive(.bytes(let bytes)):
            if bytes.count == 1 {
                return bytes[0]
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> UInt16 {
        switch self {
        case .primitive(.int(let value)):
            return UInt16(value)
        case .primitive(.bytes(let bytes)):
            if bytes.count == 2 {
                let data = Data(bytes)
                return UInt16(data.withUnsafeBytes { $0.load(as: UInt16.self) })
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
    @inlinable func decode() throws -> UInt32 {
        switch self {
        case .primitive(.int(let value)):
            return UInt32(value)
        case .primitive(.bytes(let bytes)):
            if bytes.count == 4 {
                let data = Data(bytes)
                return UInt32(data.withUnsafeBytes { $0.load(as: UInt32.self) })
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
    @inlinable func decode() throws -> UInt64 {
        switch self {
        case .primitive(.long(let value)):
            return UInt64(value)
        case .primitive(.bytes(let bytes)):
            if bytes.count == 8 {
                let data = Data(bytes)
                return UInt64(data.withUnsafeBytes { $0.load(as: UInt64.self) })
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
    @inlinable func decode() throws -> Float {
        guard case .primitive(.float(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return value
    }
    @inlinable func decode() throws -> Double {
        guard case .primitive(.double(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return value
    }

    @inlinable func decode() throws -> [UInt8] {
        guard case .primitive(.bytes(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return value
    }

    @inlinable func decode() throws -> String {
        guard case .primitive(.string(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return value
    }

//    @inlinable func decode() throws -> [UInt32] {
//        let sch = self.schema(key)
//        switch sch {
//        case .fixedSchema(let fixed)
//            return try decoder.primitive.decode(fixedSize: fixed.size)
//        default:
//            throw BinaryDecodingError.typeMismatchWithSchema
//        }
//
}