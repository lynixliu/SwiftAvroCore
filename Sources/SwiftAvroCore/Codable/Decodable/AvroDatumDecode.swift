//
//  AvroDatum.swift
//
//  Created by Kacper Kawecki on 25/03/2021.
//  Copyright © 2021 by Kacper Kawecki and the project authors.
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

import Foundation


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
        switch self {
        case .primitive(.int(let value)):
            return Int(value)
        case .logical(.date(let value)), .logical(.timeMillis(let value)):
            return Int(value)
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> Int8 {
        guard case .primitive(.int(let value)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return Int8(value)
    }

    @inlinable func decode() throws -> Int16 {
        switch self {
        case .primitive(.int(let value)):
            return Int16(value)
        case .logical(.date(let value)), .logical(.timeMillis(let value)):
            return Int16(value)
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> Int32 {
        switch self {
        case .primitive(.int(let value)):
            return Int32(value)
        case .logical(.date(let value)), .logical(.timeMillis(let value)):
            return Int32(value)
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> Int64 {
        switch self {
        case .primitive(.long(let value)), .primitive(.int(let value)):
            return value
        case .logical(.timeMicros(let value)), .logical(.timestampMillis(let value)), .logical(.timestampMicros(let value)):
            return value
        case .logical(.localTimestampMicros(let value)), .logical(.localTimestampMillis(let value)):
            return value
        case .logical(.date(let value)), .logical(.timeMillis(let value)):
            return value
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
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
                return UInt16(data.withUnsafeBytes {
                    $0.load(as: UInt16.self)
                })
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> UInt32 {
        switch self {
        case .primitive(.durationElement(let value)):
            return value
        case .primitive(.int(let value)):
            return UInt32(value)
        case .primitive(.bytes(let bytes)):
            if bytes.count == 4 {
                let data = Data(bytes)
                return UInt32(data.withUnsafeBytes {
                    $0.load(as: UInt32.self)
                })
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
                return UInt64(data.withUnsafeBytes {
                    $0.load(as: UInt64.self)
                })
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
        switch self {
        case .primitive(.double(let value)):
            return value
        case .primitive(.float(let value)):
            return Double(value)
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> Data {
        guard case .primitive(.bytes(let bytes)) = self else {
            throw BinaryDecodingError.typeMismatchWithSchema
        }
        return Data(bytes)
    }

    @inlinable func decode() throws -> String {
        switch self {
        case .primitive(.string(let value)):
            return value
        case .logical(.uuid(let value)):
            return value
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    // Logical values
    @inlinable func decode() throws -> Decimal {
        switch self {
        case .logical(.decimal(_, precision: _, scale: _)):
            throw BinaryDecodingError.notImplemented
        default:
            throw BinaryDecodingError.malformedAvro
        }
    }

    @inlinable func decode() throws -> UUID {
        switch self {
        case .logical(.uuid(let value)):
            if let result = UUID(uuidString: value) {
                return result
            } else {
                throw BinaryDecodingError.malformedAvro
            }
        case .primitive(.string(let value)):
            if let result = UUID(uuidString: value) {
                return result
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }

    @inlinable func decode() throws -> Date {
        switch self {
        case .logical(.date(let days)):
            return Date(timeIntervalSince1970: Double(days * 86400))
        case .logical(.timeMillis(let milliseconds)):
            return Date(timeIntervalSince1970: Double(milliseconds) / 1000.0)
        case .logical(.timeMicros(let microseconds)):
            return Date(timeIntervalSince1970: Double(microseconds) / 1000000.0)
        case .logical(.timestampMillis(let milliseconds)):
            return Date(timeIntervalSince1970: Double(milliseconds) / 1000.0)
        case .logical(.timestampMicros(let microseconds)):
            return Date(timeIntervalSince1970: Double(microseconds) / 1000000.0)
        case .logical(.localTimestampMillis(let milliseconds)):
            let timeZoneSeconds = Double(TimeZone.current.secondsFromGMT())
            return Date(timeIntervalSince1970: Double(milliseconds) / 1000.0 + timeZoneSeconds)
        case .logical(.localTimestampMicros(let microseconds)):
            let timeZoneSeconds = Double(TimeZone.current.secondsFromGMT())
            return Date(timeIntervalSince1970: Double(microseconds) / 1000000.0 + timeZoneSeconds)
        case .primitive(.int(let seconds)):
            return Date(timeIntervalSince1970: Double(seconds))
        case .primitive(.long(let seconds)):
            return Date(timeIntervalSince1970: Double(seconds))
        case .primitive(.float(let seconds)):
            return Date(timeIntervalSince1970: Double(seconds))
        case .primitive(.double(let seconds)):
            return Date(timeIntervalSince1970: seconds)
        case .primitive(.string(let dateString)):
            if let date = ISO8601DateFormatter().date(from: dateString) {
                return date
            } else {
                throw BinaryDecodingError.typeMismatchWithSchema
            }
        default:
            throw BinaryDecodingError.typeMismatchWithSchema
        }
    }
}
