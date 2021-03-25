//
// Created by Kacper Kawecki on 24/03/2021.
//

import Foundation

internal enum AvroPrimitiveValue {
    case null
    case boolean(Bool)
    case int(Int64)
    case long(Int64)
    case float(Float)
    case double(Double)
    case byte(UInt8)
    case bytes([UInt8])
    case string(String)
    case durationElement(UInt32)
}

internal enum AvroLogicalValue {
    case decimal([UInt8], precision: Int?, scale: Int?)
    case uuid(String)
    case date(Int64)
    case timeMillis(Int64)
    case timeMicros(Int64)
    case timestampMillis(Int64)
    case timestampMicros(Int64)
    case localTimestampMillis(Int64)
    case localTimestampMicros(Int64)
    case duration([UInt8])
}