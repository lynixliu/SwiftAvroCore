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
}