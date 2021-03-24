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
    case bytes([UInt8])
    case string(String)
}

internal enum AvroDatumValue {
    case primitive(AvroPrimitiveValue)
    case array([AvroDatumValue])
    case keyed([String: AvroDatumValue])
}