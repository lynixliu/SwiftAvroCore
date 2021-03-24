//
// Created by Kacper Kawecki on 24/03/2021.
//

import Foundation

/// Return a 32-bit ZigZag-decoded value
internal extension UInt32 {
    func decodeZigZag() -> Int32 {
        return Int32(self >> 1) ^ -Int32(self & 1)
    }
}

/// Return a 64-bit ZigZag-decoded value
internal extension UInt64 {
     func decodeZigZag() -> Int64 {
        return Int64(self >> 1) ^ -Int64(self & 1)
    }
}