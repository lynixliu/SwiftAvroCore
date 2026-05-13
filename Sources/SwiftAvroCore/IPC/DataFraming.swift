//
//  IPC/DataFraming.swift
//  SwiftAvroCore
//

import Foundation

extension Data {

    /// Parses Avro IPC frames from raw data.
    ///
    /// Each frame is prefixed with a big-endian `UInt32` byte count.
    /// A zero-length prefix signals the end of the frame sequence.
    public func deFraming() -> [Data] {
        var frames: [Data] = []
        var offset = 0

        while offset + 4 <= count {
            let length: UInt32 = (UInt32(self[offset])     << 24) |
                                 (UInt32(self[offset + 1]) << 16) |
                                 (UInt32(self[offset + 2]) <<  8) |
                                  UInt32(self[offset + 3])
            offset += 4

            if length == 0 { break }

            let payloadEnd = offset + Int(length)
            guard payloadEnd <= count else { break }
            frames.append(self[offset..<payloadEnd])
            offset = payloadEnd
        }
        return frames
    }

    /// Deframes and concatenates all frame payloads into a single `Data`.
    ///
    /// Equivalent to `deFraming().reduce(Data(), +)` but avoids the intermediate
    /// array and per-step copies.
    public func deFramed() -> Data {
        var out = Data()
        var offset = 0
        while offset + 4 <= count {
            let length: UInt32 = (UInt32(self[offset])     << 24) |
                                 (UInt32(self[offset + 1]) << 16) |
                                 (UInt32(self[offset + 2]) <<  8) |
                                  UInt32(self[offset + 3])
            offset += 4
            if length == 0 { break }
            let payloadEnd = offset + Int(length)
            guard payloadEnd <= count else { break }
            out.append(contentsOf: self[offset..<payloadEnd])
            offset = payloadEnd
        }
        return out
    }

    /// Wraps the receiver in Avro IPC framing, splitting into chunks of at most
    /// `maxFrameLength` bytes each, and appending the mandatory zero-length terminator.
    public func framing(maxFrameLength: Int = 16 * 1024) -> Data {
        guard maxFrameLength > 0 else { return Data([0, 0, 0, 0]) }
        var result = Data()
        result.reserveCapacity(count + (count / maxFrameLength + 2) * 4)
        var offset = 0
        while offset < count {
            let chunkSize = Swift.min(count - offset, maxFrameLength)
            result.append(contentsOf: Int32(chunkSize).bigEndianBytes)
            let end = offset + chunkSize
            result.append(self[offset..<end])
            offset = end
        }
        result.append(contentsOf: Int32(0).bigEndianBytes)
        return result
    }
}

private extension FixedWidthInteger {
    var bigEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
}
