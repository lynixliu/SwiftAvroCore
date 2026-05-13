//
//  AvroIPCFramingTest.swift
//  SwiftAvroCoreTests
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro IPC Framing")
struct AvroIPCFramingTest {

    // MARK: - framing

    @Test func framingProducesLengthPrefixedChunks() {
        let payload = Data(repeating: 0xAB, count: 10)
        let framed = payload.framing(maxFrameLength: 4)

        // Expected: [0,0,0,4] + 4 bytes + [0,0,0,4] + 4 bytes + [0,0,0,2] + 2 bytes + [0,0,0,0]
        #expect(framed.count == 3 * 4 + 10 + 4)
        // Terminator is the last 4 bytes
        #expect(framed.suffix(4) == Data([0, 0, 0, 0]))
    }

    @Test func framingEmptyDataYieldsOnlyTerminator() {
        let framed = Data().framing()
        #expect(framed == Data([0, 0, 0, 0]))
    }

    @Test func framingZeroMaxFrameLengthYieldsOnlyTerminator() {
        let framed = Data(repeating: 0xFF, count: 8).framing(maxFrameLength: 0)
        #expect(framed == Data([0, 0, 0, 0]))
    }

    // MARK: - deFraming

    @Test func deFramingRoundTrip() {
        let original = Data([1, 2, 3, 4, 5, 6, 7, 8])
        let frames = original.framing(maxFrameLength: 4).deFraming()
        #expect(frames.count == 2)
        #expect(frames[0] == Data([1, 2, 3, 4]))
        #expect(frames[1] == Data([5, 6, 7, 8]))
    }

    @Test func deFramingEmptyInputYieldsNoFrames() {
        #expect(Data().deFraming().isEmpty)
    }

    @Test func deFramingStopsAtZeroLengthTerminator() {
        // Two frames followed by a terminator followed by extra bytes that must be ignored.
        var raw = Data()
        raw.append(contentsOf: [0, 0, 0, 2, 0xAA, 0xBB])  // frame 1
        raw.append(contentsOf: [0, 0, 0, 0])               // terminator
        raw.append(contentsOf: [0, 0, 0, 2, 0xCC, 0xDD])  // must not be read
        let frames = raw.deFraming()
        #expect(frames.count == 1)
        #expect(frames[0] == Data([0xAA, 0xBB]))
    }

    @Test func deFramingTruncatedFrameIsIgnored() {
        // Length prefix says 8 bytes but only 3 are present.
        let raw = Data([0, 0, 0, 8, 0x01, 0x02, 0x03])
        let frames = raw.deFraming()
        #expect(frames.isEmpty)
    }

    // MARK: - deFramed

    @Test func deFramedConcatenatesPayloads() {
        let original = Data([1, 2, 3, 4, 5, 6, 7, 8])
        let reassembled = original.framing(maxFrameLength: 4).deFramed()
        #expect(reassembled == original)
    }

    @Test func deFramedEmptyInputIsEmpty() {
        #expect(Data().deFramed() == Data())
    }
}
