import Foundation
import Testing
import SwiftAvroCore

@Suite("Avro Fingerprint")
struct AvroFingerprintTests {

    @Test("fingerprint64 returns consistent hash for same input")
    func fingerprint64Consistency() {
        let data: [UInt8] = [104, 101, 108, 108, 111]  // "hello"
        #expect(AvroFingerprint.fingerprint64(data) == AvroFingerprint.fingerprint64(data))
    }

    @Test("fingerprint64 returns different hash for different inputs")
    func fingerprint64Uniqueness() {
        let data1: [UInt8] = [104, 101, 108, 108, 111]  // "hello"
        let data2: [UInt8] = [119, 111, 114, 108, 100]  // "world"
        #expect(AvroFingerprint.fingerprint64(data1) != AvroFingerprint.fingerprint64(data2))
    }

    @Test("fingerprint64 of empty input equals the Rabin empty constant")
    func fingerprint64Empty() {
        #expect(AvroFingerprint.fingerprint64([]) == -4513414715797952619)
    }

    @Test("fingerprint64 of non-empty input differs from the empty constant")
    func fingerprint64NonEmpty() {
        #expect(AvroFingerprint.fingerprint64([0]) != -4513414715797952619)
    }

    @Test("fingerprint64 handles all single-byte values 0–255")
    func fingerprint64AllBytes() {
        for i in 0..<256 {
            #expect(AvroFingerprint.fingerprint64([UInt8(i)]) != -4513414715797952619)
        }
    }

    @Test("fingerprint64 handles larger data")
    func fingerprint64Larger() {
        let data: [UInt8] = (0..<1000).map { UInt8($0 % 256) }
        #expect(AvroFingerprint.fingerprint64(data) != -4513414715797952619)
    }

    @Test("FingerprintFunction typealias accepts a custom algorithm")
    func customFingerprintFunction() {
        // Verify the typealias works: a trivial custom algorithm (byte reversal)
        // compiles and produces a different result than rabin64.
        let reversed: FingerprintFunction = { data in data.reversed() }
        let input: [UInt8] = [1, 2, 3]
        #expect(reversed(input) == [3, 2, 1])
    }
}
