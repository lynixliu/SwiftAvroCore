import Foundation
import Testing
import SwiftAvroCore

@Suite("Avro Fingerprint")
struct AvroFingerPrintTests {

    @Test("convenience init creates instance with default size")
    func initConvenience() {
        let fp = AvroFingerPrint()
        #expect(fp != nil)  // AvroFingerPrint is a class type
    }

    @Test("init with custom size creates table")
    func initCustomSize() {
        let fp = AvroFingerPrint(size: 128)
        #expect(fp != nil)
    }

    @Test("fingerPrint64 returns consistent hash for same input")
    func fingerPrint64Consistency() {
        let fp = AvroFingerPrint()
        let data: [UInt8] = [104, 101, 108, 108, 111]  // "hello"
        let hash1 = fp.fingerPrint64(data)
        let hash2 = fp.fingerPrint64(data)
        #expect(hash1 == hash2)
    }

    @Test("fingerPrint64 returns different hash for different input")
    func fingerPrint64Uniqueness() {
        let fp = AvroFingerPrint()
        let data1: [UInt8] = [104, 101, 108, 108, 111]  // "hello"
        let data2: [UInt8] = [119, 111, 114, 108, 100]  // "world"
        let hash1 = fp.fingerPrint64(data1)
        let hash2 = fp.fingerPrint64(data2)
        #expect(hash1 != hash2)
    }

    @Test("fingerPrint64 handles empty input")
    func fingerPrint64Empty() {
        let fp = AvroFingerPrint()
        let empty: [UInt8] = []
        let hash = fp.fingerPrint64(empty)
        #expect(hash == -4513414715797952619)  // The empty constant
    }

    @Test("fingerPrint64 handles single byte")
    func fingerPrint64SingleByte() {
        let fp = AvroFingerPrint()
        let data: [UInt8] = [0]
        let hash = fp.fingerPrint64(data)
        #expect(hash != -4513414715797952619)
    }

    @Test("fingerPrint64 handles all single byte values 0-255")
    func fingerPrint64AllBytes() {
        let fp = AvroFingerPrint()
        for i in 0..<256 {
            let data: [UInt8] = [UInt8(i)]
            let hash = fp.fingerPrint64(data)
            #expect(hash != -4513414715797952619)
        }
    }

    @Test("fingerPrint64 handles larger data")
    func fingerPrint64Larger() {
        let fp = AvroFingerPrint()
        let data: [UInt8] = Array((0..<1000).map { UInt8($0 % 256) })
        let hash = fp.fingerPrint64(data)
        #expect(hash != -4513414715797952619)
    }
}