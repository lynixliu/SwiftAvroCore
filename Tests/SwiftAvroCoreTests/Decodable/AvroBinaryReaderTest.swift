//
// Created by Kacper Kawecki on 24/03/2021.
//

import XCTest
@testable import SwiftAvroCore

internal class AvroBinaryReaderTest: XCTestCase {
    func testReadBoolean() throws {
        let data = Data([0x0, 0x1])
        let reader = AvroBinaryReader(data: data)
        XCTAssertFalse(try reader.readBoolean(), "Failed parsing false")
        XCTAssertTrue(try reader.readBoolean(), "Failed parsing true")
        XCTAssertThrowsError(try reader.readBoolean())
    }

    func testReadNull() {
        let data = Data([])
        let reader = AvroBinaryReader(data: data)
        XCTAssertNoThrow(reader.readNull())
        XCTAssertNoThrow(reader.readNull())
    }

    func testReadLong() throws {
        let data = Data([0x06, 0x80, 0x01, 0x7f])
        let reader = AvroBinaryReader(data: data)
        XCTAssertEqual(try reader.readLong(), 3)
        XCTAssertEqual(try reader.readLong(), 64)
        XCTAssertEqual(try reader.readLong(), -64)
        XCTAssertThrowsError(try reader.readLong())
    }

    func testReadInt() throws {
        let data = Data([0x06, 0x80, 0x01, 0x7f])
        let reader = AvroBinaryReader(data: data)
        XCTAssertEqual(try reader.readLong(), 3)
        XCTAssertEqual(try reader.readLong(), 64)
        XCTAssertEqual(try reader.readLong(), -64)
        XCTAssertThrowsError(try reader.readLong())
    }

    func testReadBytes() throws {
        let data = Data([
            0x06, // length 3
            0x80, 0x01, 0x7f, // 3 bytes
            0x0, // length 0
            0x08, // length 4
            0x11, 0x0, 0xff, 0x32,
            0x07 // length -4
        ])
        let reader = AvroBinaryReader(data: data)
        XCTAssertEqual(try reader.readBytes(), [0x80, 0x01, 0x7f], "Bytes don't match")
        XCTAssertEqual(try reader.readBytes(), [], "Bytes don't match")
        XCTAssertEqual(try reader.readBytes(), [0x11, 0x0, 0xff, 0x32], "Bytes don't match")
        XCTAssertThrowsError(try reader.readBytes()) { (error) in
            XCTAssertNotNil(error as? BinaryDecodingError, "\(error) is not BinaryDecodingError")
            XCTAssertEqual(error as? BinaryDecodingError, BinaryDecodingError.malformedAvro)
        }
        XCTAssertThrowsError(try reader.readBytes()) { (error) in
            XCTAssertNotNil(error as? BinaryDecodingError, "\(error) is not BinaryDecodingError")
            XCTAssertEqual(error as? BinaryDecodingError, BinaryDecodingError.outOfBufferBoundary)
        }
    }

    func testReadString() throws {
        let data = Data([
            0x06, // length 3
            0x66, 0x6f, 0x6f, // foo
            0x0, // length 0
            0x0a, // length 5
            0x68, 0x65, 0x6c, 0x6c, 0x6f, //"hello"
            0x07 // length -4
        ])
        let reader = AvroBinaryReader(data: data)
        XCTAssertEqual(try reader.readString(), "foo", "String don't match")
        XCTAssertEqual(try reader.readString(), "", "String don't match")
        XCTAssertEqual(try reader.readString(), "hello", "String don't match")
        XCTAssertThrowsError(try reader.readBytes()) { (error) in
            XCTAssertNotNil(error as? BinaryDecodingError, "\(error) is not BinaryDecodingError")
            XCTAssertEqual(error as? BinaryDecodingError, BinaryDecodingError.malformedAvro)
        }
        XCTAssertThrowsError(try reader.readBytes()) { (error) in
            XCTAssertNotNil(error as? BinaryDecodingError, "\(error) is not BinaryDecodingError")
            XCTAssertEqual(error as? BinaryDecodingError, BinaryDecodingError.outOfBufferBoundary)
        }
    }

    func testReadFloat() throws {
        let data = Data([0xc3, 0xf5, 0x48, 0x40, 0x0])
        let reader = AvroBinaryReader(data: data)
        XCTAssertEqual(try reader.readFloat(), 3.14)
        XCTAssertThrowsError(try reader.readFloat()) { (error) in
            XCTAssertNotNil(error as? BinaryDecodingError, "\(error) is not BinaryDecodingError")
            XCTAssertEqual(error as? BinaryDecodingError, BinaryDecodingError.outOfBufferBoundary)
        }
    }

    func testReadDouble() throws {
        let data = Data([0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40])
        let reader = AvroBinaryReader(data: data)
        XCTAssertEqual(try reader.readDouble(), 3.14)
    }
}
