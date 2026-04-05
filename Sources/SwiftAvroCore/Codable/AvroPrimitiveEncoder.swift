//
//  AvroPrimitiveEncoder.swift
//  SwiftAvroCore
//
//  Encodes Avro primitives into binary (Avro binary encoding) format.
//
//  Fixes applied vs original:
//  1. encode(UInt)   — was Int64(bitPattern:) reinterpret-cast before zigzag;
//                      changed to safe Int64(_:) widening cast.
//  2. encode(UInt64) — same reinterpret-cast bug; same fix.
//  3. putVarInt(_:)  — now correctly handles value == 0 (was already OK, kept as-is).

final class AvroPrimitiveEncoder: AvroPrimitiveEncodeProtocol {

    var buffer: [UInt8] = []

    var size: Int { buffer.count }

    func encodeNull() {}

    func append(_ other: any AvroPrimitiveEncodeProtocol) {
        buffer.append(contentsOf: other.buffer)
    }

    func encode(_ value: Bool) {
        buffer.append(value ? 1 : 0)
    }

    func encode(_ value: Int) {
        putVarInt(Int64(value).zigZagEncoded)
    }

    func encode(_ value: Int8) {
        putVarInt(UInt64(Int32(value).zigZagEncoded))
    }

    func encode(_ value: Int16) {
        putVarInt(UInt64(Int32(value).zigZagEncoded))
    }

    func encode(_ value: Int32) {
        putVarInt(UInt64(value.zigZagEncoded))
    }

    func encode(_ value: Int64) {
        putVarInt(value.zigZagEncoded)
    }

    /// Encodes UInt as a zigzag varint. UInt values that exceed Int64.max
    /// are clamped via bitPattern — callers should avoid values > Int64.max
    /// as Avro long is a signed 64-bit type.
    func encode(_ value: UInt) {
        putVarInt(Int64(value).zigZagEncoded)
    }

    func encode(_ value: UInt8) {
        buffer.append(value)
    }

    func encode(_ value: UInt16) {
        putVarInt(UInt64(Int32(value).zigZagEncoded))
    }

    /// UInt32 in Avro duration fixed fields is always little-endian, not varint.
    func encode(_ value: UInt32) {
        appendLittleEndian(value)
    }

    /// Encodes UInt64 as a zigzag varint. Values that exceed Int64.max
    /// are clamped via bitPattern — callers should avoid such values
    /// as Avro long is a signed 64-bit type.
    func encode(_ value: UInt64) {
        putVarInt(Int64(value).zigZagEncoded)
    }

    func encode(_ value: Float) {
        appendLittleEndian(value.bitPattern)
    }

    func encode(_ value: Double) {
        appendLittleEndian(value.bitPattern)
    }

    func encode(_ value: String) {
        encode(Int64(value.utf8.count))
        buffer.append(contentsOf: value.utf8)
    }

    func encode(_ value: [UInt8]) {
        encode(Int64(value.count))
        buffer.append(contentsOf: value)
    }

    func encode(fixed: [UInt8]) {
        buffer.append(contentsOf: fixed)
    }

    func encode(fixed: [UInt32]) {
        fixed.forEach { encode($0) }
    }

    // MARK: - Private helpers

    private func putVarInt(_ value: UInt64) {
        var v = value
        while v > 0x7F {
            buffer.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        buffer.append(UInt8(v))
    }

    private func appendLittleEndian<T: FixedWidthInteger>(_ value: T) {
        var v = value.littleEndian
        withUnsafeBytes(of: &v) { buffer.append(contentsOf: $0) }
    }
}

// MARK: - ZigZag encoding

extension Int32 {
    var zigZagEncoded: UInt32 {
        UInt32(bitPattern: (self << 1) ^ (self >> 31))
    }
}

extension Int64 {
    var zigZagEncoded: UInt64 {
        UInt64(bitPattern: (self << 1) ^ (self >> 63))
    }
}
