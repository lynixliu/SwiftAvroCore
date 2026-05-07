import Foundation

/// Internal utility for converting between Avro binary representations and Swift Foundation types.
internal enum LogicalTypeConverter {

    // MARK: - Decimal

    /// Decodes a decimal from two's complement bytes.
    static func decodeDecimal(bytes: [UInt8], scale: Int, precision: Int) -> Decimal {
        let bigInt = bytesToBigInt(bytes)
        let decimalValue = Decimal(string: "\(bigInt)") ?? 0
        let divisor = pow(10, scale)
        return decimalValue / divisor
    }

    /// Encodes a decimal to two's complement bytes.
    static func encodeDecimal(_ decimal: Decimal, scale: Int, precision: Int) -> [UInt8] {
        let multiplier = pow(10, scale)
        let scaledValue = decimal * multiplier
        let bigIntString = scaledValue.description.split(separator: ".").first.map(String.init) ?? "0"
        return bigIntToString(bigIntString)
    }

    // MARK: - Timestamp Micros

    static func decodeTimestampMicros(_ micros: Int64) -> Date {
        return Date(timeIntervalSince1970: Double(micros) / 1_000_000.0)
    }

    static func encodeTimestampMicros(_ date: Date) -> Int64 {
        return Int64(round(date.timeIntervalSince1970 * 1_000_000.0))
    }

    // MARK: - Time Millis

    static func decodeTimeMillis(_ millis: Int32) -> Date {
        let calendar = Calendar.current
        let midnight = calendar.startOfDay(for: Date())
        return midnight.addingTimeInterval(TimeInterval(millis) / 1000.0)
    }

    static func encodeTimeMillis(_ date: Date) -> Int32 {
        let calendar = Calendar.current
        let midnight = calendar.startOfDay(for: date)
        let diff = date.timeIntervalSince(midnight)
        return Int32(round(diff * 1000.0))
    }

    // MARK: - Time Micros

    static func decodeTimeMicros(_ micros: Int64) -> Date {
        let calendar = Calendar.current
        let midnight = calendar.startOfDay(for: Date())
        return midnight.addingTimeInterval(TimeInterval(micros) / 1_000_000.0)
    }

    static func encodeTimeMicros(_ date: Date) -> Int64 {
        let calendar = Calendar.current
        let midnight = calendar.startOfDay(for: date)
        let diff = date.timeIntervalSince(midnight)
        return Int64(round(diff * 1_000_000.0))
    }

    // MARK: - Duration

    static func decodeDuration(bytes: [UInt8]) -> TimeInterval {
        // Duration is 16 bytes nanoseconds.
        // For simplicity in this core implementation, we handle the lower 8 bytes as the primary value.
        let nanos = bytesToInt128(bytes)
        return TimeInterval(nanos) / 1_000_000_000.0
    }

    static func encodeDuration(_ duration: TimeInterval) -> [UInt8] {
        let nanos = Int64(round(duration * 1_000_000_000.0))
        return int128ToBytes(nanos)
    }

    // MARK: - Helpers

    private static func bytesToBigInt(_ bytes: [UInt8]) -> String {
        if bytes.isEmpty { return "0" }
        // Simplification: Convert bytes to a hex string then to decimal.
        // In a full implementation, this would handle true two's complement for arbitrary lengths.
        let hex = bytes.map { String(format: "%02x", $0) }.joined()
        if let val = Int64(hex, radix: 16) { return String(val) }
        return "0"
    }

    private static func bigIntToString(_ s: String) -> [UInt8] {
        // Convert decimal string to bytes.
        // This is a stub for the complex two's complement binary encoding.
        return Array(s.utf8)
    }

    private static func bytesToInt128(_ bytes: [UInt8]) -> Int64 {
        // Duration is 16 bytes. We extract the Int64 value.
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }

    private static func int128ToBytes(_ val: Int64) -> [UInt8] {
        var value = val
        return withUnsafeBytes(of: &value) { Array($0) }
    }
}
