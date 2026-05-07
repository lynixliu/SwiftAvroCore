import Foundation

/// Internal utility for converting between Avro binary representations and Swift Foundation types.
internal enum LogicalTypeConverter {

    // MARK: - Date

    static func decodeDate(_ days: Int) -> Date {
        return Date(timeIntervalSince1970: Double(days) * 86400.0)
    }

    static func encodeDate(_ date: Date) -> Int {
        return Int(floor(date.timeIntervalSince1970 / 86400.0))
    }

    static func decodeTimestampMillis(_ millis: Int64) -> Date {
        return Date(timeIntervalSince1970: Double(millis) / 1000.0)
    }

    static func encodeTimestampMillis(_ date: Date) -> Int64 {
        return Int64(round(date.timeIntervalSince1970 * 1000.0))
    }

    // MARK: - Decimal

    /// Decodes a decimal from two's complement bytes.
    static func decodeDecimal(bytes: [UInt8], scale: Int, precision: Int) -> Decimal {
        guard !bytes.isEmpty else { return 0 }
        let negative = bytes[0] & 0x80 != 0
        let magnitude = negative ? twosComplementMagnitude(bytes) : bytes
        let digits = decimalDigits(fromMagnitudeBytes: magnitude)
        let decimalString = scaledDecimalString(digits: digits, scale: scale, negative: negative)
        return Decimal(string: decimalString) ?? 0
    }

    /// Encodes a decimal to two's complement bytes.
    static func encodeDecimal(_ decimal: Decimal, scale: Int, precision: Int) throws -> [UInt8] {
        let (negative, digits) = try unscaledDecimalDigits(decimal, scale: scale, precision: precision)
        let magnitude = magnitudeBytes(fromDecimalDigits: digits)
        return negative ? negativeTwosComplementBytes(fromMagnitude: magnitude) : positiveTwosComplementBytes(fromMagnitude: magnitude)
    }

    static func encodeDecimal(_ decimal: Decimal, scale: Int, precision: Int, fixedSize: Int) throws -> [UInt8] {
        var bytes = try encodeDecimal(decimal, scale: scale, precision: precision)
        guard fixedSize >= bytes.count else { throw BinaryEncodingError.invalidDecimal }
        let pad = bytes.first.map { $0 & 0x80 == 0 ? UInt8(0x00) : UInt8(0xFF) } ?? 0x00
        if fixedSize > bytes.count {
            bytes.insert(contentsOf: repeatElement(pad, count: fixedSize - bytes.count), at: 0)
        }
        return bytes
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
        return Date(timeIntervalSince1970: TimeInterval(millis) / 1000.0)
    }

    static func encodeTimeMillis(_ date: Date) -> Int32 {
        let secondsInDay: TimeInterval = 86400
        let timeOfDay = date.timeIntervalSince1970.truncatingRemainder(dividingBy: secondsInDay)
        let adjusted = timeOfDay < 0 ? timeOfDay + secondsInDay : timeOfDay
        return Int32(round(adjusted * 1000.0))
    }

    // MARK: - Time Micros

    static func decodeTimeMicros(_ micros: Int64) -> Date {
        return Date(timeIntervalSince1970: TimeInterval(micros) / 1_000_000.0)
    }

    static func encodeTimeMicros(_ date: Date) -> Int64 {
        let secondsInDay: TimeInterval = 86400
        let timeOfDay = date.timeIntervalSince1970.truncatingRemainder(dividingBy: secondsInDay)
        let adjusted = timeOfDay < 0 ? timeOfDay + secondsInDay : timeOfDay
        return Int64(round(adjusted * 1_000_000.0))
    }

    // MARK: - Duration

    static func decodeDuration(bytes: [UInt8]) -> [UInt32] {
        guard bytes.count >= 12 else { return [0, 0, 0] }
        let months = UInt32(bytes[0]) | UInt32(bytes[1]) << 8 | UInt32(bytes[2]) << 16 | UInt32(bytes[3]) << 24
        let days   = UInt32(bytes[4]) | UInt32(bytes[5]) << 8 | UInt32(bytes[6]) << 16 | UInt32(bytes[7]) << 24
        let millis = UInt32(bytes[8]) | UInt32(bytes[9]) << 8 | UInt32(bytes[10]) << 16 | UInt32(bytes[11]) << 24
        return [months, days, millis]
    }

    private static func twosComplementMagnitude(_ bytes: [UInt8]) -> [UInt8] {
        var magnitude = bytes.map { ~$0 }
        addOne(to: &magnitude)
        return trimLeadingZeros(magnitude)
    }

    private static func positiveTwosComplementBytes(fromMagnitude magnitude: [UInt8]) -> [UInt8] {
        var bytes = trimLeadingZeros(magnitude)
        if bytes.isEmpty { return [0] }
        if bytes[0] & 0x80 != 0 { bytes.insert(0, at: 0) }
        return bytes
    }

    private static func negativeTwosComplementBytes(fromMagnitude magnitude: [UInt8]) -> [UInt8] {
        var paddedMagnitude = trimLeadingZeros(magnitude)
        if paddedMagnitude.isEmpty { return [0] }
        while true {
            var bytes = paddedMagnitude.map { ~$0 }
            addOne(to: &bytes)
            if bytes[0] & 0x80 != 0 {
                return trimLeadingSignBytes(bytes)
            }
            paddedMagnitude.insert(0, at: 0)
        }
    }

    private static func addOne(to bytes: inout [UInt8]) {
        var carry = 1
        for i in stride(from: bytes.count - 1, through: 0, by: -1) {
            let sum = Int(bytes[i]) + carry
            bytes[i] = UInt8(sum & 0xFF)
            carry = sum >> 8
            if carry == 0 { break }
        }
        if carry > 0 { bytes.insert(UInt8(carry), at: 0) }
    }

    private static func decimalDigits(fromMagnitudeBytes bytes: [UInt8]) -> String {
        var digits = [0]
        for byte in trimLeadingZeros(bytes) {
            var carry = Int(byte)
            for i in stride(from: digits.count - 1, through: 0, by: -1) {
                let value = digits[i] * 256 + carry
                digits[i] = value % 10
                carry = value / 10
            }
            while carry > 0 {
                digits.insert(carry % 10, at: 0)
                carry /= 10
            }
        }
        return digits.map(String.init).joined()
    }

    private static func magnitudeBytes(fromDecimalDigits digits: String) -> [UInt8] {
        var bytes: [UInt8] = [0]
        for character in digits {
            guard let digit = character.wholeNumberValue else { continue }
            var carry = digit
            for i in stride(from: bytes.count - 1, through: 0, by: -1) {
                let value = Int(bytes[i]) * 10 + carry
                bytes[i] = UInt8(value & 0xFF)
                carry = value >> 8
            }
            while carry > 0 {
                bytes.insert(UInt8(carry & 0xFF), at: 0)
                carry >>= 8
            }
        }
        return trimLeadingZeros(bytes)
    }

    private static func scaledDecimalString(digits: String, scale: Int, negative: Bool) -> String {
        let stripped = stripLeadingZeros(digits)
        guard stripped != "0" else { return "0" }
        let unsigned: String
        if scale == 0 {
            unsigned = stripped
        } else if scale >= stripped.count {
            unsigned = "0." + String(repeating: "0", count: scale - stripped.count) + stripped
        } else {
            let point = stripped.index(stripped.endIndex, offsetBy: -scale)
            unsigned = stripped[..<point] + "." + stripped[point...]
        }
        return negative ? "-" + unsigned : unsigned
    }

    private static func unscaledDecimalDigits(_ decimal: Decimal, scale: Int, precision: Int) throws -> (Bool, String) {
        var value = decimal
        var scaled = Decimal()
        NSDecimalMultiplyByPowerOf10(&scaled, &value, Int16(scale), .plain)
        var rounded = Decimal()
        NSDecimalRound(&rounded, &scaled, 0, .plain)
        var string = NSDecimalNumber(decimal: rounded).stringValue
        guard string.lowercased() != "nan" else { throw BinaryEncodingError.invalidDecimal }
        let negative = string.first == "-"
        if negative { string.removeFirst() }
        guard string.allSatisfy(\.isNumber) else { throw BinaryEncodingError.invalidDecimal }
        let digits = stripLeadingZeros(string)
        if precision > 0, digits.count > precision { throw BinaryEncodingError.invalidDecimal }
        return (negative && digits != "0", digits)
    }

    private static func trimLeadingZeros(_ bytes: [UInt8]) -> [UInt8] {
        let trimmed = bytes.drop { $0 == 0 }
        return trimmed.isEmpty ? [0] : Array(trimmed)
    }

    private static func trimLeadingSignBytes(_ bytes: [UInt8]) -> [UInt8] {
        var bytes = bytes
        while bytes.count > 1 {
            let first = bytes[0]
            let second = bytes[1]
            if (first == 0x00 && second & 0x80 == 0) || (first == 0xFF && second & 0x80 != 0) {
                bytes.removeFirst()
            } else {
                break
            }
        }
        return bytes
    }

    private static func stripLeadingZeros(_ string: String) -> String {
        let stripped = string.drop { $0 == "0" }
        return stripped.isEmpty ? "0" : String(stripped)
    }
}
