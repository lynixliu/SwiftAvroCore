import XCTest

#if !os(macOS)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(swift_avro_coreTests.allTests),
        testCase(AvroSchemaCodingTest.allTests),
        testCase(AvroSchemaEquatableTest.allTests),
        testCase(AvroDecodableTest.allTests),
        testCase(AvroEncodableTest.allTests),
    ]
}
#endif
