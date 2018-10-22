import XCTest
@testable import SwiftAvroCore

final class SwiftAvroCoreTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(SwiftAvroCore().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
