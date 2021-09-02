import XCTest
@testable import SwiftAvroCore

final class SwiftAvroCoreTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(SwiftAvroCore().text, "Hello, World!")
    }
    func testEndToEnd() {
        // The JSON schema
        let jsonSchema = """
        {"type":"record",
        "fields":[
        {"name": "requestId", "type": "int"},
        {"name": "requestName", "type": "string"},
        {"name": "parameter", "type": {"type":"array", "items": "int"}}
        ]}
        """
        struct Model: Codable {
            var requestId: Int32 = 1
            var requestName: String = ""
            var parameter: [Int32] = []
        }
        // Make an Avro instance
        let avro = Avro()
        let myModel = Model(requestId: 42, requestName: "hello", parameter: [1,2])
        // Decode schema from json
        _ = avro.decodeSchema(schema: jsonSchema)!
        // encode to avro binray
        let binaryValue = try!avro.encode(myModel)
        // decode from avro binary
        let decodedValue: Model = try! avro.decode(from: binaryValue)
        XCTAssertEqual(decodedValue.requestId, myModel.requestId, "int32 don't match.")
        XCTAssertEqual(decodedValue.requestName, myModel.requestName, "string don't match.")
        XCTAssertEqual(decodedValue.parameter, myModel.parameter, "int32 arrays don't match.")
    }
    static var allTests = [
        ("testExample", testExample),
        ("testEndToEnd", testEndToEnd),
    ]
}
