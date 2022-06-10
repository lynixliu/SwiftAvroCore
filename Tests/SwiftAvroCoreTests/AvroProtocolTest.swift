//
//  AvroProtocolTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 1/05/22.
//

import XCTest
@testable import SwiftAvroCore

class AvroProtocolTest: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        // Any test you write for XCTest can be annotated as throws and async.
        // Mark your test throws to produce an unexpected failure when your test encounters an uncaught error.
        // Mark your test async to allow awaiting for asynchronous code to complete. Check the results with assertions afterwards.
    }
    
    func testProtocol() {
        let protocolJson = """
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "doc": "Protocol Greetings",
  "types": [
     {"name": "Greeting", "type": "record", "fields": [{"name": "message", "type": "string"}]},
     {"name": "Curse", "type": "error", "fields": [{"name": "message", "type": "string"}]}],
  "messages": {
    "hello": {
       "doc": "Say hello.",
       "request": [{"name": "greeting", "type": "Greeting"}],
       "response": "Greeting",
       "errors": ["Curse"]
    }
  }
}
"""
        let decoder = JSONDecoder()
        let protoclData = protocolJson.data(using: .utf8)!
        let testProtocol = try? decoder.decode(AvroProtocol.self, from: protoclData)
        XCTAssertNotEqual(testProtocol, nil)
        XCTAssertEqual(testProtocol!.type, "protocol")
        XCTAssertEqual(testProtocol!.name, "HelloWorld")
        XCTAssertEqual(testProtocol?.namespace, "com.acme")
        XCTAssertEqual(testProtocol?.types?.count, 2)
        XCTAssertEqual(testProtocol?.types?[0].getName(), "Greeting")
        XCTAssertEqual(testProtocol?.types?[0].getTypeName(), "record")
        XCTAssertEqual(testProtocol?.types?[0].getRecord()?.fields.count, 1)
        XCTAssertEqual(testProtocol?.types?[0].getRecord()?.fields[0].name, "message")
        XCTAssertEqual(testProtocol?.types?[0].getRecord()?.fields[0].type, AvroSchema.init(type: "string"))
        XCTAssertEqual(testProtocol?.types?[1].getName(), "Curse")
        XCTAssertEqual(testProtocol?.types?[1].getTypeName(), "error")
        XCTAssertEqual(testProtocol?.types?[1].getError()?.fields.count, 1)
        XCTAssertEqual(testProtocol?.types?[1].getError()?.fields[0].name, "message")
        XCTAssertEqual(testProtocol?.types?[1].getError()?.fields[0].type, AvroSchema.init(type: "string"))
        XCTAssertEqual(testProtocol?.messages?.count,1)
        XCTAssertEqual(testProtocol?.messages?["hello"]?.doc, "Say hello.")
        XCTAssertEqual(testProtocol?.messages?["hello"]?.request?.count,1)
        XCTAssertEqual(testProtocol?.messages?["hello"]?.request?[0].type,"Greeting")
        XCTAssertEqual(testProtocol?.messages?["hello"]?.request?[0].name,"greeting")
        XCTAssertEqual(testProtocol?.messages?["hello"]?.response!, "Greeting")
        XCTAssertEqual(testProtocol?.messages?["hello"]?.errors!, ["Curse"])
    }
    
    func testRequestDecode() {
        struct Request:Codable {
            let clientHash: [UInt8]
            let clientProtocol: String?
            let serverHash: [UInt8]
            var meta: [String: [UInt8]]?
        }

        struct arg {
            let data: Data
            let expected: Request
        }
        let testSchema = Avro().newSchema(schema: MessageConstant.requestSchema)!
        let decoder = AvroDecoder(schema: testSchema)
        for t in [arg(data: Data([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                  0,
                                  0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                  0]),
                      expected: Request(clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                        clientProtocol: nil,
                                        serverHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                        meta: nil)),
                  arg(data: Data([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                   0x02, 0x06, 0x66, 0x6f, 0x6f,
                                   0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,0]),
                      expected: Request(clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                                   clientProtocol: "foo",
                                                   serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                                   meta: nil)),
                  arg(data: Data([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                   0x02, 0x06, 0x66, 0x6f, 0x6f,
                                   0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                   0x02, 0x02, 0x04, 0x66, 0x6f, 0x06, 0x1, 0x2, 0x3, 0,0]),
                      expected: Request(clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                        clientProtocol: "foo",
                                        serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                        meta: ["fo":[1,2,3]]))]{
            let model = try! decoder.decode(Request.self, from: t.data)
            XCTAssertEqual(t.expected.clientHash, model.clientHash)
            XCTAssertEqual(t.expected.clientProtocol, model.clientProtocol)
            XCTAssertEqual(t.expected.serverHash, model.serverHash)
            XCTAssertEqual(t.expected.meta, model.meta)
        }
    }
    
    func testResponseDecode() {
        struct arg {
            let model: HandshakeResponse
            let expected: Data
        }
        let encoder = AvroEncoder()
        let testSchema = Avro().newSchema(schema: MessageConstant.responseSchema)
        for t in [arg(model: HandshakeResponse(match: HandshakeMatch.BOTH, serverProtocol: nil, serverHash: nil),expected: Data([0,0,0,0])),
                  arg(model: HandshakeResponse(match: HandshakeMatch.CLIENT, serverProtocol: "foo", serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf]),
                      expected: Data([2,
                                      0x02,0x06,0x66,0x6f,0x6f,
                                      0x02,0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                                      0])),
                  arg(model: HandshakeResponse(match: HandshakeMatch.CLIENT, serverProtocol: "foo", serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                                      meta: ["fo":[1,2,3]]),
                      expected: Data([2,
                                      0x02,0x06,0x66,0x6f,0x6f,
                                      0x02,0x01,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
                                      0x02,0x02,0x04,0x66,0x6f,0x06,0x01,0x02,0x03,0x00]))
        ]{
           let data = try! encoder.encode(t.model, schema: testSchema!)
           XCTAssertTrue(data == t.expected)
        }
    }

    func testPerformanceExample() throws {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }

}
