//
//  AvroRequestResponseTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 15/03/22.
//

import XCTest
@testable import SwiftAvroCore
class AvroRequestResponseTest: XCTestCase {

func testHandshake() {
    let supportProtocol = """
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
       "request": [{"name": "greeting", "type": "Greeting" }],
       "response": "Greeting",
       "errors": ["Curse"]
    }
  }
}
"""
    let avro = Avro()
    let sss = """
{
"type": "record","name": "HandshakeRequest","fields": [{"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},{"name": "serverHash","type": "MD5"}]}
"""
    let scheme = try avro.decodeSchema(schema: sss) 
    print(scheme)
    let serverHash = [uint8]([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10])
    let clientHash = [uint8]([0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10])
    do {
        let server = try MessageResponse(avro:avro, serverHash: serverHash, serverProtocol: supportProtocol)
        let client = try MessageRequest(avro:avro, clientHash: clientHash, clientProtocol: MessageConstant.requestSchema)
        let requestData = try client.initHandshakeRequest()
        XCTAssertEqual(Data(), requestData, "initial request data mismatch")
        let resposeClient = try server.resolveHandshakeRequest(requestData: requestData)
        XCTAssertEqual(Data(), requestData, "response CLIENT data mismatch")
        let (r, got) = try client.decodeResponse(responseData: resposeClient)
        XCTAssertEqual(r.match, HandshakeMatch.CLIENT, "response CLIENT mismatch")
        XCTAssertEqual(Data(), got, "response CLIENT mismatch")
        let requestClient = try client.resolveHandshakeResponse(response: r)
        XCTAssertEqual(Data(), requestClient, "request CLIENT data mismatch")
    } catch {
        XCTAssert(false, "handshake failed")
    }
}
    
}

