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
    let serverHash = [uint8]([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10])
    let clientHash = [uint8]([0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10])
    do {
        let server = try MessageResponse(serverHash: serverHash, serverProtocol: supportProtocol)
        let client = try MessageRequest(clientHash: clientHash, clientProtocol: supportProtocol)
//        let requestData = try client.initHandshakeRequest()
 //       XCTAssertEqual(Data([UInt8]([1,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
  //                                   1,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16])), requestData, "initial request data mismatch")
        let requestData = Data([UInt8]([1,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                        1,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16]))
        let resposeClient = try server.resolveHandshakeRequest(requestData: requestData)
        resposeClient.forEach { UInt8 in
            print(UInt8,terminator: ",")
        }
        XCTAssertEqual(Data([UInt8]([4,194,7,123,10,32,32,34,110,97,109,101,115,112,97,99,101,34,58,32,34,99,111,109,46,97,99,109,101,34,44,10,32,32,34,112,114,111,116,111,99,111,108,34,58,32,34,72,101,108,108,111,87,111,114,108,100,34,44,10,32,32,34,100,111,99,34,58,32,34,80,114,111,116,111,99,111,108,32,71,114,101,101,116,105,110,103,115,34,44,10,32,32,34,116,121,112,101,115,34,58,32,91,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,114,101,99,111,114,100,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,44,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,67,117,114,115,101,34,44,32,34,116,121,112,101,34,58,32,34,101,114,114,111,114,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,93,44,10,32,32,34,109,101,115,115,97,103,101,115,34,58,32,123,10,32,32,32,32,34,104,101,108,108,111,34,58,32,123,10,32,32,32,32,32,32,32,34,100,111,99,34,58,32,34,83,97,121,32,104,101,108,108,111,46,34,44,10,32,32,32,32,32,32,32,34,114,101,113,117,101,115,116,34,58,32,91,123,34,110,97,109,101,34,58,32,34,103,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,71,114,101,101,116,105,110,103,34,32,125,93,44,10,32,32,32,32,32,32,32,34,114,101,115,112,111,110,115,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,10,32,32,32,32,32,32,32,34,101,114,114,111,114,115,34,58,32,91,34,67,117,114,115,101,34,93,10,32,32,32,32,125,10,32,32,125,10,125,0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16])), resposeClient, "response CLIENT data mismatch")
        /*let (r, got) = try client.decodeResponse(responseData: resposeClient)
        XCTAssertEqual(r.match, HandshakeMatch.CLIENT, "response CLIENT mismatch")
        XCTAssertEqual(Data(), got, "response CLIENT mismatch")
        let requestClient = try client.resolveHandshakeResponse(response: r)
        XCTAssertEqual(Data(), requestClient, "request CLIENT data mismatch")*/
    } catch {
        XCTAssert(false, "handshake failed")
    }
}
    
}

