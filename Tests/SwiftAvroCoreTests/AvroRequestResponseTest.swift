//
//  AvroRequestResponseTest.swift
//  SwiftAvroCoreTests
//
//  Created by Yang.Liu on 15/03/22.
//

import XCTest
@testable import SwiftAvroCore
class AvroRequestResponseTest: XCTestCase {
    struct testArg {
    let supportProtocol: String = """
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
    let clientHash: [UInt8] = [UInt8]([0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10])
    let serverHash: [UInt8] = [UInt8]([0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10])
        let context: Context = Context(handshakeRequestMeta: [String: [UInt8]](),
                                   handshakeResponeMeta:[String: [UInt8]](),
                                   requestMeta:[String: [UInt8]](),
                                   responseMeta:[String: [UInt8]]())
    }
func testHandshake() {
    let arg = testArg()
    do {
        let server = try MessageResponse(context: arg.context, serverHash: arg.serverHash, serverProtocol: arg.supportProtocol)
        let client = try MessageRequest(context: arg.context, clientHash: arg.clientHash, clientProtocol: arg.supportProtocol)
        let requestData = try client.initHandshakeRequest()
        XCTAssertEqual(Data([UInt8]([0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                     0,
                                     0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                    0])), requestData, "initial request data mismatch")
        let (_,resposeNone) = try server.resolveHandshakeRequest(requestData: requestData)
        XCTAssertEqual(Data([UInt8]([4,2, 194,7,123,10,32,32,34,110,97,109,101,115,112,97,99,101,34,58,32,34,99,111,109,46,97,99,109,101,34,44,10,32,32,34,112,114,111,116,111,99,111,108,34,58,32,34,72,101,108,108,111,87,111,114,108,100,34,44,10,32,32,34,100,111,99,34,58,32,34,80,114,111,116,111,99,111,108,32,71,114,101,101,116,105,110,103,115,34,44,10,32,32,34,116,121,112,101,115,34,58,32,91,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,114,101,99,111,114,100,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,44,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,67,117,114,115,101,34,44,32,34,116,121,112,101,34,58,32,34,101,114,114,111,114,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,93,44,10,32,32,34,109,101,115,115,97,103,101,115,34,58,32,123,10,32,32,32,32,34,104,101,108,108,111,34,58,32,123,10,32,32,32,32,32,32,32,34,100,111,99,34,58,32,34,83,97,121,32,104,101,108,108,111,46,34,44,10,32,32,32,32,32,32,32,34,114,101,113,117,101,115,116,34,58,32,91,123,34,110,97,109,101,34,58,32,34,103,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,71,114,101,101,116,105,110,103,34,32,125,93,44,10,32,32,32,32,32,32,32,34,114,101,115,112,111,110,115,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,10,32,32,32,32,32,32,32,34,101,114,114,111,114,115,34,58,32,91,34,67,117,114,115,101,34,93,10,32,32,32,32,125,10,32,32,125,10,125,
                                     2,1,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                     0])), resposeNone, "response NONE data mismatch")
        let (r, got) = try client.decodeResponse(responseData:resposeNone)
        XCTAssertEqual(r.match, HandshakeMatch.NONE, "match NONE mismatch")
        XCTAssertEqual(r.serverHash, arg.serverHash, "server hash mismatch")
        XCTAssertEqual(r.serverProtocol!, arg.supportProtocol, "server hash mismatch")
        XCTAssertEqual(r.meta, nil, "meta mismatch")
        XCTAssertEqual(got, Data(),"response payload mismatch")
        got.forEach { UInt8 in
            print(UInt8,terminator: ",")
        }
        let requestWithCorrectHash = try client.resolveHandshakeResponse(response: r)
        XCTAssertEqual(Data([0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                             2,
                             194,7,123,10,32,32,34,110,97,109,101,115,112,97,99,101,34,58,32,34,99,111,109,46,97,99,109,101,34,44,10,32,32,34,112,114,111,116,111,99,111,108,34,58,32,34,72,101,108,108,111,87,111,114,108,100,34,44,10,32,32,34,100,111,99,34,58,32,34,80,114,111,116,111,99,111,108,32,71,114,101,101,116,105,110,103,115,34,44,10,32,32,34,116,121,112,101,115,34,58,32,91,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,114,101,99,111,114,100,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,44,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,67,117,114,115,101,34,44,32,34,116,121,112,101,34,58,32,34,101,114,114,111,114,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,93,44,10,32,32,34,109,101,115,115,97,103,101,115,34,58,32,123,10,32,32,32,32,34,104,101,108,108,111,34,58,32,123,10,32,32,32,32,32,32,32,34,100,111,99,34,58,32,34,83,97,121,32,104,101,108,108,111,46,34,44,10,32,32,32,32,32,32,32,34,114,101,113,117,101,115,116,34,58,32,91,123,34,110,97,109,101,34,58,32,34,103,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,71,114,101,101,116,105,110,103,34,32,125,93,44,10,32,32,32,32,32,32,32,34,114,101,115,112,111,110,115,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,10,32,32,32,32,32,32,32,34,101,114,114,111,114,115,34,58,32,91,34,67,117,114,115,101,34,93,10,32,32,32,32,125,10,32,32,125,10,125,
                             0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10,0]), requestWithCorrectHash, "request CLIENT data mismatch")
        let (_, resposeBoth) = try server.resolveHandshakeRequest(requestData: requestWithCorrectHash!)
        XCTAssertEqual(Data([UInt8]([0,0,0,0])), resposeBoth, "response Both data mismatch")
        // request with correct sever hash
        let (_,resposeClient) = try server.resolveHandshakeRequest(requestData: Data([0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                                                                0,
                                                                                0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                                                               0]))
        XCTAssertEqual(Data([UInt8]([2,2, 194,7,123,10,32,32,34,110,97,109,101,115,112,97,99,101,34,58,32,34,99,111,109,46,97,99,109,101,34,44,10,32,32,34,112,114,111,116,111,99,111,108,34,58,32,34,72,101,108,108,111,87,111,114,108,100,34,44,10,32,32,34,100,111,99,34,58,32,34,80,114,111,116,111,99,111,108,32,71,114,101,101,116,105,110,103,115,34,44,10,32,32,34,116,121,112,101,115,34,58,32,91,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,114,101,99,111,114,100,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,44,10,32,32,32,32,32,123,34,110,97,109,101,34,58,32,34,67,117,114,115,101,34,44,32,34,116,121,112,101,34,58,32,34,101,114,114,111,114,34,44,32,34,102,105,101,108,100,115,34,58,32,91,123,34,110,97,109,101,34,58,32,34,109,101,115,115,97,103,101,34,44,32,34,116,121,112,101,34,58,32,34,115,116,114,105,110,103,34,125,93,125,93,44,10,32,32,34,109,101,115,115,97,103,101,115,34,58,32,123,10,32,32,32,32,34,104,101,108,108,111,34,58,32,123,10,32,32,32,32,32,32,32,34,100,111,99,34,58,32,34,83,97,121,32,104,101,108,108,111,46,34,44,10,32,32,32,32,32,32,32,34,114,101,113,117,101,115,116,34,58,32,91,123,34,110,97,109,101,34,58,32,34,103,114,101,101,116,105,110,103,34,44,32,34,116,121,112,101,34,58,32,34,71,114,101,101,116,105,110,103,34,32,125,93,44,10,32,32,32,32,32,32,32,34,114,101,115,112,111,110,115,101,34,58,32,34,71,114,101,101,116,105,110,103,34,44,10,32,32,32,32,32,32,32,34,101,114,114,111,114,115,34,58,32,91,34,67,117,114,115,101,34,93,10,32,32,32,32,125,10,32,32,125,10,125,
                                     2,1,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,
                                     0])), resposeClient, "response NONE data mismatch")
        let (decodeClientResponsse, payload) = try client.decodeResponse(responseData: resposeClient)
        XCTAssertEqual(decodeClientResponsse.match,HandshakeMatch.CLIENT, "response Client data mismatch")
        XCTAssertEqual(decodeClientResponsse.serverProtocol, arg.supportProtocol, "response protocol data mismatch")
        XCTAssertEqual(decodeClientResponsse.serverHash, [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0xA,0xB,0xC,0xD,0xE,0xF,0x10], "response server hash mismatch")
        XCTAssertEqual(decodeClientResponsse.meta, nil, "response Client data mismatch")
        XCTAssertEqual(payload, Data(), "response Client data mismatch")
    } catch {
        XCTAssert(false, "handshake failed")
    }
}
    
func testRequestPing() {
    let arg = testArg()
    do {
        let server = try MessageResponse(context: arg.context, serverHash: arg.serverHash, serverProtocol: arg.supportProtocol)
        let client = try MessageRequest(context: arg.context, clientHash: arg.serverHash, clientProtocol: arg.supportProtocol)
        let requestData = try client.encodeHandshakeRequest(request: HandshakeRequest(clientHash: arg.serverHash, clientProtocol: arg.supportProtocol, serverHash: arg.serverHash))
        try client.addSession(hash: arg.serverHash, protocolString: arg.supportProtocol)
        let (requestHandshake,resposeNone) = try server.resolveHandshakeRequest(requestData: requestData)
        let (r, got) = try client.decodeResponse(responseData:resposeNone)
        XCTAssertEqual(r.match, HandshakeMatch.BOTH, "BOTH NONE mismatch")
        XCTAssertEqual(r.serverHash, nil, "server hash mismatch")
        XCTAssertEqual(r.serverProtocol, nil, "server hash mismatch")
        XCTAssertEqual(r.meta, nil, "meta mismatch")
        XCTAssertEqual(got, Data(),"response payload mismatch")
        struct requestMessage:Codable {
        }
        let requestMessageData = requestMessage()
        let msgData = try client.writeRequest(messageName: "", parameters: [requestMessageData])
        msgData.forEach { UInt8 in
            print(UInt8,terminator: ",")
        }
        var expectData = Data()
        expectData.append(contentsOf: [0,0]) // empty meta and empty message name
        XCTAssertEqual(msgData, expectData,"response payload mismatch")
        let (meta,messageName,request) = try server.readRequest(header: requestHandshake, from: msgData) as ([String: [UInt8]]?, String?, [requestMessage])
        XCTAssertEqual(meta, nil,"response payload mismatch")
        XCTAssertEqual(messageName, nil,"response payload mismatch")
        XCTAssertEqual(request.count, 0,"response payload mismatch")
    } catch {
        XCTAssert(false, "handshake failed")
    }
}
    
func testRequestOK() {
    let arg = testArg()
    do {
        let server = try MessageResponse(context: arg.context, serverHash: arg.serverHash, serverProtocol: arg.supportProtocol)
        let client = try MessageRequest(context: arg.context, clientHash: arg.serverHash, clientProtocol: arg.supportProtocol)
        let requestData = try client.encodeHandshakeRequest(request: HandshakeRequest(clientHash: arg.serverHash, clientProtocol: arg.supportProtocol, serverHash: arg.serverHash))
        try client.addSession(hash: arg.serverHash, protocolString: arg.supportProtocol)
        let (requestHandshake,resposeNone) = try server.resolveHandshakeRequest(requestData: requestData)
        let (r, got) = try client.decodeResponse(responseData:resposeNone)
        XCTAssertEqual(r.match, HandshakeMatch.BOTH, "BOTH NONE mismatch")
        XCTAssertEqual(r.serverHash, nil, "server hash mismatch")
        XCTAssertEqual(r.serverProtocol, nil, "server hash mismatch")
        XCTAssertEqual(r.meta, nil, "meta mismatch")
        XCTAssertEqual(got, Data(),"response payload mismatch")
        struct requestMessage:Codable {
            var message: String = "requestData"
        }
        let requestMessageData = requestMessage()
        let msgData = try client.writeRequest(messageName: "hello", parameters: [requestMessageData])
        msgData.forEach { UInt8 in
            print(UInt8,terminator: ",")
        }
        var expectData = Data()
        expectData.append(contentsOf: [0,10]) // empty meta and length of message name
        expectData.append("hello".data(using: .utf8)!) // message name
        expectData.append(contentsOf: [22]) // length of message
        expectData.append("requestData".data(using: .utf8)!) //message
        XCTAssertEqual(msgData, expectData,"response payload mismatch")
        let (meta,messageName,request) = try server.readRequest(header: requestHandshake, from: msgData) as ([String: [UInt8]]?, String?, [requestMessage])
        XCTAssertEqual(meta, nil,"response payload mismatch")
        XCTAssertEqual(messageName, "hello","response payload mismatch")
        XCTAssertEqual(request.count, 1,"response payload mismatch")
        XCTAssertEqual(request[0].message, "requestData","response payload mismatch")
    } catch {
        XCTAssert(false, "handshake failed")
    }
}
    
}

