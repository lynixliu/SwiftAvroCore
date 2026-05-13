//
//  TestHelpers.swift
//  SwiftAvroRpcTests
//
//  Shared fixtures used across AvroIPCTests and AvroIPCHTTPTests.
//

import Foundation
import SwiftAvroCore
@testable import SwiftAvroRpc

// MARK: - Protocol

let helloProtocol = """
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "types": [
    {"name": "Greeting", "type": "record", "fields": [{"name": "message", "type": "string"}]},
    {"name": "Curse",    "type": "error",  "fields": [{"name": "message", "type": "string"}]}
  ],
  "messages": {
    "hello": {
      "request":  [{"name": "greeting", "type": "Greeting"}],
      "response": "Greeting",
      "errors":   ["Curse"]
    }
  }
}
"""

// MARK: - Hashes

let testClientHash: MD5Hash = Array(repeating: 0x01, count: 16)
let testServerHash: MD5Hash = Array(repeating: 0x02, count: 16)

// MARK: - Types

struct Greeting: Codable, Sendable, Equatable {
    var message: String
}

// MARK: - Handlers

struct EchoHandler: AvroIPCHandler {
    func handle(messageName: String, requestData: Data) async throws -> Data {
        requestData
    }
}

struct GreetingHandler: AvroIPCHandler {
    func handle(messageName: String, requestData: Data) async throws -> Data {
        let avro   = Avro()
        let schema = avro.newSchema(schema: """
            {"type":"record","name":"Greeting","namespace":"com.acme",
             "fields":[{"name":"message","type":"string"}]}
            """)!
        return try avro.encodeFrom(Greeting(message: "hello back"), schema: schema)
    }
}

struct FailingHandler: AvroIPCHandler {
    func handle(messageName: String, requestData: Data) async throws -> Data {
        throw AvroIPCError.noHandler(messageName)
    }
}

// MARK: - Utilities

func extractPort(from address: String?) -> Int {
    guard let address,
          let colonIndex = address.lastIndex(of: ":"),
          let port = Int(address[address.index(after: colonIndex)...]) else {
        return 0
    }
    return port
}
