//
//  TestHelpers.swift
//  SwiftAvroCoreTests
//

import Foundation
import SwiftAvroCore
@testable import SwiftAvroCore

// MARK: - Sync marker helpers

func makeSyncMarker() -> [UInt8] {
    (0..<16).map { _ in UInt8.random(in: 0...255) }
}

func createAvroWithMarker(_ schema: String, syncMarker: [UInt8]? = nil) -> (Avro, [UInt8]) {
    let marker = syncMarker ?? makeSyncMarker()
    let avro = Avro()
    avro.decodeSchema(schema: schema)
    return (avro, marker)
}
