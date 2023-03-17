//
//  File.swift
//  
//
//  Created by standard on 3/17/23.
//

import Foundation


//
//extension Data {
//    static func random(length: Int) throws -> Data {
//        return Data((0 ..< length).map { _ in UInt8.random(in: UInt8.min ... UInt8.max) })
//    }
//}
//

struct Kitty: Codable, Equatable {
    enum KittyColor: String, Codable, CaseIterable {
        case Brown
        case White
        case Black
    }
    let name: String
    let color: KittyColor
    
    static func random() -> Self {
        Self(name:[
            "Whiskers",
            "Felix",
            "Oscar",
            "Smudge",
            "Fluffy",
            "Angel",
            "Lady",
            "Lucky"
        ].randomElement()!
             , color:KittyColor.allCases.randomElement()!
        )
    }
}


struct KittyAction: Codable, Equatable {
    enum KittyActionType:  String, Codable, CaseIterable {
        case meow
        case jump
        case bite
    }
    let label: String
    let type: KittyActionType
    let timestamp: Date
    let dataValue: [UInt8]
    let intValue: Int
    let floatValue: Float
    let doubleValue: Double
    let kitty: Kitty
    
    static func random() -> Self {
        Self(label: [
            "yah just kidding",
            "random text",
            "very very very very very very long label",
            "hahahhhahah"
        ].randomElement()!, type: KittyActionType.allCases.randomElement()!,
             timestamp: Date(),
             dataValue: [UInt8.random(in: 1...40), UInt8.random(in: 1...40), UInt8.random(in: 1...40)],
             intValue: Int.random(in: -100...4990),
             floatValue: Float.random(in: -1000...40),
             doubleValue: Double.random(in: -100...40),
             kitty: Kitty.random())
    }
}
