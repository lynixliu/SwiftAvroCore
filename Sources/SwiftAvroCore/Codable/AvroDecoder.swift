//
//  AvroClient/AvroDecoder.swift
//
//  Created by Yang Liu on 6/09/18.
//  Copyright © 2018 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation

final class AvroDecoder {
    private let schema: AvroSchema
    fileprivate let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!
    public var userInfo: [CodingUserInfoKey : Any] = [CodingUserInfoKey : Any]()
    init(schema: AvroSchema) {
        self.schema = schema
        userInfo[infoKey] = AvroEncodingOption.AvroBinary
    }
    
    func setUserInfo(userInfo: [CodingUserInfoKey : Any]) {
        self.userInfo = userInfo
    }
    
    func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let encodingOption = userInfo[infoKey] as! AvroEncodingOption
        switch encodingOption {
        case .AvroBinary:
            let decoder = try AvroBinaryDecoder(schema: schema, data: data)
            return try decoder.decode(type)
        case .AvroJson:
            return try JSONDecoder().decode(type, from: data)
        }
    }
}
