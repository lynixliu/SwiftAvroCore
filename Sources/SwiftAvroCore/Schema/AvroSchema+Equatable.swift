//
//  AvroClient/AvroSchema+Equatable.swift
//
//  Created by Yang Liu on 30/08/18.
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
// MARK: - Equatable & Hashable

extension AvroSchema {

    public static func == (lhs: AvroSchema, rhs: AvroSchema) -> Bool {
        switch (lhs, rhs) {
        case (.nullSchema,    .nullSchema):    return true
        case (.booleanSchema, .booleanSchema): return true
        case (.floatSchema,   .floatSchema):   return true
        case (.doubleSchema,  .doubleSchema):  return true
        case (.stringSchema,  .stringSchema):  return true
        case let (.intSchema(l),    .intSchema(r)):    return l == r
        case let (.longSchema(l),   .longSchema(r)):   return l == r
        case let (.bytesSchema(l),  .bytesSchema(r)):  return l == r
        case let (.recordSchema(l), .recordSchema(r)): return l == r
        case let (.arraySchema(l),  .arraySchema(r)):  return l == r
        case let (.mapSchema(l),    .mapSchema(r)):    return l == r
        case let (.fixedSchema(l),  .fixedSchema(r)):  return l == r
        case let (.enumSchema(l),   .enumSchema(r)):   return l == r
        case let (.unionSchema(l),  .unionSchema(r)):  return l == r
        case let (.fieldSchema(l),  .fieldSchema(r)):  return l == r
        case let (.fieldsSchema(l), .fieldsSchema(r)): return l == r
        case let (.errorSchema(l),  .errorSchema(r)):   return l == r
        default: return false
        }
    }

    public func hash(into hasher: inout Hasher) {
        switch self {
        case .nullSchema:           hasher.combine(Types.null)
        case .booleanSchema:        hasher.combine(Types.boolean)
        case .floatSchema:          hasher.combine(Types.float)
        case .doubleSchema:         hasher.combine(Types.double)
        case .stringSchema:         hasher.combine(Types.string)
        case .intSchema(let s):     hasher.combine(s.type)
        case .longSchema(let s):    hasher.combine(s.type)
        case .bytesSchema(let s):   hasher.combine(s.type)
        case .recordSchema(let s):  hasher.combine(s.namespace); hasher.combine(s.name)
        case .arraySchema(let s):   hasher.combine(s.type);      hasher.combine(s.items)
        case .mapSchema(let s):     hasher.combine(s.type);      hasher.combine(s.values)
        case .fixedSchema(let s):   hasher.combine(s.namespace); hasher.combine(s.name)
        case .enumSchema(let s):    hasher.combine(s.namespace); hasher.combine(s.name)
        case .unionSchema(let s):   s.branches.forEach { hasher.combine($0) }
        case .fieldSchema(let s):   hasher.combine(s.name)
        case .fieldsSchema(let ss): ss.forEach { hasher.combine($0.name) }
        case .errorSchema(let s):   hasher.combine(s.namespace); hasher.combine(s.name)
        case .unknownSchema:        hasher.combine(ObjectIdentifier(AvroSchema.self))
        }
    }
}
