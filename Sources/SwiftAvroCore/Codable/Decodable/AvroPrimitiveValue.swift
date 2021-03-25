//
//  AvroKeyedDecodingContainer.swift
//
//  Created by Kacper Kawecki on 24/03/2021.
//  Copyright © 2021 by Kacper Kawecki and the project authors.
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

internal enum AvroPrimitiveValue {
    case null
    case boolean(Bool)
    case int(Int64)
    case long(Int64)
    case float(Float)
    case double(Double)
    case byte(UInt8)
    case bytes([UInt8])
    case string(String)
    case durationElement(UInt32)
}

internal enum AvroLogicalValue {
    case decimal([UInt8], precision: Int?, scale: Int?)
    case uuid(String)
    case date(Int64)
    case timeMillis(Int64)
    case timeMicros(Int64)
    case timestampMillis(Int64)
    case timestampMicros(Int64)
    case localTimestampMillis(Int64)
    case localTimestampMicros(Int64)
    case duration([UInt8])
}