// swift-tools-version:5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftAvroCore",
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "SwiftAvroCore",
            targets: ["SwiftAvroCore"]),
    ],
    dependencies: [
        .package(url: "https://github.com/wickwirew/Runtime.git", from: "2.2.4"),
    ],
    targets: [
        .target(
            name: "SwiftAvroCore",
            dependencies: [
            "Runtime",
            ]),
        .testTarget(
            name: "SwiftAvroCoreTests",
            dependencies: ["SwiftAvroCore"]),
    ]
)
