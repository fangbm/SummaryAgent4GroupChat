// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "SummaryAgent4GroupChatMac",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "SummaryAgent4GroupChat", targets: ["SummaryAgent4GroupChat"]),
    ],
    targets: [
        .executableTarget(
            name: "SummaryAgent4GroupChat",
            path: "Sources/SummaryAgent4GroupChat"
        ),
    ]
)
