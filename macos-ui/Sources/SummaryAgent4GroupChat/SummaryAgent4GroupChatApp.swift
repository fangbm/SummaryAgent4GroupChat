import SwiftUI

@main
struct SummaryAgent4GroupChatApp: App {
    @StateObject private var model = AgentModel()

    var body: some Scene {
        WindowGroup("SummaryAgent4GroupChat") {
            ContentView()
                .environmentObject(model)
                .frame(minWidth: 980, minHeight: 680)
        }
        .defaultSize(width: 1180, height: 760)
    }
}
