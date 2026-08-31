import AppKit
import Darwin
import Foundation

@MainActor
final class AgentModel: ObservableObject {
    @Published private(set) var status: AgentStatus?
    @Published private(set) var logTail = "尚未连接控制服务。"
    @Published private(set) var message = "请选择 SummaryAgent4GroupChat 的 agent.toml。"
    @Published private(set) var isBusy = false
    @Published var configPath: String

    private var controlProcess: Process?
    private var client: ControlClient?

    init() {
        configPath = UserDefaults.standard.string(forKey: "SummaryAgent4GroupChat.configPath") ?? ""
    }

    func chooseConfig() {
        if status?.agent_running == true {
            message = "请先停止主程序，再切换配置文件。"
            return
        }
        let panel = NSOpenPanel()
        panel.title = "选择 agent.toml"
        panel.prompt = "使用此配置"
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.allowsMultipleSelection = false
        panel.allowedFileTypes = ["toml"]
        if panel.runModal() == .OK, let url = panel.url {
            configPath = url.path
            UserDefaults.standard.set(url.path, forKey: "SummaryAgent4GroupChat.configPath")
            message = "已选择配置：\(url.lastPathComponent)"
            disconnect()
        }
    }

    func connectAndRefresh() async {
        guard !configPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            message = "请先选择 agent.toml。"
            return
        }
        isBusy = true
        defer { isBusy = false }
        do {
            if client == nil {
                try startControl()
            }
            try await refresh()
            message = "控制服务已连接。"
        } catch {
            message = "连接失败：\(error.localizedDescription)"
            disconnect()
        }
    }

    func startAgent() async {
        await performAction("agent.start")
    }

    func stopAgent() async {
        await performAction("agent.stop")
    }

    func refresh() async throws {
        guard let client else { return }
        status = try await client.call("status.get", as: AgentStatus.self)
        let logs = try await client.call("logs.tail", as: LogTail.self)
        logTail = logs.text
    }

    func openConfig() {
        guard !configPath.isEmpty else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: configPath))
    }

    func disconnect() {
        controlProcess?.terminate()
        controlProcess = nil
        client = nil
        status = nil
    }

    private func performAction(_ action: String) async {
        isBusy = true
        defer { isBusy = false }
        do {
            if client == nil { try startControl() }
            guard let client else { return }
            let result = try await client.call(action, as: AgentAction.self)
            try await refresh()
            message = result.message ?? (action == "agent.start" ? "主程序已启动。" : "主程序已停止。")
        } catch {
            message = "操作失败：\(error.localizedDescription)"
        }
    }

    private func startControl() throws {
        let executable = try locateControlExecutable()
        let socket = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("SummaryAgent4GroupChat.\(getuid()).\(UUID().uuidString).sock")
            .path
        let token = UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let configURL = URL(fileURLWithPath: configPath).standardizedFileURL
        guard FileManager.default.fileExists(atPath: configURL.path) else {
            throw CocoaError(.fileNoSuchFile)
        }
        let root = configURL.deletingLastPathComponent().lastPathComponent == "config"
            ? configURL.deletingLastPathComponent().deletingLastPathComponent()
            : configURL.deletingLastPathComponent()
        let process = Process()
        process.executableURL = executable
        process.currentDirectoryURL = root
        process.arguments = ["--pipe", socket, "--config", configURL.path, "--working-dir", root.path]
        var environment = ProcessInfo.processInfo.environment
        environment["SUMMARY_AGENT_CONTROL_TOKEN"] = token
        process.environment = environment
        try process.run()
        controlProcess = process
        client = ControlClient(socketPath: socket, token: token)
    }

    private func locateControlExecutable() throws -> URL {
        if let configured = ProcessInfo.processInfo.environment["SUMMARY_AGENT_CONTROL_PATH"],
           FileManager.default.isExecutableFile(atPath: configured) {
            return URL(fileURLWithPath: configured)
        }
        if let bundled = Bundle.main.resourceURL?
            .appendingPathComponent("bin/wechat-summary-control"),
           FileManager.default.isExecutableFile(atPath: bundled.path) {
            return bundled
        }
        throw NSError(
            domain: "SummaryAgent4GroupChat",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "未找到 wechat-summary-control。安装版应在应用 Resources/bin 中提供它；开发时可设置 SUMMARY_AGENT_CONTROL_PATH。"]
        )
    }
}
