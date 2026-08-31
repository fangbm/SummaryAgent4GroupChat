import SwiftUI

private enum SidebarItem: String, CaseIterable, Identifiable {
    case dashboard = "仪表盘"
    case platforms = "接入平台"
    case schedules = "定时总结"
    case models = "模型与媒体"
    case runtime = "运行信息"
    case settings = "设置"

    var id: Self { self }
    var systemImage: String {
        switch self {
        case .dashboard: "rectangle.3.group"
        case .platforms: "point.3.connected.trianglepath.dotted"
        case .schedules: "calendar"
        case .models: "sparkles"
        case .runtime: "terminal"
        case .settings: "gearshape"
        }
    }
}

struct ContentView: View {
    @State private var selection: SidebarItem? = .dashboard

    var body: some View {
        NavigationSplitView {
            List(SidebarItem.allCases, selection: $selection) { item in
                Label(item.rawValue, systemImage: item.systemImage).tag(item)
            }
            .navigationTitle("SummaryAgent4GroupChat")
        } detail: {
            switch selection ?? .dashboard {
            case .dashboard: DashboardView()
            case .platforms: PlatformView()
            case .schedules: CapabilityView(title: "定时总结", text: "定时任务由共享 Rust 配置与任务中心执行。完整的表单编辑器会跟随 Windows 控制协议继续迁移。")
            case .models: CapabilityView(title: "模型与媒体", text: "LLM、NovelAI、图片/视频转述与语音转写配置保持与 Windows 版兼容。此阶段可使用“设置”页打开同一份 agent.toml。")
            case .runtime: RuntimeView()
            case .settings: SettingsView()
            }
        }
        .task { await refreshOnLaunch() }
    }

    @EnvironmentObject private var model: AgentModel

    private func refreshOnLaunch() async {
        guard !model.configPath.isEmpty else { return }
        await model.connectAndRefresh()
    }
}

private struct DashboardView: View {
    @EnvironmentObject private var model: AgentModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text("仪表盘").font(.largeTitle)
                Text(model.message).foregroundStyle(.secondary)
                HStack(spacing: 12) {
                    statusCard("主程序", value: model.status?.agent_running == true ? "运行中" : "未运行", icon: "power")
                    statusCard("平台", value: model.status?.platform.uppercased() ?? "未连接", icon: "network")
                    statusCard("目标", value: model.status.map { "\($0.targets) 个" } ?? "-", icon: "number")
                    statusCard("LLM", value: model.status?.llm_configured == true ? "已配置" : "未配置", icon: "brain")
                }
                HStack {
                    Button("启动主程序") { Task { await model.startAgent() } }
                        .buttonStyle(.borderedProminent)
                    Button("停止主程序") { Task { await model.stopAgent() } }
                    Button("刷新") { Task { await model.connectAndRefresh() } }
                    if model.isBusy { ProgressView().controlSize(.small) }
                }
                macOSNotice
            }
            .padding(28)
        }
        .navigationTitle("仪表盘")
    }

    private func statusCard(_ title: String, value: String, icon: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Label(title, systemImage: icon).foregroundStyle(.secondary)
            Text(value).font(.title3.weight(.semibold))
        }
        .frame(maxWidth: .infinity, minHeight: 96, alignment: .leading)
        .padding()
        .background(.background, in: RoundedRectangle(cornerRadius: 12))
        .overlay(RoundedRectangle(cornerRadius: 12).stroke(.quaternary))
    }

    private var macOSNotice: some View {
        ContentUnavailableView(
            "微信能力仅限 Windows",
            systemImage: "desktopcomputer.trianglebadge.exclamationmark",
            description: Text("macOS 版支持 Discord、模型、任务与投递能力。wx4py 和 wxdb 依赖 Windows 微信客户端，因此在 Mac 上不会启动。")
        )
        .frame(maxWidth: .infinity)
        .padding(.top, 24)
    }
}

private struct PlatformView: View {
    @EnvironmentObject private var model: AgentModel

    var body: some View {
        Form {
            Section("当前接入") {
                LabeledContent("平台", value: model.status?.platform.uppercased() ?? "未连接")
                LabeledContent("目标数", value: model.status.map { "\($0.targets)" } ?? "-")
            }
            Section("macOS 支持范围") {
                Label("Discord Bot 与频道/论坛帖子线程可用", systemImage: "checkmark.circle.fill")
                    .foregroundStyle(.green)
                Label("微信 wx4py 与 wxdb 仅支持 Windows", systemImage: "xmark.circle.fill")
                    .foregroundStyle(.orange)
            }
            Section("配置") {
                Button("刷新平台状态") { Task { await model.connectAndRefresh() } }
                Button("打开 agent.toml") { model.openConfig() }
                    .disabled(model.configPath.isEmpty)
            }
        }
        .formStyle(.grouped)
        .navigationTitle("接入平台")
    }
}

private struct RuntimeView: View {
    @EnvironmentObject private var model: AgentModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("运行信息").font(.largeTitle)
                Spacer()
                Button("刷新日志") { Task { await model.connectAndRefresh() } }
            }
            Text(model.status?.working_dir ?? "尚未连接控制服务。").foregroundStyle(.secondary)
            ScrollView {
                Text(model.logTail)
                    .font(.system(.body, design: .monospaced))
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .textSelection(.enabled)
                    .padding()
            }
            .background(.background, in: RoundedRectangle(cornerRadius: 10))
            .overlay(RoundedRectangle(cornerRadius: 10).stroke(.quaternary))
        }
        .padding(28)
        .navigationTitle("运行信息")
    }
}

private struct SettingsView: View {
    @EnvironmentObject private var model: AgentModel

    var body: some View {
        Form {
            Section("共享配置") {
                LabeledContent("配置文件") {
                    Text(model.configPath.isEmpty ? "尚未选择" : model.configPath)
                        .lineLimit(2)
                        .textSelection(.enabled)
                }
                HStack {
                    Button("选择 agent.toml") { model.chooseConfig() }
                    Button("连接控制服务") { Task { await model.connectAndRefresh() } }
                        .disabled(model.configPath.isEmpty)
                    Button("在默认编辑器中打开") { model.openConfig() }
                        .disabled(model.configPath.isEmpty)
                }
            }
            Section("本机控制") {
                Text("SwiftUI 仅通过当前用户的 Unix Socket 与 Rust 控制服务通信；API Key 不会被读取或回显到界面。")
                    .foregroundStyle(.secondary)
            }
        }
        .formStyle(.grouped)
        .navigationTitle("设置")
    }
}

private struct CapabilityView: View {
    let title: String
    let text: String

    var body: some View {
        ContentUnavailableView(title, systemImage: "hammer", description: Text(text))
            .navigationTitle(title)
    }
}
