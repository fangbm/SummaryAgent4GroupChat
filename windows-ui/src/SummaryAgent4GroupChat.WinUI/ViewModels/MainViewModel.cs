using System.Text;
using System.Text.Json;
using CommunityToolkit.Mvvm.ComponentModel;
using Microsoft.UI.Dispatching;
using SummaryAgent4GroupChat.WinUI.Services;

namespace SummaryAgent4GroupChat.WinUI.ViewModels;

public sealed partial class MainViewModel : ObservableObject
{
    private readonly CancellationTokenSource _lifetime = new();
    private readonly DispatcherQueue _dispatcher = DispatcherQueue.GetForCurrentThread();
    private ControlClient? _client;
    private bool _initialized;

    [ObservableProperty] private string _configText = string.Empty;
    [ObservableProperty] private string _validationMessage = "正在连接控制服务…";
    [ObservableProperty] private string _statusSummary = "主程序未托管运行";
    [ObservableProperty] private string _terminalText = "GUI 已就绪，主程序终端输出会显示在这里。\n";
    [ObservableProperty] private string _logText = "正在读取日志…";
    [ObservableProperty] private string _notice = string.Empty;
    [ObservableProperty] private bool _isAgentRunning;
    [ObservableProperty] private bool _followTerminal = true;
    [ObservableProperty] private bool _followLogs = true;

    public async Task InitializeAsync()
    {
        if (_initialized)
        {
            return;
        }
        _initialized = true;
        try
        {
            _client = await ControlBootstrap.ConnectAsync(_lifetime.Token);
            await RefreshAsync();
            _ = SubscribeOutputAsync(_lifetime.Token);
            _ = PollLogsAsync(_lifetime.Token);
        }
        catch (Exception error)
        {
            Notice = $"控制服务启动失败：{error.Message}";
            ValidationMessage = Notice;
        }
    }

    public async Task RefreshAsync()
    {
        if (_client is null)
        {
            return;
        }
        try
        {
            var configReply = await _client.CallAsync("config.read", cancellationToken: _lifetime.Token);
            configReply.ThrowIfError();
            var config = configReply.Result!.Value;
            ConfigText = config.GetProperty("toml").GetString() ?? string.Empty;
            ValidationMessage = config.GetProperty("validation").GetString() is { Length: > 0 } validation ? validation : "配置有效";

            var statusReply = await _client.CallAsync("status.get", cancellationToken: _lifetime.Token);
            statusReply.ThrowIfError();
            ApplyStatus(statusReply.Result!.Value);
            await RefreshLogsAsync();
            Notice = "已刷新";
        }
        catch (Exception error)
        {
            Notice = $"刷新失败：{error.Message}";
        }
    }

    public async Task ValidateAsync()
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("config.validate", new { toml = ConfigText }, _lifetime.Token);
            reply.ThrowIfError();
            var result = reply.Result!.Value;
            ValidationMessage = result.GetProperty("message").GetString() ?? "校验完成";
        }
        catch (Exception error)
        {
            ValidationMessage = error.Message;
        }
    }

    public async Task SaveAsync()
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("config.write", new { toml = ConfigText }, _lifetime.Token);
            reply.ThrowIfError();
            ValidationMessage = "配置已保存，主程序会自动热重载。";
            Notice = "配置已保存";
            await RefreshAsync();
        }
        catch (Exception error)
        {
            ValidationMessage = $"保存失败：{error.Message}";
        }
    }

    public async Task StartAgentAsync() => await AgentCommandAsync("agent.start", "主程序已启动");
    public async Task StopAgentAsync() => await AgentCommandAsync("agent.stop", "主程序已停止");
    public async Task InstallRuntimeAsync() => await AgentCommandAsync("runtime.install", "已请求管理员权限安装微信运行环境");
    public async Task RunWxdbInitAsync() => await AgentCommandAsync("wxdb.init", "已请求管理员权限运行 wxdb init");
    public async Task OpenPathAsync(string kind) => await AgentCommandAsync("path.open", "已打开路径", new { kind });

    public void ClearTerminal() => TerminalText = string.Empty;
    public void ClearLogs() => LogText = string.Empty;

    private async Task AgentCommandAsync(string method, string success, object? parameters = null)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync(method, parameters, _lifetime.Token);
            reply.ThrowIfError();
            Notice = success;
            var status = await _client.CallAsync("status.get", cancellationToken: _lifetime.Token);
            status.ThrowIfError();
            ApplyStatus(status.Result!.Value);
        }
        catch (Exception error)
        {
            Notice = $"操作失败：{error.Message}";
        }
    }

    private async Task SubscribeOutputAsync(CancellationToken cancellationToken)
    {
        if (_client is null) return;
        try
        {
            await _client.SubscribeAsync("output.subscribe", eventData =>
            {
                var data = eventData.GetProperty("data");
                var source = data.GetProperty("source").GetString() ?? "output";
                var text = data.GetProperty("text").GetString() ?? string.Empty;
                _dispatcher.TryEnqueue(() => AppendTerminal($"[{source}] {text}\n"));
                return Task.CompletedTask;
            }, cancellationToken);
        }
        catch (Exception error) when (!cancellationToken.IsCancellationRequested)
        {
            _dispatcher.TryEnqueue(() => AppendTerminal($"[gui] 终端订阅已断开：{error.Message}\n"));
        }
    }

    private async Task PollLogsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await RefreshLogsAsync();
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
        }
    }

    private async Task RefreshLogsAsync()
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("logs.tail", cancellationToken: _lifetime.Token);
            reply.ThrowIfError();
            var value = reply.Result!.Value.GetProperty("text").GetString() ?? string.Empty;
            _dispatcher.TryEnqueue(() => LogText = value);
        }
        catch (Exception error)
        {
            _dispatcher.TryEnqueue(() => LogText = $"读取日志失败：{error.Message}");
        }
    }

    private void ApplyStatus(JsonElement status)
    {
        IsAgentRunning = status.GetProperty("agent_running").GetBoolean();
        var platform = status.GetProperty("platform").GetString() ?? "wx";
        var targets = status.GetProperty("targets").GetInt32();
        StatusSummary = $"{(IsAgentRunning ? "主程序运行中" : "主程序未托管运行")} · 平台 {platform} · 目标 {targets} 个";
    }

    private void AppendTerminal(string text)
    {
        const int maxChars = 160 * 1024;
        var combined = TerminalText + text;
        TerminalText = combined.Length > maxChars ? combined[^maxChars..] : combined;
    }
}
