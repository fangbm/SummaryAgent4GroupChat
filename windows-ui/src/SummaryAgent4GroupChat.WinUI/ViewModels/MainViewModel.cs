using System.Collections.ObjectModel;
using System.Text.Json;
using CommunityToolkit.Mvvm.ComponentModel;
using Microsoft.UI.Dispatching;
using SummaryAgent4GroupChat.WinUI.Models;
using SummaryAgent4GroupChat.WinUI.Services;

namespace SummaryAgent4GroupChat.WinUI.ViewModels;

public sealed partial class MainViewModel : ObservableObject
{
    private readonly CancellationTokenSource _lifetime = new();
    private readonly DispatcherQueue _dispatcher = DispatcherQueue.GetForCurrentThread();
    private ControlClient? _client;
    private bool _initialized;
    // Structured (redacted) view of the current config returned by config.read.
    // The form reads from this and saves through config.patch; it never edits
    // TOML text itself.
    private JsonElement _parsedConfig;

    [ObservableProperty] private string _configText = string.Empty;
    [ObservableProperty] private string _validationMessage = "正在连接控制服务…";
    [ObservableProperty] private string _statusSummary = "主程序未托管运行";
    [ObservableProperty] private string _terminalText = "GUI 已就绪，主程序终端输出会显示在这里。\n";
    [ObservableProperty] private string _logText = "正在读取日志…";
    [ObservableProperty] private string _notice = string.Empty;
    [ObservableProperty] private bool _isAgentRunning;
    [ObservableProperty] private bool _followTerminal = true;
    [ObservableProperty] private bool _followLogs = true;
    [ObservableProperty] private bool _isCheckingUpdates;
    [ObservableProperty] private string _updateCheckStatus = "启动后会自动检查应用与受管理依赖的更新。";
    [ObservableProperty] private bool _dependenciesNeedInstall;
    [ObservableProperty] private string _dependencyStatus = "正在检测运行依赖…";
    [ObservableProperty] private bool _isMaintenanceOperationRunning;
    [ObservableProperty] private string _maintenanceStatus = "尚未运行维护操作。";
    [ObservableProperty] private string _maintenanceOutput = string.Empty;
    public ObservableCollection<UpdateCheckItem> UpdateItems { get; } = [];
    public event Action<string>? MaintenanceDialogRequested;

    [ObservableProperty] private string _platformKind = "wx";
    [ObservableProperty] private string _weChatGroups = string.Empty;
    [ObservableProperty] private string _discordChannels = string.Empty;
    [ObservableProperty] private string _wxdbExecutable = "wxdb";
    [ObservableProperty] private string _wxdbCacheDirectory = string.Empty;
    [ObservableProperty] private string _historyPageSize = "10000";
    [ObservableProperty] private string _disabledImageRooms = string.Empty;

    [ObservableProperty] private string _triggerCommands = "/总结, #总结";
    [ObservableProperty] private string _whitelistRooms = string.Empty;
    [ObservableProperty] private bool _ignoreSelf = true;
    [ObservableProperty] private string _requestCooldownSeconds = "300";
    [ObservableProperty] private string _imageCooldownSeconds = "0";
    [ObservableProperty] private bool _manualImagesByDefault;

    [ObservableProperty] private bool _scheduleEnabled = true;
    [ObservableProperty] private string _scheduleTime = "22:00";
    [ObservableProperty] private string _scheduleRangeHours = "24";
    [ObservableProperty] private string _scheduleRooms = string.Empty;
    [ObservableProperty] private bool _scheduleSendText = true;
    [ObservableProperty] private bool _scheduleSendImage = true;

    [ObservableProperty] private string _llmApiKeyEnvironment = "LLM_API_KEY";
    [ObservableProperty] private string _llmBaseUrl = string.Empty;
    [ObservableProperty] private string _llmModel = string.Empty;
    [ObservableProperty] private string _llmTimeoutSeconds = "120";
    [ObservableProperty] private bool _llmStreamingEnabled = true;
    [ObservableProperty] private string _llmStreamFirstEventTimeoutSeconds = "30";
    [ObservableProperty] private string _llmStreamIdleTimeoutSeconds = "30";
    [ObservableProperty] private string _llmMaxOutputTokens = "2000";
    [ObservableProperty] private string _llmChunkConcurrency = "4";
    [ObservableProperty] private bool _imageGenerationEnabled = true;
    [ObservableProperty] private string _imageApiKeyEnvironment = "IMAGE_API_KEY";
    [ObservableProperty] private string _imageBaseUrl = string.Empty;
    [ObservableProperty] private string _imageModel = string.Empty;
    [ObservableProperty] private string _imageTimeoutSeconds = "300";
    [ObservableProperty] private bool _imageCaptionEnabled;
    [ObservableProperty] private bool _videoCaptionEnabled;
    [ObservableProperty] private bool _voiceTranscriptionEnabled;

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
            _ = SubscribeOperationsAsync(_lifetime.Token);
            _ = PollLogsAsync(_lifetime.Token);
            await CheckRuntimeDependenciesAsync();
            _ = CheckForUpdatesAsync(silent: true);
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
            if (config.TryGetProperty("parsed", out var parsed) && parsed.ValueKind == JsonValueKind.Object)
            {
                _parsedConfig = parsed.Clone();
            }
            else
            {
                _parsedConfig = default;
            }
            LoadFormFromConfig();
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
        var operations = BuildFormOperations();
        if (operations.Count == 0)
        {
            Notice = "配置无变更";
            return;
        }
        await PatchConfigAsync(operations);
    }

    public async Task SaveRawConfigAsync()
    {
        await SaveConfigTextAsync();
    }

    private async Task PatchConfigAsync(List<Dictionary<string, object?>> operations)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("config.patch", new { operations }, _lifetime.Token);
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

    private async Task SaveConfigTextAsync()
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
    public async Task InstallRuntimeAsync() => await StartMaintenanceOperationAsync("runtime.install", "安装微信运行环境");
    public async Task RunWxdbInitAsync() => await StartMaintenanceOperationAsync("wxdb.init", "运行 wxdb init");
    public async Task InstallUpdateAsync(UpdateCheckItem item)
    {
        if (!item.CanInstall || string.IsNullOrWhiteSpace(item.Target)) return;
        object parameters = item.Target == "pip"
            ? new { target = item.Target, package = item.PackageName }
            : new { target = item.Target };
        await StartMaintenanceOperationAsync("update.install", $"更新 {item.Name}", parameters);
    }
    public async Task OpenPathAsync(string kind) => await AgentCommandAsync("path.open", "已打开路径", new { kind });

    public async Task<bool> CheckRuntimeDependenciesAsync()
    {
        if (_client is null) return false;
        DependencyStatus = "正在检测运行依赖…";
        try
        {
            var reply = await _client.CallAsync("runtime.check", cancellationToken: _lifetime.Token);
            reply.ThrowIfError();
            var result = reply.Result!.Value;
            DependenciesNeedInstall = !result.GetProperty("ready").GetBoolean();
            DependencyStatus = result.GetProperty("detail").GetString() ?? "运行依赖检查完成。";
            return DependenciesNeedInstall;
        }
        catch (Exception error)
        {
            DependenciesNeedInstall = false;
            DependencyStatus = $"运行依赖检查失败：{error.Message}";
            return false;
        }
    }

    public async Task CheckForUpdatesAsync(bool silent = false)
    {
        if (_client is null || IsCheckingUpdates) return;
        IsCheckingUpdates = true;
        UpdateCheckStatus = "正在检查应用与依赖更新…";
        try
        {
            var reply = await _client.CallAsync("update.check", cancellationToken: _lifetime.Token);
            reply.ThrowIfError();
            var result = reply.Result!.Value;
            UpdateItems.Clear();
            foreach (var entry in result.GetProperty("entries").EnumerateArray())
            {
                var status = ReadUpdateValue(entry, "status", "unknown");
                var updateAvailable = entry.TryGetProperty("update_available", out var available) && available.ValueKind == JsonValueKind.True;
                var name = ReadUpdateValue(entry, "name", "未知组件");
                var target = name switch
                {
                    "SummaryAgent4GroupChat" => "application",
                    "wxdb" => "wxdb",
                    _ when name.StartsWith("Python: ", StringComparison.Ordinal) => "pip",
                    _ => string.Empty,
                };
                var packageName = target == "pip" ? name["Python: ".Length..] : null;
                var canInstall = updateAvailable || target == "wxdb";
                UpdateItems.Add(new UpdateCheckItem(
                    name,
                    ReadUpdateValue(entry, "current_version", "未检测"),
                    ReadUpdateValue(entry, "latest_version", "无"),
                    UpdateStatusText(status),
                    ReadUpdateValue(entry, "detail", string.Empty),
                    updateAvailable,
                    canInstall,
                    target,
                    packageName));
            }
            var updateCount = result.GetProperty("update_count").GetInt32();
            UpdateCheckStatus = updateCount > 0
                ? $"发现 {updateCount} 个可更新组件。"
                : "已检查，受管理组件均为最新或没有可用更新。";
            if (updateCount > 0)
            {
                Notice = UpdateCheckStatus;
            }
            else if (!silent)
            {
                Notice = "更新检查已完成";
            }
            await CheckRuntimeDependenciesAsync();
        }
        catch (Exception error)
        {
            UpdateCheckStatus = $"更新检查失败：{error.Message}";
            if (!silent) Notice = UpdateCheckStatus;
        }
        finally
        {
            IsCheckingUpdates = false;
        }
    }

    public void ClearTerminal() => TerminalText = string.Empty;
    public void ClearLogs() => LogText = string.Empty;

    public void LoadFormFromConfig()
    {
        PlatformKind = ReadString("platform", "kind", "wx");
        WeChatGroups = ReadList("wx4py", "groups");
        DiscordChannels = ReadList("discord", "channels");
        WxdbExecutable = ReadString("wxdb", "executable", "wxdb");
        WxdbCacheDirectory = ReadString("wxdb", "cache_dir", string.Empty);
        HistoryPageSize = ReadString("history", "max_messages", "10000");
        DisabledImageRooms = string.Join(", ", ReadDisabledImageRooms());

        TriggerCommands = ReadList("listen", "triggers");
        WhitelistRooms = ReadList("listen", "whitelist_rooms");
        IgnoreSelf = ReadBool("listen", "ignore_self", true);
        RequestCooldownSeconds = ReadString("rate_limit", "successful_request_cooldown_seconds", "300");
        ImageCooldownSeconds = ReadString("rate_limit", "successful_image_cooldown_seconds", "0");
        ManualImagesByDefault = ReadBool("manual_summary", "image_by_default", false);

        ScheduleEnabled = ReadBool("scheduled_summary", "enabled", true);
        var hour = ReadString("scheduled_summary", "local_hour", "22");
        var minute = ReadString("scheduled_summary", "local_minute", "0");
        ScheduleTime = $"{ParseInt(hour, 22):00}:{ParseInt(minute, 0):00}";
        ScheduleRangeHours = ReadString("scheduled_summary", "range_hours", "24");
        ScheduleRooms = ReadList("scheduled_summary", "rooms");
        ScheduleSendText = ReadBool("scheduled_summary", "send_text", true);
        ScheduleSendImage = ReadBool("scheduled_summary", "send_image", true);

        LlmApiKeyEnvironment = ReadString("llm", "api_key_env", "LLM_API_KEY");
        LlmBaseUrl = ReadString("llm", "base_url", string.Empty);
        LlmModel = ReadString("llm", "model", string.Empty);
        LlmTimeoutSeconds = ReadString("llm", "timeout_seconds", "120");
        LlmStreamingEnabled = ReadBool("llm", "stream", true);
        LlmStreamFirstEventTimeoutSeconds = ReadString("llm", "stream_first_event_timeout_seconds", "30");
        LlmStreamIdleTimeoutSeconds = ReadString("llm", "stream_idle_timeout_seconds", "30");
        LlmMaxOutputTokens = ReadString("llm", "max_output_tokens", "2000");
        LlmChunkConcurrency = ReadString("llm", "max_concurrent_chunk_requests", "4");
        ImageGenerationEnabled = ReadBool("image_gen", "enabled", true);
        ImageApiKeyEnvironment = ReadString("image_gen", "api_key_env", "IMAGE_API_KEY");
        ImageBaseUrl = ReadString("image_gen", "base_url", string.Empty);
        ImageModel = ReadString("image_gen", "model", string.Empty);
        ImageTimeoutSeconds = ReadString("image_gen", "timeout_seconds", "300");
        ImageCaptionEnabled = ReadBool("image_caption", "enabled", false);
        VideoCaptionEnabled = ReadBool("video_caption", "enabled", false);
        VoiceTranscriptionEnabled = ReadBool("voice_transcription", "enabled", false);
    }

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

    private async Task StartMaintenanceOperationAsync(string method, string title, object? parameters = null)
    {
        if (_client is null) return;
        if (IsMaintenanceOperationRunning)
        {
            Notice = "已有维护操作正在运行，请等待完成。";
            return;
        }

        IsMaintenanceOperationRunning = true;
        MaintenanceStatus = $"正在请求管理员权限：{title}…";
        MaintenanceOutput = $"[{DateTime.Now:HH:mm:ss}] 已创建后台任务，等待管理员权限确认。\n";
        MaintenanceDialogRequested?.Invoke(title);
        try
        {
            var reply = await _client.CallAsync(method, parameters, _lifetime.Token);
            reply.ThrowIfError();
            Notice = $"{title}已在后台启动";
        }
        catch (Exception error)
        {
            IsMaintenanceOperationRunning = false;
            MaintenanceStatus = $"{title}启动失败：{error.Message}";
            AppendMaintenanceOutput($"[错误] {error.Message}\n");
            Notice = MaintenanceStatus;
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

    private async Task SubscribeOperationsAsync(CancellationToken cancellationToken)
    {
        if (_client is null) return;
        try
        {
            await _client.SubscribeAsync("operation.subscribe", eventData =>
            {
                var eventName = eventData.GetProperty("event").GetString() ?? string.Empty;
                var data = eventData.GetProperty("data").Clone();
                _dispatcher.TryEnqueue(() => ApplyMaintenanceEvent(eventName, data));
                return Task.CompletedTask;
            }, cancellationToken);
        }
        catch (Exception error) when (!cancellationToken.IsCancellationRequested)
        {
            _dispatcher.TryEnqueue(() => AppendMaintenanceOutput($"[连接错误] 维护任务订阅已断开：{error.Message}\n"));
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

    private void ApplyMaintenanceEvent(string eventName, JsonElement data)
    {
        var operation = data.TryGetProperty("operation", out var operationElement)
            ? operationElement.GetString() ?? "维护操作"
            : "维护操作";
        var message = data.TryGetProperty("message", out var messageElement)
            ? messageElement.GetString() ?? string.Empty
            : string.Empty;
        if (!string.IsNullOrWhiteSpace(message))
        {
            var source = data.TryGetProperty("source", out var sourceElement)
                ? sourceElement.GetString()
                : null;
            var prefix = source is "stdout" or "stderr" ? $"[{source}] " : string.Empty;
            AppendMaintenanceOutput($"{prefix}{message}\n");
        }

        if (eventName == "operation.completed")
        {
            var success = data.TryGetProperty("success", out var successElement) && successElement.ValueKind == JsonValueKind.True;
            IsMaintenanceOperationRunning = false;
            MaintenanceStatus = success
                ? $"{OperationDisplayName(operation)}已成功完成。"
                : $"{OperationDisplayName(operation)}失败：{message}";
            Notice = MaintenanceStatus;
            if (success)
            {
                _ = CheckRuntimeDependenciesAsync();
            }
        }
        else if (!string.IsNullOrWhiteSpace(message))
        {
            MaintenanceStatus = message;
        }
    }

    private void AppendMaintenanceOutput(string text)
    {
        const int maxChars = 256 * 1024;
        var combined = MaintenanceOutput + text;
        MaintenanceOutput = combined.Length > maxChars ? combined[^maxChars..] : combined;
    }

    private static string OperationDisplayName(string operation) => operation switch
    {
        "runtime.install" => "安装微信运行环境",
        "wxdb.init" => "运行 wxdb init",
        "wxdb.update" => "更新 wxdb",
        "pip.update" => "更新 Python 依赖",
        "application.update" => "更新 SummaryAgent4GroupChat",
        _ => operation,
    };

    private static string ReadUpdateValue(JsonElement entry, string property, string fallback) =>
        entry.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()!
            : fallback;

    private static string UpdateStatusText(string status) => status switch
    {
        "update_available" => "可更新",
        "up_to_date" => "已是最新",
        "installed" => "已安装",
        "not_managed" => "无需检查",
        "not_detected" => "未检测到",
        "available_unknown_current" => "已发现版本",
        "unavailable" => "暂不可用",
        _ => "未知",
    };

    private JsonElement Section(string name) =>
        _parsedConfig.ValueKind == JsonValueKind.Object && _parsedConfig.TryGetProperty(name, out var value)
            ? value
            : default;

    private static string ReadString(JsonElement section, string key, string fallback)
    {
        if (section.ValueKind != JsonValueKind.Object || !section.TryGetProperty(key, out var value))
        {
            return fallback;
        }
        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString() ?? fallback,
            JsonValueKind.Number => value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => fallback,
        };
    }

    private string ReadString(string section, string key, string fallback) =>
        ReadString(Section(section), key, fallback);

    private bool ReadBool(string section, string key, bool fallback)
    {
        var container = Section(section);
        if (container.ValueKind != JsonValueKind.Object || !container.TryGetProperty(key, out var value))
        {
            return fallback;
        }
        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) && parsed,
            _ => fallback,
        };
    }

    private List<string> ReadListValues(string section, string key)
    {
        var container = Section(section);
        if (container.ValueKind != JsonValueKind.Object || !container.TryGetProperty(key, out var value))
        {
            return [];
        }
        return value.ValueKind switch
        {
            JsonValueKind.Array => value.EnumerateArray()
                .Where(item => item.ValueKind == JsonValueKind.String)
                .Select(item => item.GetString() ?? string.Empty)
                .ToList(),
            JsonValueKind.String => [value.GetString() ?? string.Empty],
            _ => [],
        };
    }

    private string ReadList(string section, string key) =>
        string.Join(", ", ReadListValues(section, key));

    private List<string> ReadDisabledImageRooms()
    {
        var rooms = Section("room_capabilities");
        if (rooms.ValueKind != JsonValueKind.Object)
        {
            return [];
        }
        return rooms.EnumerateObject()
            .Where(room => room.Value.ValueKind == JsonValueKind.Object
                && room.Value.TryGetProperty("image_summary_enabled", out var enabled)
                && enabled.ValueKind == JsonValueKind.False)
            .Select(room => room.Name)
            .ToList();
    }

    private HashSet<string> KnownRooms()
    {
        var rooms = ReadListValues("wx4py", "groups")
            .Concat(ReadListValues("discord", "channels"))
            .ToHashSet(StringComparer.Ordinal);
        return rooms;
    }

    private void AddOperation(
        List<Dictionary<string, object?>> operations,
        string[] section,
        string key,
        object? value) =>
        operations.Add(new Dictionary<string, object?>
        {
            ["section"] = section,
            ["key"] = key,
            ["value"] = value,
        });

    /// Builds config.patch operations by diffing form fields against the loaded
    /// configuration, so unrelated manual edits in the raw editor survive.
    private List<Dictionary<string, object?>> BuildFormOperations()
    {
        var operations = new List<Dictionary<string, object?>>();

        AddIfChanged(operations, "platform", "kind", PlatformKind);
        AddListIfChanged(operations, "wx4py", "groups", WeChatGroups);
        AddListIfChanged(operations, "discord", "channels", DiscordChannels);
        AddIfChanged(operations, "wxdb", "executable", WxdbExecutable);
        AddOptionalIfChanged(operations, "wxdb", "cache_dir", WxdbCacheDirectory);
        AddNumberIfChanged(operations, "history", "max_messages", HistoryPageSize, 10000);

        AddListIfChanged(operations, "listen", "triggers", TriggerCommands);
        AddListIfChanged(operations, "listen", "whitelist_rooms", WhitelistRooms);
        AddBoolIfChanged(operations, "listen", "ignore_self", IgnoreSelf);
        AddNumberIfChanged(operations, "rate_limit", "successful_request_cooldown_seconds", RequestCooldownSeconds, 300);
        AddNumberIfChanged(operations, "rate_limit", "successful_image_cooldown_seconds", ImageCooldownSeconds, 0);
        AddBoolIfChanged(operations, "manual_summary", "image_by_default", ManualImagesByDefault);

        AddBoolIfChanged(operations, "scheduled_summary", "enabled", ScheduleEnabled);
        var scheduleParts = ScheduleTime.Split(':', StringSplitOptions.TrimEntries);
        AddNumberIfChanged(operations, "scheduled_summary", "local_hour", scheduleParts.ElementAtOrDefault(0), 22);
        AddNumberIfChanged(operations, "scheduled_summary", "local_minute", scheduleParts.ElementAtOrDefault(1), 0);
        AddNumberIfChanged(operations, "scheduled_summary", "range_hours", ScheduleRangeHours, 24);
        AddListIfChanged(operations, "scheduled_summary", "rooms", ScheduleRooms);
        AddBoolIfChanged(operations, "scheduled_summary", "send_text", ScheduleSendText);
        AddBoolIfChanged(operations, "scheduled_summary", "send_image", ScheduleSendImage);

        AddIfChanged(operations, "llm", "api_key_env", LlmApiKeyEnvironment);
        AddOptionalIfChanged(operations, "llm", "base_url", LlmBaseUrl);
        AddOptionalIfChanged(operations, "llm", "model", LlmModel);
        AddNumberIfChanged(operations, "llm", "timeout_seconds", LlmTimeoutSeconds, 120);
        AddBoolIfChanged(operations, "llm", "stream", LlmStreamingEnabled);
        AddNumberIfChanged(operations, "llm", "stream_first_event_timeout_seconds", LlmStreamFirstEventTimeoutSeconds, 30);
        AddNumberIfChanged(operations, "llm", "stream_idle_timeout_seconds", LlmStreamIdleTimeoutSeconds, 30);
        AddNumberIfChanged(operations, "llm", "max_output_tokens", LlmMaxOutputTokens, 2000);
        AddNumberIfChanged(operations, "llm", "max_concurrent_chunk_requests", LlmChunkConcurrency, 4);
        AddBoolIfChanged(operations, "image_gen", "enabled", ImageGenerationEnabled);
        AddIfChanged(operations, "image_gen", "api_key_env", ImageApiKeyEnvironment);
        AddOptionalIfChanged(operations, "image_gen", "base_url", ImageBaseUrl);
        AddOptionalIfChanged(operations, "image_gen", "model", ImageModel);
        AddNumberIfChanged(operations, "image_gen", "timeout_seconds", ImageTimeoutSeconds, 300);
        AddBoolIfChanged(operations, "image_caption", "enabled", ImageCaptionEnabled);
        AddBoolIfChanged(operations, "video_caption", "enabled", VideoCaptionEnabled);
        AddBoolIfChanged(operations, "voice_transcription", "enabled", VoiceTranscriptionEnabled);

        AddRoomCapabilityOperations(operations);
        return operations;
    }

    private void AddRoomCapabilityOperations(List<Dictionary<string, object?>> operations)
    {
        var desired = DisabledImageRooms
            .Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Where(room => KnownRooms().Contains(room))
            .ToHashSet(StringComparer.Ordinal);
        var existing = ReadDisabledImageRooms().ToHashSet(StringComparer.Ordinal);

        foreach (var room in existing.Where(room => !desired.Contains(room)))
        {
            AddOperation(operations, ["room_capabilities"], room, null);
        }
        foreach (var room in desired.Where(room => !existing.Contains(room)))
        {
            AddOperation(operations, ["room_capabilities"], room, new Dictionary<string, object?>
            {
                ["image_summary_enabled"] = false,
            });
        }
    }

    private void AddIfChanged(List<Dictionary<string, object?>> operations, string section, string key, string current)
    {
        var original = ReadString(section, key, string.Empty);
        if (current != original)
        {
            AddOperation(operations, [section], key, current);
        }
    }

    private void AddOptionalIfChanged(List<Dictionary<string, object?>> operations, string section, string key, string current)
    {
        var original = ReadString(section, key, string.Empty);
        if (string.IsNullOrWhiteSpace(current))
        {
            if (!string.IsNullOrWhiteSpace(original))
            {
                // Cleared in the form: remove the key entirely.
                AddOperation(operations, [section], key, null);
            }
            return;
        }
        if (current != original)
        {
            AddOperation(operations, [section], key, current.Trim());
        }
    }

    private void AddBoolIfChanged(List<Dictionary<string, object?>> operations, string section, string key, bool current)
    {
        if (current != ReadBool(section, key, current))
        {
            AddOperation(operations, [section], key, current);
        }
    }

    private void AddNumberIfChanged(List<Dictionary<string, object?>> operations, string section, string key, string? current, int fallback)
    {
        var parsed = ParseInt(current, fallback);
        if (parsed != ParseInt(ReadString(section, key, fallback.ToString()), fallback))
        {
            AddOperation(operations, [section], key, parsed);
        }
    }

    private void AddListIfChanged(List<Dictionary<string, object?>> operations, string section, string key, string current)
    {
        var values = current.Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var original = ReadListValues(section, key);
        var changed = values.Length != original.Count
            || !values.Zip(original, (left, right) => string.Equals(left, right, StringComparison.Ordinal)).All(equal => equal);
        if (changed)
        {
            AddOperation(operations, [section], key, values);
        }
    }

    private static int ParseInt(string? value, int fallback) =>
        int.TryParse(value, out var parsed) && parsed >= 0 ? parsed : fallback;
}
