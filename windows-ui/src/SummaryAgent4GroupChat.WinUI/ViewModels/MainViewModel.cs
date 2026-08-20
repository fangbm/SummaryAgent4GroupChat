using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
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
        ApplyFormToConfig();
        await SaveConfigTextAsync();
    }

    public async Task SaveRawConfigAsync()
    {
        await SaveConfigTextAsync();
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
    public async Task InstallRuntimeAsync() => await AgentCommandAsync("runtime.install", "已请求管理员权限安装微信运行环境");
    public async Task RunWxdbInitAsync() => await AgentCommandAsync("wxdb.init", "已请求管理员权限运行 wxdb init");
    public async Task OpenPathAsync(string kind) => await AgentCommandAsync("path.open", "已打开路径", new { kind });

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

    private void ApplyFormToConfig()
    {
        WriteString("platform", "kind", PlatformKind);
        WriteList("wx4py", "groups", WeChatGroups);
        WriteList("discord", "channels", DiscordChannels);
        WriteString("wxdb", "executable", WxdbExecutable);
        WriteString("wxdb", "cache_dir", WxdbCacheDirectory);
        WriteNumber("history", "max_messages", HistoryPageSize, 10000);
        WriteDisabledImageRooms(DisabledImageRooms);

        WriteList("listen", "triggers", TriggerCommands);
        WriteList("listen", "whitelist_rooms", WhitelistRooms);
        WriteBool("listen", "ignore_self", IgnoreSelf);
        WriteNumber("rate_limit", "successful_request_cooldown_seconds", RequestCooldownSeconds, 300);
        WriteNumber("rate_limit", "successful_image_cooldown_seconds", ImageCooldownSeconds, 0);
        WriteBool("manual_summary", "image_by_default", ManualImagesByDefault);

        WriteBool("scheduled_summary", "enabled", ScheduleEnabled);
        var scheduleParts = ScheduleTime.Split(':', StringSplitOptions.TrimEntries);
        WriteNumber("scheduled_summary", "local_hour", scheduleParts.ElementAtOrDefault(0), 22);
        WriteNumber("scheduled_summary", "local_minute", scheduleParts.ElementAtOrDefault(1), 0);
        WriteNumber("scheduled_summary", "range_hours", ScheduleRangeHours, 24);
        WriteList("scheduled_summary", "rooms", ScheduleRooms);
        WriteBool("scheduled_summary", "send_text", ScheduleSendText);
        WriteBool("scheduled_summary", "send_image", ScheduleSendImage);

        WriteString("llm", "api_key_env", LlmApiKeyEnvironment);
        WriteOptionalString("llm", "base_url", LlmBaseUrl);
        WriteOptionalString("llm", "model", LlmModel);
        WriteNumber("llm", "timeout_seconds", LlmTimeoutSeconds, 120);
        WriteBool("llm", "stream", LlmStreamingEnabled);
        WriteNumber("llm", "stream_first_event_timeout_seconds", LlmStreamFirstEventTimeoutSeconds, 30);
        WriteNumber("llm", "stream_idle_timeout_seconds", LlmStreamIdleTimeoutSeconds, 30);
        WriteNumber("llm", "max_output_tokens", LlmMaxOutputTokens, 2000);
        WriteNumber("llm", "max_concurrent_chunk_requests", LlmChunkConcurrency, 4);
        WriteBool("image_gen", "enabled", ImageGenerationEnabled);
        WriteString("image_gen", "api_key_env", ImageApiKeyEnvironment);
        WriteOptionalString("image_gen", "base_url", ImageBaseUrl);
        WriteOptionalString("image_gen", "model", ImageModel);
        WriteNumber("image_gen", "timeout_seconds", ImageTimeoutSeconds, 300);
        WriteBool("image_caption", "enabled", ImageCaptionEnabled);
        WriteBool("video_caption", "enabled", VideoCaptionEnabled);
        WriteBool("voice_transcription", "enabled", VoiceTranscriptionEnabled);
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

    private string ReadString(string section, string key, string fallback)
    {
        var value = ReadValue(section, key);
        if (string.IsNullOrWhiteSpace(value)) return fallback;
        var quoted = Regex.Match(value, "^\\s*\\\"(?<value>(?:\\\\.|[^\\\"])*)\\\"\\s*$");
        return quoted.Success ? quoted.Groups["value"].Value.Replace("\\\"", "\"").Replace("\\\\", "\\") : value.Trim();
    }

    private bool ReadBool(string section, string key, bool fallback) =>
        bool.TryParse(ReadValue(section, key), out var value) ? value : fallback;

    private string ReadList(string section, string key)
    {
        var value = ReadValue(section, key);
        return string.Join(", ", Regex.Matches(value, "\\\"(?<value>(?:\\\\.|[^\\\"])*)\\\"")
            .Select(match => match.Groups["value"].Value.Replace("\\\"", "\"").Replace("\\\\", "\\")));
    }

    private IEnumerable<string> ReadDisabledImageRooms()
    {
        var body = ReadSection("room_capabilities");
        return Regex.Matches(body, "(?m)^\\s*\\\"?(?<room>[^\\\"=]+?)\\\"?\\s*=\\s*\\{\\s*image_summary_enabled\\s*=\\s*false\\s*\\}\\s*(?:#.*)?$")
            .Select(match => match.Groups["room"].Value.Trim());
    }

    private string ReadValue(string section, string key)
    {
        var match = Regex.Match(ReadSection(section), $"(?m)^\\s*{Regex.Escape(key)}\\s*=\\s*(?<value>[^\\r\\n#]*)(?:#.*)?$");
        return match.Success ? match.Groups["value"].Value.Trim() : string.Empty;
    }

    private string ReadSection(string section)
    {
        var match = Regex.Match(ConfigText, $"(?ms)^\\[{Regex.Escape(section)}\\]\\s*\\r?\\n(?<body>.*?)(?=^\\[|\\z)");
        return match.Success ? match.Groups["body"].Value : string.Empty;
    }

    private void WriteString(string section, string key, string value) =>
        WriteValue(section, key, $"\\\"{value.Trim().Replace("\\", "\\\\").Replace("\"", "\\\"")}\\\"");

    private void WriteOptionalString(string section, string key, string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }
        WriteString(section, key, value);
    }

    private void WriteBool(string section, string key, bool value) =>
        WriteValue(section, key, value ? "true" : "false");

    private void WriteNumber(string section, string key, string? value, int fallback) =>
        WriteValue(section, key, ParseInt(value, fallback).ToString());

    private void WriteList(string section, string key, string value)
    {
        var values = value.Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(item => $"\\\"{item.Replace("\\", "\\\\").Replace("\"", "\\\"")}\\\"");
        WriteValue(section, key, $"[{string.Join(", ", values)}]");
    }

    private void WriteDisabledImageRooms(string value)
    {
        var desired = value.Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToHashSet(StringComparer.Ordinal);
        var knownRooms = WeChatGroups.Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Concat(DiscordChannels.Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
            .ToHashSet(StringComparer.Ordinal);
        var existing = ReadSection("room_capabilities");
        var lines = existing.Split(["\r\n", "\n"], StringSplitOptions.None)
            .Where(line => !Regex.IsMatch(line, "^\\s*\\\"?[^\\\"=]+?\\\"?\\s*=\\s*\\{\\s*image_summary_enabled\\s*=\\s*false\\s*\\}\\s*(?:#.*)?$"))
            .ToList();
        foreach (var room in desired)
        {
            if (!knownRooms.Contains(room)) continue;
            lines.Add($"\\\"{room.Replace("\\", "\\\\").Replace("\"", "\\\"")}\\\" = {{ image_summary_enabled = false }}");
        }
        ReplaceSection("room_capabilities", string.Join(Environment.NewLine, lines).TrimEnd() + Environment.NewLine);
    }

    private void WriteValue(string section, string key, string value)
    {
        var body = ReadSection(section);
        var line = $"{key} = {value}";
        var expression = new Regex($"(?m)^\\s*{Regex.Escape(key)}\\s*=.*$");
        var updated = expression.IsMatch(body)
            ? expression.Replace(body, line, 1)
            : body.TrimEnd() + Environment.NewLine + line + Environment.NewLine;
        ReplaceSection(section, updated);
    }

    private void ReplaceSection(string section, string body)
    {
        var expression = new Regex($"(?ms)^\\[{Regex.Escape(section)}\\]\\s*\\r?\\n.*?(?=^\\[|\\z)");
        var replacement = $"[{section}]{Environment.NewLine}{body.TrimEnd()}{Environment.NewLine}{Environment.NewLine}";
        if (expression.IsMatch(ConfigText))
        {
            ConfigText = expression.Replace(ConfigText, replacement, 1);
            return;
        }
        ConfigText = ConfigText.TrimEnd() + Environment.NewLine + Environment.NewLine + replacement;
    }

    private static int ParseInt(string? value, int fallback) =>
        int.TryParse(value, out var parsed) && parsed >= 0 ? parsed : fallback;
}
