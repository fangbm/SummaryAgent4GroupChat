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
    [ObservableProperty] private string _taskCenterStatus = "正在加载任务…";
    [ObservableProperty] private string _selectedTaskDetails = "选择任务后可查看脱敏来源索引。";
    [ObservableProperty] private string _providerHealthStatus = "尚未检测供应商健康状态。";
    public ObservableCollection<UpdateCheckItem> UpdateItems { get; } = [];
    public ObservableCollection<TaskCenterItem> Tasks { get; } = [];
    public ObservableCollection<OutboxItem> OutboxItems { get; } = [];
    public ObservableCollection<SourceReferenceItem> SelectedTaskSources { get; } = [];
    public ObservableCollection<ProviderHealthItem> ProviderHealthItems { get; } = [];
    public event Action<string>? MaintenanceDialogRequested;

    [ObservableProperty] private string _platformKind = "wx";
    [ObservableProperty] private string _weChatGroups = string.Empty;
    [ObservableProperty] private string _discordChannels = string.Empty;
    // Discord bot token is write-only for the same reason as model API keys.
    [ObservableProperty] private string _discordTokenInput = string.Empty;
    [ObservableProperty] private string _discordLongTextDelivery = "chunks";
    [ObservableProperty] private string _discordLongTextFileMinChunks = "3";
    [ObservableProperty] private string _wxdbExecutable = "wxdb";
    [ObservableProperty] private string _wxdbCacheDirectory = string.Empty;
    [ObservableProperty] private string _historyPageSize = "10000";
    [ObservableProperty] private string _disabledImageRooms = string.Empty;
    [ObservableProperty] private string _policyTemplatesJson = "{}";
    [ObservableProperty] private string _reportGroupsJson = "{}";
    [ObservableProperty] private string _providerFallbacksJson = "{}";

    [ObservableProperty] private string _triggerCommands = "/总结, #总结";
    [ObservableProperty] private string _whitelistRooms = string.Empty;
    [ObservableProperty] private bool _requireAllowedUsers;
    [ObservableProperty] private string _allowedUsers = string.Empty;
    [ObservableProperty] private bool _ignoreSelf = true;
    [ObservableProperty] private string _requestCooldownSeconds = "300";
    [ObservableProperty] private string _imageCooldownSeconds = "0";
    [ObservableProperty] private bool _manualImagesByDefault;
    [ObservableProperty] private string _summaryDetail = "standard";
    [ObservableProperty] private bool _budgetEnabled;
    [ObservableProperty] private string _dailySummaryLimit = "0";
    [ObservableProperty] private string _dailyImageLimit = "0";
    [ObservableProperty] private string _dailyMediaLimit = "0";

    [ObservableProperty] private bool _scheduleEnabled = true;
    [ObservableProperty] private string _scheduleTime = "22:00";
    [ObservableProperty] private string _scheduleRangeHours = "24";
    [ObservableProperty] private string _scheduleRooms = string.Empty;
    [ObservableProperty] private bool _scheduleSendText = true;
    [ObservableProperty] private bool _scheduleSendImage = true;

    // Keys are deliberately write-only: config.read redacts them before the
    // form sees anything, and an empty editor keeps the existing value.
    [ObservableProperty] private string _llmApiKeysInput = string.Empty;
    [ObservableProperty] private string _llmBaseUrl = string.Empty;
    [ObservableProperty] private string _llmModel = string.Empty;
    [ObservableProperty] private string _llmTimeoutSeconds = "120";
    [ObservableProperty] private bool _llmStreamingEnabled = true;
    [ObservableProperty] private string _llmStreamFirstEventTimeoutSeconds = "30";
    [ObservableProperty] private string _llmStreamIdleTimeoutSeconds = "30";
    [ObservableProperty] private string _llmMaxOutputTokens = "2000";
    [ObservableProperty] private string _llmChunkConcurrency = "4";
    [ObservableProperty] private bool _imageGenerationEnabled = true;
    [ObservableProperty] private string _imageProvider = "openai";
    [ObservableProperty] private string _imageApiKeysInput = string.Empty;
    [ObservableProperty] private string _imageBaseUrl = string.Empty;
    [ObservableProperty] private string _imageModel = string.Empty;
    [ObservableProperty] private string _imageSize = "2:3";
    [ObservableProperty] private string _imageResolution = "1k";
    [ObservableProperty] private string _imageQuality = string.Empty;
    [ObservableProperty] private bool _imageOfficialFallback;
    [ObservableProperty] private string _imagePollInitialDelaySeconds = "10";
    [ObservableProperty] private string _imagePollIntervalSeconds = "5";
    [ObservableProperty] private string _imageTimeoutSeconds = "300";
    [ObservableProperty] private string _imageRetryAttempts = "5";
    [ObservableProperty] private string _imageMaxConcurrentPerKey = "0";
    [ObservableProperty] private string _imagePromptTemplate = string.Empty;
    [ObservableProperty] private string _imageRequestBodyOverridesJson = "{}";
    [ObservableProperty] private bool _novelAiEnabled;
    [ObservableProperty] private string _novelAiApiKeysInput = string.Empty;
    [ObservableProperty] private string _novelAiBaseUrl = "https://image.novelai.net";
    [ObservableProperty] private string _novelAiModel = "nai-diffusion-5-full";
    [ObservableProperty] private string _novelAiSize = "2:3";
    [ObservableProperty] private string _novelAiTimeoutSeconds = "300";
    [ObservableProperty] private string _novelAiRetryAttempts = "5";
    [ObservableProperty] private string _novelAiMaxConcurrentPerKey = "0";
    [ObservableProperty] private string _novelAiRequestBodyOverridesJson = "{}";
    [ObservableProperty] private string _imagePipelineMaxConcurrentRequests = "0";
    [ObservableProperty] private string _imageSummaryTotalTimeoutSeconds = "240";
    [ObservableProperty] private string _imagePromptTotalTimeoutSeconds = "120";
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
            await RefreshTaskCenterAsync();
            await RefreshProviderHealthAsync();
            _ = SubscribeOutputAsync(_lifetime.Token);
            _ = SubscribeOperationsAsync(_lifetime.Token);
            _ = PollLogsAsync(_lifetime.Token);
            _ = PollTaskCenterAsync(_lifetime.Token);
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
        try
        {
            var operations = BuildFormOperations();
            if (operations.Count == 0)
            {
                Notice = "配置无变更";
                return;
            }
            await PatchConfigAsync(operations);
        }
        catch (Exception error)
        {
            ValidationMessage = $"配置格式无效：{error.Message}";
        }
    }

    public async Task SavePoliciesAndReportsAsync()
    {
        try
        {
            var operations = new List<Dictionary<string, object?>>();
            AddJsonTableOperations(operations, "policy_templates", PolicyTemplatesJson);
            AddJsonTableOperations(operations, "report_groups", ReportGroupsJson);
            await PatchConfigAsync(operations);
        }
        catch (Exception error)
        {
            ValidationMessage = $"策略或群组配置格式无效：{error.Message}";
        }
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
        DiscordTokenInput = string.Empty;
        DiscordLongTextDelivery = ReadString("discord", "long_text_delivery", "chunks");
        DiscordLongTextFileMinChunks = ReadString("discord", "long_text_file_min_chunks", "3");
        WxdbExecutable = ReadString("wxdb", "executable", "wxdb");
        WxdbCacheDirectory = ReadString("wxdb", "cache_dir", string.Empty);
        HistoryPageSize = ReadString("history", "max_messages", "10000");
        DisabledImageRooms = string.Join(", ", ReadDisabledImageRooms());

        TriggerCommands = ReadList("listen", "triggers");
        WhitelistRooms = ReadList("listen", "whitelist_rooms");
        RequireAllowedUsers = ReadBool("listen", "require_allowed_users", false);
        AllowedUsers = ReadList("listen", "allowed_users");
        IgnoreSelf = ReadBool("listen", "ignore_self", true);
        RequestCooldownSeconds = ReadString("rate_limit", "successful_request_cooldown_seconds", "300");
        ImageCooldownSeconds = ReadString("rate_limit", "successful_image_cooldown_seconds", "0");
        ManualImagesByDefault = ReadBool("manual_summary", "image_by_default", false);
        SummaryDetail = ReadString("text_summary", "detail", "standard");
        BudgetEnabled = ReadBool("budget", "enabled", false);
        DailySummaryLimit = ReadString("budget", "daily_summary_limit", "0");
        DailyImageLimit = ReadString("budget", "daily_image_limit", "0");
        DailyMediaLimit = ReadString("budget", "daily_media_limit", "0");

        ScheduleEnabled = ReadBool("scheduled_summary", "enabled", true);
        var hour = ReadString("scheduled_summary", "local_hour", "22");
        var minute = ReadString("scheduled_summary", "local_minute", "0");
        ScheduleTime = $"{ParseInt(hour, 22):00}:{ParseInt(minute, 0):00}";
        ScheduleRangeHours = ReadString("scheduled_summary", "range_hours", "24");
        ScheduleRooms = ReadList("scheduled_summary", "rooms");
        ScheduleSendText = ReadBool("scheduled_summary", "send_text", true);
        ScheduleSendImage = ReadBool("scheduled_summary", "send_image", true);

        LlmApiKeysInput = string.Empty;
        LlmBaseUrl = ReadString("llm", "base_url", string.Empty);
        LlmModel = ReadString("llm", "model", string.Empty);
        LlmTimeoutSeconds = ReadString("llm", "timeout_seconds", "120");
        LlmStreamingEnabled = ReadBool("llm", "stream", true);
        LlmStreamFirstEventTimeoutSeconds = ReadString("llm", "stream_first_event_timeout_seconds", "30");
        LlmStreamIdleTimeoutSeconds = ReadString("llm", "stream_idle_timeout_seconds", "30");
        LlmMaxOutputTokens = ReadString("llm", "max_output_tokens", "2000");
        LlmChunkConcurrency = ReadString("llm", "max_concurrent_chunk_requests", "4");
        ImageGenerationEnabled = ReadBool("image_gen", "enabled", true);
        ImageProvider = ReadString("image_gen", "provider", "openai");
        ImageApiKeysInput = string.Empty;
        ImageBaseUrl = ReadString("image_gen", "base_url", string.Empty);
        ImageModel = ReadString("image_gen", "model", string.Empty);
        ImageSize = ReadString("image_gen", "size", "2:3");
        ImageResolution = ReadString("image_gen", "resolution", string.Empty);
        ImageQuality = ReadString("image_gen", "quality", string.Empty);
        ImageOfficialFallback = ReadBool("image_gen", "official_fallback", false);
        ImagePollInitialDelaySeconds = ReadString("image_gen", "poll_initial_delay_seconds", "10");
        ImagePollIntervalSeconds = ReadString("image_gen", "poll_interval_seconds", "5");
        ImageTimeoutSeconds = ReadString("image_gen", "timeout_seconds", "300");
        ImageRetryAttempts = ReadString("image_gen", "retry_5xx_attempts", "5");
        ImageMaxConcurrentPerKey = ReadString("image_gen", "max_concurrent_per_key", "0");
        ImagePromptTemplate = ReadString("image_gen", "prompt_template", string.Empty);
        ImageRequestBodyOverridesJson = SerializeConfigField("image_gen", "request_body_overrides");
        NovelAiEnabled = ReadBool("novelai", "enabled", false);
        NovelAiApiKeysInput = string.Empty;
        NovelAiBaseUrl = ReadString("novelai", "base_url", "https://image.novelai.net");
        NovelAiModel = ReadString("novelai", "model", "nai-diffusion-5-full");
        NovelAiSize = ReadString("novelai", "size", "2:3");
        NovelAiTimeoutSeconds = ReadString("novelai", "timeout_seconds", "300");
        NovelAiRetryAttempts = ReadString("novelai", "retry_5xx_attempts", "5");
        NovelAiMaxConcurrentPerKey = ReadString("novelai", "max_concurrent_per_key", "0");
        NovelAiRequestBodyOverridesJson = SerializeConfigField("novelai", "request_body_overrides");
        ImagePipelineMaxConcurrentRequests = ReadString("image_pipeline", "max_concurrent_requests", "0");
        ImageSummaryTotalTimeoutSeconds = ReadString("image_pipeline", "summary_total_timeout_seconds", "240");
        ImagePromptTotalTimeoutSeconds = ReadString("image_pipeline", "prompt_total_timeout_seconds", "120");
        ImageCaptionEnabled = ReadBool("image_caption", "enabled", false);
        VideoCaptionEnabled = ReadBool("video_caption", "enabled", false);
        VoiceTranscriptionEnabled = ReadBool("voice_transcription", "enabled", false);
        PolicyTemplatesJson = SerializeConfigSection("policy_templates");
        ReportGroupsJson = SerializeConfigSection("report_groups");
        ProviderFallbacksJson = SerializeProviderFallbacks();
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

    private static string ReadJson(JsonElement value, string property) =>
        value.TryGetProperty(property, out var field) && field.ValueKind != JsonValueKind.Null
            ? field.ToString()
            : string.Empty;

    private static string? ReadOptionalJson(JsonElement value, string property) =>
        value.TryGetProperty(property, out var field) && field.ValueKind != JsonValueKind.Null
            ? field.ToString()
            : null;

    private static ulong ReadUInt64(JsonElement value, string property) =>
        value.TryGetProperty(property, out var field) && field.TryGetUInt64(out var number) ? number : 0;

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

    private string SerializeConfigSection(string section)
    {
        var value = Section(section);
        return value.ValueKind == JsonValueKind.Object
            ? JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true })
            : "{}";
    }

    private string SerializeConfigField(string section, string key)
    {
        var value = Section(section);
        return value.ValueKind == JsonValueKind.Object && value.TryGetProperty(key, out var field)
            ? JsonSerializer.Serialize(field, new JsonSerializerOptions { WriteIndented = true })
            : "{}";
    }

    private string SerializeProviderFallbacks()
    {
        var values = new Dictionary<string, JsonElement>();
        foreach (var section in ProviderFallbackSections)
        {
            var value = Section(section);
            if (value.ValueKind == JsonValueKind.Object && value.TryGetProperty("fallbacks", out var fallbacks))
            {
                values[section] = fallbacks.Clone();
            }
        }
        return JsonSerializer.Serialize(values, new JsonSerializerOptions { WriteIndented = true });
    }

    private static readonly string[] ProviderFallbackSections = [
        "llm", "image_gen", "image_caption", "video_caption", "voice_transcription"
    ];

    private static void AddProviderFallbackOperations(
        List<Dictionary<string, object?>> operations,
        string text)
    {
        using var document = JsonDocument.Parse(string.IsNullOrWhiteSpace(text) ? "{}" : text);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("备用供应商必须是 JSON 对象。");
        }
        foreach (var property in document.RootElement.EnumerateObject())
        {
            if (!ProviderFallbackSections.Contains(property.Name, StringComparer.Ordinal))
            {
                throw new InvalidOperationException($"不支持的能力：{property.Name}");
            }
            if (property.Value.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidOperationException($"{property.Name} 的备用供应商必须是 JSON 数组。");
            }
            operations.Add(new Dictionary<string, object?>
            {
                ["section"] = new[] { property.Name },
                ["key"] = "fallbacks",
                ["value"] = JsonSerializer.Deserialize<object>(property.Value.GetRawText()),
            });
        }
    }

    private static void AddJsonTableOperations(
        List<Dictionary<string, object?>> operations,
        string section,
        string text)
    {
        using var document = JsonDocument.Parse(string.IsNullOrWhiteSpace(text) ? "{}" : text);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"{section} 必须是 JSON 对象。");
        }
        foreach (var property in document.RootElement.EnumerateObject())
        {
            operations.Add(new Dictionary<string, object?>
            {
                ["section"] = new[] { section },
                ["key"] = property.Name,
                ["value"] = JsonSerializer.Deserialize<object>(property.Value.GetRawText()),
            });
        }
    }

    private void AddJsonObjectOperation(
        List<Dictionary<string, object?>> operations,
        string section,
        string key,
        string text)
    {
        using var document = JsonDocument.Parse(string.IsNullOrWhiteSpace(text) ? "{}" : text);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"{key} 必须是 JSON 对象。");
        }
        AddOperation(operations, [section], key, JsonSerializer.Deserialize<object>(document.RootElement.GetRawText()));
    }

    private async Task PollTaskCenterAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await RefreshTaskCenterAsync();
            await RefreshProviderHealthAsync();
            await Task.Delay(TimeSpan.FromSeconds(4), cancellationToken);
        }
    }

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
        AddSecretIfEntered(operations, "discord", "token", DiscordTokenInput);
        AddIfChanged(operations, "discord", "long_text_delivery", DiscordLongTextDelivery);
        AddNumberIfChanged(operations, "discord", "long_text_file_min_chunks", DiscordLongTextFileMinChunks, 3);
        AddIfChanged(operations, "wxdb", "executable", WxdbExecutable);
        AddOptionalIfChanged(operations, "wxdb", "cache_dir", WxdbCacheDirectory);
        AddNumberIfChanged(operations, "history", "max_messages", HistoryPageSize, 10000);

        AddListIfChanged(operations, "listen", "triggers", TriggerCommands);
        AddListIfChanged(operations, "listen", "whitelist_rooms", WhitelistRooms);
        AddBoolIfChanged(operations, "listen", "require_allowed_users", RequireAllowedUsers);
        AddListIfChanged(operations, "listen", "allowed_users", AllowedUsers);
        AddBoolIfChanged(operations, "listen", "ignore_self", IgnoreSelf);
        AddNumberIfChanged(operations, "rate_limit", "successful_request_cooldown_seconds", RequestCooldownSeconds, 300);
        AddNumberIfChanged(operations, "rate_limit", "successful_image_cooldown_seconds", ImageCooldownSeconds, 0);
        AddBoolIfChanged(operations, "manual_summary", "image_by_default", ManualImagesByDefault);
        AddIfChanged(operations, "text_summary", "detail", SummaryDetail);
        AddBoolIfChanged(operations, "budget", "enabled", BudgetEnabled);
        AddNumberIfChanged(operations, "budget", "daily_summary_limit", DailySummaryLimit, 0);
        AddNumberIfChanged(operations, "budget", "daily_image_limit", DailyImageLimit, 0);
        AddNumberIfChanged(operations, "budget", "daily_media_limit", DailyMediaLimit, 0);

        AddBoolIfChanged(operations, "scheduled_summary", "enabled", ScheduleEnabled);
        var scheduleParts = ScheduleTime.Split(':', StringSplitOptions.TrimEntries);
        AddNumberIfChanged(operations, "scheduled_summary", "local_hour", scheduleParts.ElementAtOrDefault(0), 22);
        AddNumberIfChanged(operations, "scheduled_summary", "local_minute", scheduleParts.ElementAtOrDefault(1), 0);
        AddNumberIfChanged(operations, "scheduled_summary", "range_hours", ScheduleRangeHours, 24);
        AddListIfChanged(operations, "scheduled_summary", "rooms", ScheduleRooms);
        AddBoolIfChanged(operations, "scheduled_summary", "send_text", ScheduleSendText);
        AddBoolIfChanged(operations, "scheduled_summary", "send_image", ScheduleSendImage);

        AddSecretKeysIfEntered(operations, "llm", LlmApiKeysInput);
        AddOptionalIfChanged(operations, "llm", "base_url", LlmBaseUrl);
        AddOptionalIfChanged(operations, "llm", "model", LlmModel);
        AddNumberIfChanged(operations, "llm", "timeout_seconds", LlmTimeoutSeconds, 120);
        AddBoolIfChanged(operations, "llm", "stream", LlmStreamingEnabled);
        AddNumberIfChanged(operations, "llm", "stream_first_event_timeout_seconds", LlmStreamFirstEventTimeoutSeconds, 30);
        AddNumberIfChanged(operations, "llm", "stream_idle_timeout_seconds", LlmStreamIdleTimeoutSeconds, 30);
        AddNumberIfChanged(operations, "llm", "max_output_tokens", LlmMaxOutputTokens, 2000);
        AddNumberIfChanged(operations, "llm", "max_concurrent_chunk_requests", LlmChunkConcurrency, 4);
        AddBoolIfChanged(operations, "image_gen", "enabled", ImageGenerationEnabled);
        AddIfChanged(operations, "image_gen", "provider", ImageProvider);
        AddSecretKeysIfEntered(operations, "image_gen", ImageApiKeysInput);
        AddOptionalIfChanged(operations, "image_gen", "base_url", ImageBaseUrl);
        AddOptionalIfChanged(operations, "image_gen", "model", ImageModel);
        AddIfChanged(operations, "image_gen", "size", ImageSize);
        AddOptionalIfChanged(operations, "image_gen", "resolution", ImageResolution);
        AddOptionalIfChanged(operations, "image_gen", "quality", ImageQuality);
        AddBoolIfChanged(operations, "image_gen", "official_fallback", ImageOfficialFallback);
        AddNumberIfChanged(operations, "image_gen", "poll_initial_delay_seconds", ImagePollInitialDelaySeconds, 10);
        AddNumberIfChanged(operations, "image_gen", "poll_interval_seconds", ImagePollIntervalSeconds, 5);
        AddNumberIfChanged(operations, "image_gen", "timeout_seconds", ImageTimeoutSeconds, 300);
        AddNumberIfChanged(operations, "image_gen", "retry_5xx_attempts", ImageRetryAttempts, 5);
        AddNumberIfChanged(operations, "image_gen", "max_concurrent_per_key", ImageMaxConcurrentPerKey, 0);
        AddOptionalIfChanged(operations, "image_gen", "prompt_template", ImagePromptTemplate);
        AddJsonObjectOperation(operations, "image_gen", "request_body_overrides", ImageRequestBodyOverridesJson);
        AddBoolIfChanged(operations, "novelai", "enabled", NovelAiEnabled);
        AddSecretKeysIfEntered(operations, "novelai", NovelAiApiKeysInput);
        AddOptionalIfChanged(operations, "novelai", "base_url", NovelAiBaseUrl);
        AddOptionalIfChanged(operations, "novelai", "model", NovelAiModel);
        AddIfChanged(operations, "novelai", "size", NovelAiSize);
        AddNumberIfChanged(operations, "novelai", "timeout_seconds", NovelAiTimeoutSeconds, 300);
        AddNumberIfChanged(operations, "novelai", "retry_5xx_attempts", NovelAiRetryAttempts, 5);
        AddNumberIfChanged(operations, "novelai", "max_concurrent_per_key", NovelAiMaxConcurrentPerKey, 0);
        AddJsonObjectOperation(operations, "novelai", "request_body_overrides", NovelAiRequestBodyOverridesJson);
        AddNumberIfChanged(operations, "image_pipeline", "max_concurrent_requests", ImagePipelineMaxConcurrentRequests, 0);
        AddNumberIfChanged(operations, "image_pipeline", "summary_total_timeout_seconds", ImageSummaryTotalTimeoutSeconds, 240);
        AddNumberIfChanged(operations, "image_pipeline", "prompt_total_timeout_seconds", ImagePromptTotalTimeoutSeconds, 120);
        AddBoolIfChanged(operations, "image_caption", "enabled", ImageCaptionEnabled);
        AddBoolIfChanged(operations, "video_caption", "enabled", VideoCaptionEnabled);
        AddBoolIfChanged(operations, "voice_transcription", "enabled", VoiceTranscriptionEnabled);
        AddProviderFallbackOperations(operations, ProviderFallbacksJson);

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

    public async Task RefreshTaskCenterAsync()
    {
        if (_client is null) return;
        try
        {
            var tasksReply = await _client.CallAsync("tasks.list", new { limit = 200 }, _lifetime.Token);
            tasksReply.ThrowIfError();
            var outboxReply = await _client.CallAsync("outbox.list", new { limit = 200 }, _lifetime.Token);
            outboxReply.ThrowIfError();
            Tasks.Clear();
            foreach (var task in tasksReply.Result!.Value.GetProperty("tasks").EnumerateArray())
            {
                Tasks.Add(new TaskCenterItem(
                    ReadJson(task, "id"), ReadJson(task, "room_id"), ReadJson(task, "source"),
                    ReadJson(task, "state"), ReadJson(task, "stage"), ReadJson(task, "created_at"),
                    ReadOptionalJson(task, "summary"), ReadOptionalJson(task, "error"),
                    ReadUInt64(task, "message_count"), ReadUInt64(task, "media_count")));
            }
            OutboxItems.Clear();
            foreach (var delivery in outboxReply.Result!.Value.GetProperty("deliveries").EnumerateArray())
            {
                OutboxItems.Add(new OutboxItem(
                    ReadJson(delivery, "id"), ReadJson(delivery, "room_id"), ReadJson(delivery, "kind"),
                    ReadJson(delivery, "state"), (uint)ReadUInt64(delivery, "attempts"),
                    ReadJson(delivery, "next_attempt_at"), ReadOptionalJson(delivery, "error")));
            }
            var active = Tasks.Count(item => item.State is "queued" or "running");
            var pending = OutboxItems.Count(item => item.State is "pending" or "sending");
            TaskCenterStatus = $"任务 {Tasks.Count} 项，进行中 {active} 项，待投递 {pending} 项。";
        }
        catch (Exception error)
        {
            TaskCenterStatus = $"读取任务中心失败：{error.Message}";
        }
    }

    public async Task SelectTaskAsync(string taskId)
    {
        if (_client is null || string.IsNullOrWhiteSpace(taskId)) return;
        try
        {
            var reply = await _client.CallAsync("tasks.get", new { id = taskId }, _lifetime.Token);
            reply.ThrowIfError();
            var result = reply.Result!.Value;
            var task = result.GetProperty("task");
            SelectedTaskSources.Clear();
            foreach (var source in result.GetProperty("sources").EnumerateArray())
            {
                SelectedTaskSources.Add(new SourceReferenceItem(
                    (uint)ReadUInt64(source, "point_index"), ReadJson(source, "source_id"),
                    ReadJson(source, "occurred_at"), ReadJson(source, "sender_label"),
                    (uint)ReadUInt64(source, "message_index")));
            }
            SelectedTaskDetails = $"{ReadJson(task, "room_id")} · {ReadJson(task, "stage")} · 来源索引 {SelectedTaskSources.Count} 条。";
        }
        catch (Exception error)
        {
            SelectedTaskDetails = $"读取任务详情失败：{error.Message}";
        }
    }

    public async Task CancelTaskAsync(string taskId)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("tasks.cancel", new { id = taskId }, _lifetime.Token);
            reply.ThrowIfError();
            await RefreshTaskCenterAsync();
        }
        catch (Exception error) { TaskCenterStatus = $"取消失败：{error.Message}"; }
    }

    public async Task RetryTaskAsync(string taskId)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("tasks.retry", new { id = taskId }, _lifetime.Token);
            reply.ThrowIfError();
            await RefreshTaskCenterAsync();
        }
        catch (Exception error) { TaskCenterStatus = $"重试失败：{error.Message}"; }
    }

    public async Task CreateTaskDiagnosticBundleAsync(string taskId)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("tasks.diagnostic_bundle", new { id = taskId }, _lifetime.Token);
            reply.ThrowIfError();
            var path = ReadJson(reply.Result!.Value, "path");
            TaskCenterStatus = $"已生成任务诊断包：{path}";
        }
        catch (Exception error) { TaskCenterStatus = $"生成诊断包失败：{error.Message}"; }
    }

    public async Task RetryOutboxAsync(string deliveryId)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("outbox.retry", new { id = deliveryId }, _lifetime.Token);
            reply.ThrowIfError();
            await RefreshTaskCenterAsync();
        }
        catch (Exception error) { TaskCenterStatus = $"重新投递失败：{error.Message}"; }
    }

    public async Task ResolveOutboxAsync(string deliveryId)
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("outbox.resolve", new { id = deliveryId, action = "cancel" }, _lifetime.Token);
            reply.ThrowIfError();
            await RefreshTaskCenterAsync();
        }
        catch (Exception error) { TaskCenterStatus = $"确认处理失败：{error.Message}"; }
    }

    public async Task RefreshProviderHealthAsync()
    {
        if (_client is null) return;
        try
        {
            var reply = await _client.CallAsync("providers.health", cancellationToken: _lifetime.Token);
            reply.ThrowIfError();
            ProviderHealthItems.Clear();
            foreach (var provider in reply.Result!.Value.GetProperty("providers").EnumerateArray())
            {
                ProviderHealthItems.Add(new ProviderHealthItem(
                    ReadJson(provider, "capability"), ReadJson(provider, "provider_key"),
                    (uint)ReadUInt64(provider, "consecutive_failures"), ReadOptionalJson(provider, "circuit_open_until"),
                    ReadOptionalJson(provider, "last_error"), ReadJson(provider, "updated_at")));
            }
            ProviderHealthStatus = ProviderHealthItems.Count == 0 ? "当前没有记录到供应商故障。" : $"已记录 {ProviderHealthItems.Count} 个供应商状态。";
        }
        catch (Exception error) { ProviderHealthStatus = $"读取供应商状态失败：{error.Message}"; }
    }

    private void AddSecretKeysIfEntered(List<Dictionary<string, object?>> operations, string section, string current)
    {
        var keys = current.Split([',', '\n', '\r'], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (keys.Length > 0)
        {
            // The key pool already accepts a list and gives it priority over
            // legacy api_key/api_key_env values. Never read or compare secrets.
            AddOperation(operations, [section], "api_keys", keys);
        }
    }

    private void AddSecretIfEntered(List<Dictionary<string, object?>> operations, string section, string key, string current)
    {
        var value = current.Trim();
        if (!string.IsNullOrEmpty(value)) AddOperation(operations, [section], key, value);
    }

    private static int ParseInt(string? value, int fallback) =>
        int.TryParse(value, out var parsed) && parsed >= 0 ? parsed : fallback;
}
