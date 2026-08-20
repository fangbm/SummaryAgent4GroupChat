using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using SummaryAgent4GroupChat.WinUI.Models;

namespace SummaryAgent4GroupChat.WinUI.Services;

public sealed class ControlClient(ControlSession session)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ControlSession _session = session;

    public async Task<ControlReply> CallAsync(string method, object? parameters = null, CancellationToken cancellationToken = default)
    {
        using var pipe = await ConnectAsync(cancellationToken);
        using var writer = new StreamWriter(pipe, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
        using var reader = new StreamReader(pipe, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        var request = new { version = 1, id = Guid.NewGuid().ToString("N"), method, token = _session.Token, @params = parameters ?? new { } };
        await writer.WriteLineAsync(JsonSerializer.Serialize(request, JsonOptions));
        var line = await reader.ReadLineAsync(cancellationToken) ?? throw new IOException("控制服务意外断开连接。");
        return ParseReply(line);
    }

    public async Task SubscribeAsync(string method, Func<JsonElement, Task> onEvent, CancellationToken cancellationToken)
    {
        using var pipe = await ConnectAsync(cancellationToken);
        using var writer = new StreamWriter(pipe, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
        using var reader = new StreamReader(pipe, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        var request = new { version = 1, id = Guid.NewGuid().ToString("N"), method, token = _session.Token, @params = new { } };
        await writer.WriteLineAsync(JsonSerializer.Serialize(request, JsonOptions));
        var acknowledgement = await reader.ReadLineAsync(cancellationToken) ?? throw new IOException("控制服务未确认订阅。");
        ParseReply(acknowledgement).ThrowIfError();
        while (!cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (line is null)
            {
                return;
            }
            using var document = JsonDocument.Parse(line);
            if (document.RootElement.TryGetProperty("event", out _))
            {
                await onEvent(document.RootElement.Clone());
            }
        }
    }

    private async Task<NamedPipeClientStream> ConnectAsync(CancellationToken cancellationToken)
    {
        var pipeName = _session.Pipe.Replace("\\\\.\\pipe\\", string.Empty, StringComparison.OrdinalIgnoreCase);
        var pipe = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        await pipe.ConnectAsync(2500, cancellationToken);
        return pipe;
    }

    private static ControlReply ParseReply(string line)
    {
        using var document = JsonDocument.Parse(line);
        var root = document.RootElement;
        JsonElement? result = root.TryGetProperty("result", out var resultElement) ? resultElement.Clone() : null;
        ControlError? error = null;
        if (root.TryGetProperty("error", out var errorElement) && errorElement.ValueKind != JsonValueKind.Null)
        {
            error = new ControlError(
                errorElement.GetProperty("code").GetString() ?? "unknown",
                errorElement.GetProperty("message").GetString() ?? "控制服务错误",
                errorElement.TryGetProperty("detail", out var detail) ? detail.GetString() : null,
                errorElement.TryGetProperty("retryable", out var retryable) && retryable.GetBoolean());
        }
        return new ControlReply(result, error);
    }
}
