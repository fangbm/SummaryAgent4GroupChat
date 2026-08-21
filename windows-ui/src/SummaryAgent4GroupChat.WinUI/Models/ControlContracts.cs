using System.Text.Json;

namespace SummaryAgent4GroupChat.WinUI.Models;

public sealed record ControlSession(string Pipe, string Token, string ConfigPath, string WorkingDirectory);

public sealed record ControlReply(JsonElement? Result, ControlError? Error)
{
    public void ThrowIfError()
    {
        if (Error is not null)
        {
            throw new InvalidOperationException($"{Error.Message}{(string.IsNullOrWhiteSpace(Error.Detail) ? string.Empty : $"\n{Error.Detail}")}");
        }
    }
}

public sealed record ControlError(string Code, string Message, string? Detail, bool Retryable);

public sealed record EditorPageContext(ViewModels.MainViewModel ViewModel, string Section);

public sealed record UpdateCheckItem(
    string Name,
    string CurrentVersion,
    string LatestVersion,
    string Status,
    string Detail,
    bool UpdateAvailable,
    bool CanInstall,
    string Target,
    string? PackageName);
