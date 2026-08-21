using System.Diagnostics;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.Json;
using SummaryAgent4GroupChat.WinUI.Models;

namespace SummaryAgent4GroupChat.WinUI.Services;

public static class ControlBootstrap
{
    public const string TokenEnvironmentVariable = "SUMMARY_AGENT_CONTROL_TOKEN";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static async Task<ControlClient> ConnectAsync(CancellationToken cancellationToken)
    {
        var sessionPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "SummaryAgent4GroupChat", "control-session.json");
        Directory.CreateDirectory(Path.GetDirectoryName(sessionPath)!);
        var configPath = ResolveConfigPath();
        var workingDirectory = ResolveWorkingDirectory(configPath);
        var existing = await ReadSessionAsync(sessionPath, cancellationToken);
        if (existing is not null && File.Exists(existing.ConfigPath) && await IsAliveAsync(existing, cancellationToken))
        {
            return new ControlClient(existing);
        }

        var session = new ControlSession(
            $@"\\.\pipe\SummaryAgent4GroupChat.{Environment.UserName}.{Guid.NewGuid():N}",
            Convert.ToHexString(Guid.NewGuid().ToByteArray()),
            configPath,
            workingDirectory);
        WriteSessionFile(sessionPath, JsonSerializer.Serialize(session, JsonOptions));
        StartControl(session);
        for (var attempt = 0; attempt < 24; attempt++)
        {
            if (await IsAliveAsync(session, cancellationToken))
            {
                return new ControlClient(session);
            }
            await Task.Delay(250, cancellationToken);
        }
        throw new InvalidOperationException("无法启动 Rust 控制服务。请确认安装目录内存在 bin\\wechat-summary-control.exe。");
    }

    private static async Task<bool> IsAliveAsync(ControlSession session, CancellationToken cancellationToken)
    {
        try
        {
            var reply = await new ControlClient(session).CallAsync("status.get", cancellationToken: cancellationToken);
            return reply.Error is null;
        }
        catch (IOException)
        {
            return false;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private static async Task<ControlSession?> ReadSessionAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            return JsonSerializer.Deserialize<ControlSession>(await File.ReadAllTextAsync(path, cancellationToken), JsonOptions);
        }
        catch (IOException)
        {
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static void StartControl(ControlSession session)
    {
        var executable = Path.Combine(AppContext.BaseDirectory, "bin", "wechat-summary-control.exe");
        if (!File.Exists(executable))
        {
            executable = Path.Combine(AppContext.BaseDirectory, "wechat-summary-control.exe");
        }
        var startInfo = new ProcessStartInfo(executable)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = session.WorkingDirectory,
        };
        startInfo.ArgumentList.Add("--pipe");
        startInfo.ArgumentList.Add(session.Pipe);
        // The token travels via the environment: command lines are visible to
        // every local process through Win32 process introspection.
        startInfo.EnvironmentVariables[TokenEnvironmentVariable] = session.Token;
        startInfo.ArgumentList.Add("--config");
        startInfo.ArgumentList.Add(session.ConfigPath);
        startInfo.ArgumentList.Add("--working-dir");
        startInfo.ArgumentList.Add(session.WorkingDirectory);
        Process.Start(startInfo);
    }

    private static void WriteSessionFile(string path, string contents)
    {
        File.WriteAllText(path, contents);
        RestrictToCurrentUser(path);
    }

    private static void RestrictToCurrentUser(string path)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var fileInfo = new FileInfo(path);
        var security = fileInfo.GetAccessControl();
        var identity = WindowsIdentity.GetCurrent();
        security.SetAccessRuleProtection(true, false);
        security.AddAccessRule(new FileSystemAccessRule(
            identity.User!,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        fileInfo.SetAccessControl(security);
    }

    private static string ResolveConfigPath()
    {
        var args = Environment.GetCommandLineArgs();
        for (var index = 0; index + 1 < args.Length; index++)
        {
            if (string.Equals(args[index], "--config", StringComparison.OrdinalIgnoreCase))
            {
                return Path.GetFullPath(args[index + 1]);
            }
        }
        return Path.Combine(AppContext.BaseDirectory, "config", "agent.toml");
    }

    private static string ResolveWorkingDirectory(string configPath)
    {
        var configDirectory = Path.GetDirectoryName(configPath) ?? AppContext.BaseDirectory;
        return string.Equals(Path.GetFileName(configDirectory), "config", StringComparison.OrdinalIgnoreCase)
            ? Path.GetDirectoryName(configDirectory) ?? AppContext.BaseDirectory
            : AppContext.BaseDirectory;
    }
}
