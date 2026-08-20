using Microsoft.UI.Xaml;
using System.Text;

namespace SummaryAgent4GroupChat.WinUI;

public partial class App : Application
{
    public static Window? MainWindow { get; private set; }

    public App()
    {
        InitializeComponent();
        UnhandledException += (_, args) => WriteStartupFailure(args.Exception);
    }

    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        try
        {
            MainWindow = new MainWindow();
            MainWindow.Activate();
        }
        catch (Exception error)
        {
            WriteStartupFailure(error);
            throw;
        }
    }

    private static void WriteStartupFailure(Exception error)
    {
        try
        {
            var directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "SummaryAgent4GroupChat", "runtime");
            Directory.CreateDirectory(directory);
            File.AppendAllText(Path.Combine(directory, "winui-startup-errors.log"), $"[{DateTimeOffset.Now:O}] {error}\n\n", Encoding.UTF8);
        }
        catch
        {
            // Never mask the original UI initialization error with diagnostic I/O.
        }
    }
}
