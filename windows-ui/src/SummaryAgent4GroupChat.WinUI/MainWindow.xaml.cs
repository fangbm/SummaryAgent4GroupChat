using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using SummaryAgent4GroupChat.WinUI.Models;
using SummaryAgent4GroupChat.WinUI.ViewModels;
using SummaryAgent4GroupChat.WinUI.Views;

namespace SummaryAgent4GroupChat.WinUI;

public sealed partial class MainWindow : Window
{
    private bool _startupChecksCompleted;
    public MainViewModel ViewModel { get; } = new();

    public MainWindow()
    {
        InitializeComponent();
        SystemBackdrop = new MicaBackdrop();
        ExtendsContentIntoTitleBar = true;
        ContentFrame.Navigate(typeof(DashboardPage), ViewModel);
        Activated += async (_, _) => await InitializeAndCheckDependenciesAsync();
    }

    private async void Navigation_SelectionChanged(NavigationView sender, NavigationViewSelectionChangedEventArgs args)
    {
        if (args.SelectedItemContainer?.Tag is not string tag)
        {
            return;
        }

        if (tag == "refresh")
        {
            await ViewModel.RefreshAsync();
            return;
        }

        if (tag == "runtime")
        {
            ContentFrame.Navigate(typeof(RuntimePage), ViewModel);
            return;
        }

        if (tag == "updates")
        {
            ContentFrame.Navigate(typeof(UpdatesPage), ViewModel);
            return;
        }

        if (tag == "dashboard")
        {
            ContentFrame.Navigate(typeof(DashboardPage), ViewModel);
            return;
        }

        ContentFrame.Navigate(typeof(ConfigEditorPage), new EditorPageContext(ViewModel, tag));
    }

    private async Task InitializeAndCheckDependenciesAsync()
    {
        if (_startupChecksCompleted) return;
        _startupChecksCompleted = true;
        await ViewModel.InitializeAsync();
        if (!ViewModel.DependenciesNeedInstall) return;

        var xamlRoot = (Content as FrameworkElement)?.XamlRoot;
        if (xamlRoot is null) return;
        var dialog = new ContentDialog
        {
            XamlRoot = xamlRoot,
            Title = "需要安装微信运行依赖",
            Content = $"{ViewModel.DependencyStatus}\n\n是否现在安装？安装过程会申请管理员权限。",
            PrimaryButtonText = "安装依赖",
            CloseButtonText = "暂不安装",
            DefaultButton = ContentDialogButton.Primary,
        };
        if (await dialog.ShowAsync() == ContentDialogResult.Primary)
        {
            await ViewModel.InstallRuntimeAsync();
        }
    }
}
