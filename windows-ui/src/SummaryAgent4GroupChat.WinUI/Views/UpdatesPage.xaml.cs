using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Navigation;
using SummaryAgent4GroupChat.WinUI.Models;
using SummaryAgent4GroupChat.WinUI.ViewModels;

namespace SummaryAgent4GroupChat.WinUI.Views;

public sealed partial class UpdatesPage : Page
{
    private MainViewModel? ViewModel => DataContext as MainViewModel;

    public UpdatesPage() => InitializeComponent();

    protected override void OnNavigatedTo(NavigationEventArgs e) => DataContext = (MainViewModel)e.Parameter;

    private async void CheckUpdates_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e)
    {
        if (ViewModel is not null) await ViewModel.CheckForUpdatesAsync();
    }

    private async void CheckDependencies_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e)
    {
        if (ViewModel is not null) await ViewModel.CheckRuntimeDependenciesAsync();
    }

    private async void InstallRuntime_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e)
    {
        if (ViewModel is not null) await ViewModel.InstallRuntimeAsync();
    }

    private async void WxdbInit_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e)
    {
        if (ViewModel is not null) await ViewModel.RunWxdbInitAsync();
    }

    private async void OpenOutput_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e)
    {
        if (ViewModel is not null) await ViewModel.OpenPathAsync("output");
    }

    private async void InstallUpdate_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e)
    {
        if (ViewModel is not null && sender is Button { DataContext: UpdateCheckItem item })
        {
            await ViewModel.InstallUpdateAsync(item);
        }
    }
}
