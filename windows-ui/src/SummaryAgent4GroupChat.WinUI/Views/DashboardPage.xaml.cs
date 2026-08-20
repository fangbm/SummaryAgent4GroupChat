using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Navigation;
using SummaryAgent4GroupChat.WinUI.ViewModels;

namespace SummaryAgent4GroupChat.WinUI.Views;

public sealed partial class DashboardPage : Page
{
    private MainViewModel? ViewModel => DataContext as MainViewModel;
    public DashboardPage() => InitializeComponent();
    protected override void OnNavigatedTo(NavigationEventArgs e) => DataContext = (MainViewModel)e.Parameter;
    private async void Start_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.StartAgentAsync(); }
    private async void Stop_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.StopAgentAsync(); }
    private async void Refresh_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.RefreshAsync(); }
    private async void InstallRuntime_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.InstallRuntimeAsync(); }
    private async void WxdbInit_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.RunWxdbInitAsync(); }
    private async void OpenOutput_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.OpenPathAsync("output"); }
}
