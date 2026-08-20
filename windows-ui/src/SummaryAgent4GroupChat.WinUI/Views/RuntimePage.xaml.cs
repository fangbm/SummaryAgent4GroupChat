using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Navigation;
using SummaryAgent4GroupChat.WinUI.ViewModels;

namespace SummaryAgent4GroupChat.WinUI.Views;

public sealed partial class RuntimePage : Page
{
    private MainViewModel? ViewModel => DataContext as MainViewModel;
    public RuntimePage() => InitializeComponent();
    protected override void OnNavigatedTo(NavigationEventArgs e) => DataContext = (MainViewModel)e.Parameter;
    private void TerminalBlock_SizeChanged(object sender, Microsoft.UI.Xaml.SizeChangedEventArgs e) { if (ViewModel?.FollowTerminal == true) TerminalScroll.ChangeView(null, TerminalScroll.ScrollableHeight, null, true); }
    private void LogBlock_SizeChanged(object sender, Microsoft.UI.Xaml.SizeChangedEventArgs e) { if (ViewModel?.FollowLogs == true) LogScroll.ChangeView(null, LogScroll.ScrollableHeight, null, true); }
    private void ClearTerminal_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) => ViewModel?.ClearTerminal();
    private void ClearLogs_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) => ViewModel?.ClearLogs();
    private async void OpenLogs_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.OpenPathAsync("logs"); }
}
