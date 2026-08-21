using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Navigation;
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
}
