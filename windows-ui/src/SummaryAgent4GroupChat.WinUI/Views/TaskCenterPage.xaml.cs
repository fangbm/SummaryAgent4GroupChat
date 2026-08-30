using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Navigation;
using SummaryAgent4GroupChat.WinUI.Models;
using SummaryAgent4GroupChat.WinUI.ViewModels;

namespace SummaryAgent4GroupChat.WinUI.Views;

public sealed partial class TaskCenterPage : Page
{
    private MainViewModel? ViewModel => DataContext as MainViewModel;
    public TaskCenterPage() => InitializeComponent();
    protected override async void OnNavigatedTo(NavigationEventArgs e)
    {
        DataContext = (MainViewModel)e.Parameter;
        await ((MainViewModel)DataContext).RefreshTaskCenterAsync();
    }
    private async void Refresh_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.RefreshTaskCenterAsync(); }
    private async void RefreshProviders_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.RefreshProviderHealthAsync(); }
    private async void TaskList_SelectionChanged(object sender, SelectionChangedEventArgs e) { if (ViewModel is not null && TaskList.SelectedItem is TaskCenterItem task) await ViewModel.SelectTaskAsync(task.Id); }
    private async void CancelTask_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null && sender is FrameworkElement { Tag: string id }) await ViewModel.CancelTaskAsync(id); }
    private async void RetryTask_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null && sender is FrameworkElement { Tag: string id }) await ViewModel.RetryTaskAsync(id); }
    private async void TaskDiagnostic_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null && sender is FrameworkElement { Tag: string id }) await ViewModel.CreateTaskDiagnosticBundleAsync(id); }
    private async void RetryOutbox_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null && sender is FrameworkElement { Tag: string id }) await ViewModel.RetryOutboxAsync(id); }
    private async void ResolveOutbox_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null && sender is FrameworkElement { Tag: string id }) await ViewModel.ResolveOutboxAsync(id); }
}
