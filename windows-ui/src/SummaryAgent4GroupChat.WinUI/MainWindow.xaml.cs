using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using SummaryAgent4GroupChat.WinUI.Models;
using SummaryAgent4GroupChat.WinUI.ViewModels;
using SummaryAgent4GroupChat.WinUI.Views;

namespace SummaryAgent4GroupChat.WinUI;

public sealed partial class MainWindow : Window
{
    public MainViewModel ViewModel { get; } = new();

    public MainWindow()
    {
        InitializeComponent();
        SystemBackdrop = new MicaBackdrop();
        ExtendsContentIntoTitleBar = true;
        ContentFrame.Navigate(typeof(DashboardPage), ViewModel);
        Activated += async (_, _) => await ViewModel.InitializeAsync();
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
}
