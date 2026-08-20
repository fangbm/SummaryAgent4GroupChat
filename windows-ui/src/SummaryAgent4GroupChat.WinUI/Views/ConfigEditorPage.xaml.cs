using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Navigation;
using SummaryAgent4GroupChat.WinUI.Models;
using SummaryAgent4GroupChat.WinUI.ViewModels;

namespace SummaryAgent4GroupChat.WinUI.Views;

public sealed partial class ConfigEditorPage : Page
{
    private MainViewModel? ViewModel => DataContext as MainViewModel;

    public ConfigEditorPage() => InitializeComponent();

    protected override void OnNavigatedTo(NavigationEventArgs e)
    {
        var context = (EditorPageContext)e.Parameter;
        DataContext = context.ViewModel;

        (PageTitle.Text, PageDescription.Text) = context.Section switch
        {
            "platform" => Show(PlatformPanel, "接入平台", "平台、目标群、wxdb 与按群能力都在这里管理。"),
            "listen" => Show(ListenPanel, "监听与命令", "配置触发指令、白名单和两层冷却时间。"),
            "schedule" => Show(SchedulePanel, "定时总结", "设置每日执行时间、时间范围与发送内容。"),
            "models" => Show(ModelsPanel, "模型与媒体", "配置文字总结、图片生成和多媒体理解服务。"),
            _ => Show(PlatformPanel, "配置", "请从左侧进入对应设置页面。"),
        };
    }

    private (string Title, string Description) Show(UIElement panel, string title, string description)
    {
        PlatformPanel.Visibility = Visibility.Collapsed;
        ListenPanel.Visibility = Visibility.Collapsed;
        SchedulePanel.Visibility = Visibility.Collapsed;
        ModelsPanel.Visibility = Visibility.Collapsed;
        panel.Visibility = Visibility.Visible;
        return (title, description);
    }

    private async void Validate_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.ValidateAsync(); }
    private async void Save_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.SaveAsync(); }
    private async void SaveRaw_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.SaveRawConfigAsync(); }
    private async void Refresh_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.RefreshAsync(); }
    private async void OpenConfig_Click(object sender, RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.OpenPathAsync("config"); }
    private void LoadForm_Click(object sender, RoutedEventArgs e) => ViewModel?.LoadFormFromConfig();
}
