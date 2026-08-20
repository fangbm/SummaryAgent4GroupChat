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
            "platform" => ("接入平台", "管理 [platform]、[wx4py]、[discord]、[wxdb]、缓存位置与按群能力覆盖。所有字段可编辑，现有密钥保持脱敏。"),
            "listen" => ("监听与命令", "管理 [listen]、[rate_limit]、[manual_summary]、历史读取和长文本发送策略。"),
            "schedule" => ("定时总结", "管理 [scheduled_summary]，定时任务会在主程序热重载后采用新配置。"),
            "models" => ("模型与媒体", "管理 LLM、图片生成、图片/视频转述、语音转写、重试、并发、冷却和请求体覆盖。"),
            _ => ("配置", "完整 TOML 配置。保存前由 Rust 使用主程序同一套规则校验。"),
        };
    }

    private async void Validate_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.ValidateAsync(); }
    private async void Save_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.SaveAsync(); }
    private async void Refresh_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.RefreshAsync(); }
    private async void OpenConfig_Click(object sender, Microsoft.UI.Xaml.RoutedEventArgs e) { if (ViewModel is not null) await ViewModel.OpenPathAsync("config"); }
}
