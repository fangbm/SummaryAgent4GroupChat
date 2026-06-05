#![cfg_attr(windows, windows_subsystem = "windows")]

use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use eframe::egui;
use serde_json::{Map as JsonMap, Value as JsonValue};
use toml_edit::{value, Array, DocumentMut, InlineTable, Item, Table, Value as TomlValue};
use wechat_summary_core::AgentConfig;

const APP_NAME: &str = "SummaryAgent4GroupChat";
const TERMINAL_MAX_CHARS: usize = 128 * 1024;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;

#[derive(Debug, Parser)]
#[command(name = "wechat-summary-gui")]
#[command(about = "Native desktop manager for SummaryAgent4GroupChat")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    config_path: PathBuf,
    working_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct ConfigView {
    platform_kind: String,
    wx_groups: String,
    wx_python: String,
    wx_sidecar: String,
    wx_ready_timeout: u64,
    discord_channels: String,
    discord_token_env: String,
    triggers: String,
    match_mode: String,
    whitelist_rooms: String,
    ignore_self: bool,
    rate_limit_enabled: bool,
    summary_cooldown_seconds: i64,
    image_cooldown_seconds: i64,
    manual_image_by_default: bool,
    scheduled_enabled: bool,
    scheduled_hour: u32,
    scheduled_minute: u32,
    scheduled_range_hours: i64,
    scheduled_rooms: String,
    scheduled_send_text: bool,
    scheduled_send_image: bool,
    history_max_messages: u32,
    wx_cli_executable: String,
    wx_cli_timeout: u64,
    wx_cli_history_timeout: u64,
    wx_cli_temp_dir: String,
    llm_provider: String,
    llm_api_key_env: String,
    llm_base_url_env: String,
    llm_model: String,
    llm_model_env: String,
    llm_timeout: u64,
    llm_retry_5xx_attempts: u32,
    llm_max_tokens: u32,
    llm_max_concurrent_chunk_requests: u32,
    llm_max_input_chars: u32,
    llm_request_body_overrides: String,
    image_enabled: bool,
    image_provider: String,
    image_api_key_env: String,
    image_base_url_env: String,
    image_model_env: String,
    image_size: String,
    image_resolution: String,
    image_timeout: u64,
    image_retry_5xx_attempts: u32,
    image_caption_enabled: bool,
    image_caption_provider: String,
    image_caption_api_key_env: String,
    image_caption_base_url_env: String,
    image_caption_model: String,
    image_caption_model_env: String,
    image_caption_timeout: u64,
    image_caption_retry_5xx_attempts: u32,
    image_caption_max_tokens: u32,
    image_caption_max_images: u32,
    image_caption_max_concurrent_requests: u32,
    image_caption_request_body_overrides: String,
    voice_transcription_enabled: bool,
    voice_transcription_provider: String,
    voice_transcription_api_key_env: String,
    voice_transcription_base_url_env: String,
    voice_transcription_model: String,
    voice_transcription_model_env: String,
    voice_transcription_timeout: u64,
    voice_transcription_retry_5xx_attempts: u32,
    voice_transcription_language: String,
    voice_transcription_prompt: String,
    voice_transcription_response_format: String,
    voice_transcription_transcode_to_mp3: bool,
    voice_transcription_ffmpeg_executable: String,
    voice_transcription_mp3_bitrate: String,
    voice_transcription_max_voices: u32,
    voice_transcription_max_concurrent_requests: u32,
    voice_transcription_request_body_overrides: String,
    runtime_output_dir: String,
    runtime_log_level: String,
    runtime_cleanup_days: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Tab {
    Dashboard,
    Platform,
    Listen,
    Schedule,
    Model,
    Runtime,
}

struct GuiApp {
    state: AppState,
    view: ConfigView,
    validation: String,
    log_tail: String,
    log_path_label: String,
    terminal_output: String,
    agent: Option<AgentProcess>,
    wxdb_init: Option<WxdbInitProcess>,
    status: StatusView,
    tab: Tab,
    message: Option<String>,
    last_status_refresh: Instant,
}

struct AgentProcess {
    child: Child,
    output: Receiver<String>,
    pid: u32,
}

struct WxdbInitProcess {
    child: Child,
    output: Receiver<String>,
    pid: u32,
}

#[derive(Default)]
struct StatusView {
    targets: usize,
    app_ready: bool,
    wxdb_ready: bool,
    python_ready: bool,
    llm_key_present: bool,
    image_key_present: bool,
    discord_token_present: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = resolve_config_path(args.config.as_deref())?;
    if !config_path.exists() {
        bail!("config file does not exist: {}", config_path.display());
    }
    let working_dir = infer_working_dir(&config_path)?;
    env::set_current_dir(&working_dir).context("setting working directory")?;

    let state = AppState {
        config_path,
        working_dir,
    };
    let app = GuiApp::new(state)?;
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1040.0, 760.0])
            .with_min_inner_size([860.0, 620.0]),
        ..Default::default()
    };

    eframe::run_native(
        APP_NAME,
        native_options,
        Box::new(|cc| {
            configure_fonts(&cc.egui_ctx);
            Ok(Box::new(app))
        }),
    )
    .map_err(|error| anyhow!("{error}"))
}

fn configure_fonts(ctx: &egui::Context) {
    let Some((font_name, font_bytes)) = load_cjk_font() else {
        return;
    };

    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        font_name.clone(),
        egui::FontData::from_owned(font_bytes).into(),
    );
    fonts
        .families
        .entry(egui::FontFamily::Proportional)
        .or_default()
        .insert(0, font_name.clone());
    fonts
        .families
        .entry(egui::FontFamily::Monospace)
        .or_default()
        .insert(0, font_name);
    ctx.set_fonts(fonts);
}

fn load_cjk_font() -> Option<(String, Vec<u8>)> {
    let font_names = [
        "NotoSansSC-VF.ttf",
        "Noto Sans SC (TrueType).otf",
        "simhei.ttf",
        "Deng.ttf",
        "Dengb.ttf",
        "msyh.ttc",
        "msyhbd.ttc",
        "simsun.ttc",
    ];

    #[cfg(windows)]
    {
        let mut font_dirs = Vec::new();
        if let Some(windir) = env::var_os("WINDIR").map(PathBuf::from) {
            font_dirs.push(windir.join("Fonts"));
        }
        font_dirs.push(PathBuf::from(r"C:\Windows\Fonts"));

        for dir in font_dirs {
            for font_name in font_names {
                let path = dir.join(font_name);
                if let Ok(bytes) = fs::read(&path) {
                    return Some(("summary-agent-cjk".to_string(), bytes));
                }
            }
        }
    }

    #[cfg(not(windows))]
    {
        let font_dirs = [
            PathBuf::from("/usr/share/fonts"),
            PathBuf::from("/usr/local/share/fonts"),
        ];
        for dir in font_dirs {
            for font_name in font_names {
                let path = dir.join(font_name);
                if let Ok(bytes) = fs::read(&path) {
                    return Some(("summary-agent-cjk".to_string(), bytes));
                }
            }
        }
    }

    None
}

impl GuiApp {
    fn new(state: AppState) -> Result<Self> {
        let (view, validation) = load_config_view(&state)?;
        let mut app = Self {
            state,
            view,
            validation,
            log_tail: String::new(),
            log_path_label: String::new(),
            terminal_output: "GUI 已就绪，主程序终端输出会显示在这里。\n".to_string(),
            agent: None,
            wxdb_init: None,
            status: StatusView::default(),
            tab: Tab::Dashboard,
            message: None,
            last_status_refresh: Instant::now(),
        };
        app.refresh_status();
        Ok(app)
    }

    fn refresh(&mut self) {
        match load_config_view(&self.state) {
            Ok((view, validation)) => {
                self.view = view;
                self.validation = validation;
                self.refresh_status();
                self.message = Some("配置已刷新".to_string());
            }
            Err(error) => self.message = Some(format!("刷新失败：{error:#}")),
        }
    }

    fn save(&mut self) {
        match save_config_update(&self.state, self.view.clone())
            .and_then(|_| load_config_view(&self.state))
        {
            Ok((view, validation)) => {
                self.view = view;
                self.validation = validation;
                self.refresh_status();
                self.message = Some("配置已保存".to_string());
            }
            Err(error) => self.message = Some(format!("保存失败：{error:#}")),
        }
    }

    fn refresh_status(&mut self) {
        let targets = if self.view.platform_kind.eq_ignore_ascii_case("discord") {
            let channels = split_lines(&self.view.discord_channels);
            if channels.is_empty() {
                split_lines(&self.view.whitelist_rooms).len()
            } else {
                channels.len()
            }
        } else {
            split_lines(&self.view.wx_groups).len()
        };
        let output_dir = resolve_working_path(&self.state, &self.view.runtime_output_dir);
        let log_path = output_dir.join("wechat-summary-app.log");
        let log_path_display = log_path.display().to_string();
        self.log_path_label = log_path_display.clone();
        self.log_tail = tail_file(&log_path, 16 * 1024)
            .map(|text| redact_secret_like_tokens(&text))
            .unwrap_or_else(|error| format!("暂无日志\n路径：{log_path_display}\n原因：{error}"));
        self.status = StatusView {
            targets,
            app_ready: find_exe("wechat-summary-app").exists(),
            wxdb_ready: find_exe("wxdb").exists(),
            python_ready: resolve_working_path(&self.state, &self.view.wx_python).exists(),
            llm_key_present: env_or_direct_value_present(&self.view.llm_api_key_env),
            image_key_present: env_or_direct_value_present(&self.view.image_api_key_env),
            discord_token_present: env_present(&self.view.discord_token_env),
        };
    }

    fn open_config(&mut self) {
        if let Err(error) = open_path(&self.state.config_path) {
            self.message = Some(format!("打开配置失败：{error:#}"));
        }
    }

    fn open_output(&mut self) {
        let output_dir = resolve_working_path(&self.state, &self.view.runtime_output_dir);
        match fs::create_dir_all(&output_dir)
            .and_then(|_| open_path(&output_dir).map_err(std::io::Error::other))
        {
            Ok(_) => {}
            Err(error) => self.message = Some(format!("打开输出失败：{error:#}")),
        }
    }

    fn install_runtime(&mut self) {
        match install_runtime(&self.state) {
            Ok(_) => self.message = Some("已启动 Python Runtime 安装脚本".to_string()),
            Err(error) => self.message = Some(format!("安装运行时失败：{error:#}")),
        }
    }

    fn run_wxdb_init(&mut self) {
        self.poll_wxdb_init_output();
        if self.wxdb_init.is_some() {
            self.message = Some("wxdb init 正在运行".to_string());
            return;
        }

        match start_wxdb_init(&self.state) {
            Ok(process) => {
                self.append_terminal_line(format!("[gui] wxdb init 已启动 pid={}\n", process.pid));
                self.wxdb_init = Some(process);
                self.message = Some("wxdb init 已启动，输出会显示在 GUI 终端".to_string());
            }
            Err(error) => self.message = Some(format!("运行 wxdb init 失败：{error:#}")),
        }
    }

    fn start_agent(&mut self) {
        self.poll_agent_output();
        if self.agent.is_some() {
            self.message = Some("主程序已在 GUI 中运行".to_string());
            return;
        }

        if self.view.platform_kind.eq_ignore_ascii_case("wx") {
            let python = resolve_working_path(&self.state, &self.view.wx_python);
            if !python.exists() {
                self.message = Some(format!(
                    "Python 运行时不存在：{}。请先运行安装目录里的 install.ps1 或开始菜单里的 Install Python Runtime。",
                    python.display()
                ));
                return;
            }
        }

        match stop_existing_agent_processes() {
            Ok(Some(summary)) => self.append_terminal_line(format!("[gui] {summary}\n")),
            Ok(None) => {}
            Err(error) => {
                self.message = Some(format!("清理旧主程序失败：{error:#}"));
                return;
            }
        }

        match start_agent(&self.state) {
            Ok(agent) => {
                self.append_terminal_line(format!(
                    "[gui] 主程序已启动 pid={}，命令行窗口已隐藏\n",
                    agent.pid
                ));
                self.agent = Some(agent);
                self.message = Some("主程序已启动，终端输出已合并到 GUI".to_string());
            }
            Err(error) => self.message = Some(format!("启动失败：{error:#}")),
        }
    }

    fn start_agent_elevated(&mut self) {
        self.poll_agent_output();
        if self.agent.is_some() {
            self.message = Some("主程序已在 GUI 中运行".to_string());
            return;
        }

        if self.view.platform_kind.eq_ignore_ascii_case("wx") {
            let python = resolve_working_path(&self.state, &self.view.wx_python);
            if !python.exists() {
                self.message = Some(format!(
                    "Python 运行时不存在：{}。请先运行安装目录里的 install.ps1 或开始菜单里的 Install Python Runtime。",
                    python.display()
                ));
                return;
            }
        }

        match stop_existing_agent_processes() {
            Ok(Some(summary)) => self.append_terminal_line(format!("[gui] {summary}\n")),
            Ok(None) => {}
            Err(error) => {
                self.message = Some(format!("清理旧主程序失败：{error:#}"));
                return;
            }
        }

        match start_agent_elevated(&self.state) {
            Ok(_) => {
                self.append_terminal_line(
                    "[gui] 已请求管理员权限启动主程序；UAC 提权进程输出请查看日志尾部。\n"
                        .to_string(),
                );
                self.message = Some("已请求管理员权限启动主程序，请确认 UAC 弹窗".to_string());
            }
            Err(error) => self.message = Some(format!("管理员启动失败：{error:#}")),
        }
    }

    fn stop_agent(&mut self) {
        let Some(mut agent) = self.agent.take() else {
            self.message = Some("当前没有由 GUI 托管的主程序".to_string());
            return;
        };
        let pid = agent.pid;
        match agent.child.kill() {
            Ok(_) => {
                let _ = agent.child.wait();
                self.append_terminal_line(format!("[gui] 已停止主程序 pid={pid}\n"));
                self.message = Some("主程序已停止".to_string());
            }
            Err(error) => {
                self.append_terminal_line(format!("[gui] 停止主程序失败 pid={pid}: {error}\n"));
                self.message = Some(format!("停止失败：{error}"));
                self.agent = Some(agent);
            }
        }
    }

    fn poll_agent_output(&mut self) {
        let mut lines = Vec::new();
        let mut exit_status = None;
        if let Some(agent) = &mut self.agent {
            lines.extend(agent.output.try_iter());
            match agent.child.try_wait() {
                Ok(Some(status)) => exit_status = Some(format!("{status}")),
                Ok(None) => {}
                Err(error) => exit_status = Some(format!("检查进程状态失败：{error}")),
            }
        }

        for line in lines {
            self.append_terminal_line(line);
        }

        if let Some(status) = exit_status {
            self.append_terminal_line(format!("[gui] 主程序已退出：{status}\n"));
            self.agent = None;
        }
    }

    fn poll_wxdb_init_output(&mut self) {
        let mut lines = Vec::new();
        let mut exit_status = None;
        if let Some(process) = &mut self.wxdb_init {
            lines.extend(process.output.try_iter());
            match process.child.try_wait() {
                Ok(Some(status)) => exit_status = Some(format!("{status}")),
                Ok(None) => {}
                Err(error) => exit_status = Some(format!("检查 wxdb init 状态失败：{error}")),
            }
        }

        for line in lines {
            self.append_terminal_line(line);
        }

        if let Some(status) = exit_status {
            self.append_terminal_line(format!("[gui] wxdb init 已退出：{status}\n"));
            self.wxdb_init = None;
            self.refresh_status();
        }
    }

    fn append_terminal_line(&mut self, line: String) {
        self.terminal_output
            .push_str(&redact_secret_like_tokens(&line));
        if self.terminal_output.len() > TERMINAL_MAX_CHARS {
            let overflow = self.terminal_output.len() - TERMINAL_MAX_CHARS;
            let split_at = self
                .terminal_output
                .char_indices()
                .find_map(|(index, _)| (index >= overflow).then_some(index))
                .unwrap_or(overflow);
            self.terminal_output.drain(..split_at);
        }
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_agent_output();
        self.poll_wxdb_init_output();
        let should_refresh_runtime =
            self.agent.is_some() || self.wxdb_init.is_some() || self.tab == Tab::Runtime;
        if should_refresh_runtime && self.last_status_refresh.elapsed() >= Duration::from_secs(1) {
            self.refresh_status();
            self.last_status_refresh = Instant::now();
        }
        if should_refresh_runtime {
            ctx.request_repaint_after(Duration::from_millis(500));
        }

        egui::SidePanel::left("nav")
            .exact_width(220.0)
            .show(ctx, |ui| {
                ui.heading(APP_NAME);
                ui.add_space(4.0);
                ui.label("原生管理界面");
                ui.separator();
                nav_button(ui, &mut self.tab, Tab::Dashboard, "仪表盘");
                nav_button(ui, &mut self.tab, Tab::Platform, "接入平台");
                nav_button(ui, &mut self.tab, Tab::Listen, "监听与命令");
                nav_button(ui, &mut self.tab, Tab::Schedule, "定时总结");
                nav_button(ui, &mut self.tab, Tab::Model, "模型与图片");
                nav_button(ui, &mut self.tab, Tab::Runtime, "运行信息");
                ui.separator();
                if ui.button("刷新").clicked() {
                    self.refresh();
                }
                if ui.button("保存配置").clicked() {
                    self.save();
                }
                if ui.button("打开配置").clicked() {
                    self.open_config();
                }
                if ui.button("打开输出目录").clicked() {
                    self.open_output();
                }
                if ui.button("安装 Python Runtime").clicked() {
                    self.install_runtime();
                }
                if ui.button("运行 wxdb init").clicked() {
                    self.run_wxdb_init();
                }
                if ui.button("启动主程序").clicked() {
                    self.start_agent();
                }
                if ui.button("管理员启动主程序").clicked() {
                    self.start_agent_elevated();
                }
                if ui.button("停止托管主程序").clicked() {
                    self.stop_agent();
                }
            });

        egui::CentralPanel::default().show(ctx, |ui| match self.tab {
            Tab::Dashboard => dashboard_tab(ui, self),
            Tab::Platform => {
                egui::Frame::group(ui.style()).show(ui, |ui| platform_tab(ui, &mut self.view));
            }
            Tab::Listen => {
                egui::Frame::group(ui.style()).show(ui, |ui| listen_tab(ui, &mut self.view));
            }
            Tab::Schedule => {
                egui::Frame::group(ui.style()).show(ui, |ui| schedule_tab(ui, &mut self.view));
            }
            Tab::Model => {
                egui::ScrollArea::vertical()
                    .id_salt("model-tab-scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.set_min_width(ui.available_width());
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            ui.set_min_width(ui.available_width());
                            model_tab(ui, &mut self.view);
                        });
                    });
            }
            Tab::Runtime => {
                egui::Frame::group(ui.style()).show(ui, |ui| {
                    runtime_tab(
                        ui,
                        &mut self.view,
                        &mut self.log_tail,
                        &self.log_path_label,
                        &mut self.terminal_output,
                        self.agent.is_some(),
                    )
                });
            }
        });
    }
}

fn nav_button(ui: &mut egui::Ui, tab: &mut Tab, value: Tab, label: &str) {
    if ui.selectable_label(*tab == value, label).clicked() {
        *tab = value;
    }
}

fn dashboard_tab(ui: &mut egui::Ui, app: &GuiApp) {
    ui.heading("SummaryAgent4GroupChat");
    ui.label(format!("配置：{}", app.state.config_path.display()));
    ui.label(format!("工作目录：{}", app.state.working_dir.display()));
    if let Some(message) = &app.message {
        ui.add_space(4.0);
        ui.colored_label(egui::Color32::from_rgb(37, 99, 235), message);
    }
    ui.add_space(8.0);
    status_cards(ui, app);
    ui.add_space(10.0);
    dashboard_statistics(ui, app);
}

fn dashboard_statistics(ui: &mut egui::Ui, app: &GuiApp) {
    ui.heading("统计信息");
    let triggers = split_lines(&app.view.triggers).len();
    let whitelist_rooms = split_lines(&app.view.whitelist_rooms).len();
    let scheduled_rooms = split_lines(&app.view.scheduled_rooms).len();
    let terminal_lines = app.terminal_output.lines().count();
    let output_dir = resolve_working_path(&app.state, &app.view.runtime_output_dir);
    let log_size = fs::metadata(output_dir.join("wechat-summary-app.log"))
        .map(|metadata| format_bytes(metadata.len()))
        .unwrap_or_else(|_| "暂无".to_string());

    egui::Grid::new("dashboard-stats-grid")
        .num_columns(4)
        .spacing([10.0, 8.0])
        .show(ui, |ui| {
            card(
                ui,
                "运行状态",
                if app.agent.is_some() {
                    "GUI 托管中"
                } else {
                    "未托管"
                },
            );
            card(ui, "触发词", &format!("{} 个", triggers));
            card(ui, "白名单", &format!("{} 个", whitelist_rooms));
            card(ui, "定时目标", &format!("{} 个", scheduled_rooms));
            ui.end_row();
            card(ui, "终端行数", &format!("{} 行", terminal_lines));
            card(ui, "日志文件", &log_size);
            card(
                ui,
                "图片生成",
                if app.view.image_enabled {
                    "已启用"
                } else {
                    "已关闭"
                },
            );
            card(
                ui,
                "手动图片",
                if app.view.manual_image_by_default {
                    "默认生成"
                } else {
                    "按参数生成"
                },
            );
            ui.end_row();
        });
}

fn status_cards(ui: &mut egui::Ui, app: &GuiApp) {
    let scheduled_text = if app.view.scheduled_enabled {
        format!(
            "{:02}:{:02} / {}h",
            app.view.scheduled_hour, app.view.scheduled_minute, app.view.scheduled_range_hours
        )
    } else {
        "disabled".to_string()
    };

    egui::Grid::new("status-grid")
        .num_columns(4)
        .spacing([10.0, 8.0])
        .show(ui, |ui| {
            card(ui, "配置", &app.validation);
            card(
                ui,
                "平台",
                if app.view.platform_kind.eq_ignore_ascii_case("discord") {
                    "Discord"
                } else {
                    "微信"
                },
            );
            card(ui, "目标", &format!("{} 个", app.status.targets));
            card(ui, "定时", &scheduled_text);
            ui.end_row();
            card(ui, "主程序", yes_no(app.status.app_ready));
            card(ui, "wxdb", yes_no(app.status.wxdb_ready));
            card(ui, "Python Runtime", yes_no(app.status.python_ready));
            card(ui, "LLM Key", yes_no(app.status.llm_key_present));
            ui.end_row();
            card(ui, "Image Key", yes_no(app.status.image_key_present));
            card(
                ui,
                "Discord Token",
                yes_no(app.status.discord_token_present),
            );
            card(
                ui,
                "图片模式",
                if app.view.manual_image_by_default {
                    "默认图片"
                } else {
                    "按参数生成"
                },
            );
            ui.end_row();
        });
}

fn card(ui: &mut egui::Ui, title: &str, value: &str) {
    egui::Frame::group(ui.style())
        .fill(egui::Color32::WHITE)
        .show(ui, |ui| {
            ui.set_min_size([185.0, 54.0].into());
            ui.small(title);
            ui.strong(value);
        });
}

fn platform_tab(ui: &mut egui::Ui, view: &mut ConfigView) {
    ui.heading("接入平台");
    ui.horizontal(|ui| {
        ui.label("当前平台");
        egui::ComboBox::from_id_salt("platform-kind")
            .selected_text(if view.platform_kind.eq_ignore_ascii_case("discord") {
                "Discord / dc"
            } else {
                "微信 / wx"
            })
            .show_ui(ui, |ui| {
                ui.selectable_value(&mut view.platform_kind, "wx".to_string(), "微信 / wx");
                ui.selectable_value(
                    &mut view.platform_kind,
                    "discord".to_string(),
                    "Discord / dc",
                );
            });
    });
    two_columns(
        ui,
        |ui| {
            multiline_field(ui, "微信群聊", &mut view.wx_groups, 6);
            text_field(ui, "Python 路径", &mut view.wx_python);
            text_field(ui, "wx4py 脚本", &mut view.wx_sidecar);
            number_u64(ui, "就绪超时秒数", &mut view.wx_ready_timeout);
        },
        |ui| {
            multiline_field(ui, "Discord 频道 ID", &mut view.discord_channels, 6);
            text_field(ui, "Discord Token 环境变量", &mut view.discord_token_env);
        },
    );
}

fn listen_tab(ui: &mut egui::Ui, view: &mut ConfigView) {
    ui.heading("监听与命令");
    two_columns(
        ui,
        |ui| {
            multiline_field(ui, "触发词", &mut view.triggers, 5);
            ui.checkbox(&mut view.ignore_self, "忽略自己发送的消息");
            ui.checkbox(&mut view.manual_image_by_default, "手动总结默认生成图片");
            ui.checkbox(&mut view.rate_limit_enabled, "启用手动总结冷却");
            number_i64(ui, "总结指令冷却秒数", &mut view.summary_cooldown_seconds);
            number_i64(ui, "图片额外冷却秒数", &mut view.image_cooldown_seconds);
            number_u32(ui, "历史读取最大条数", &mut view.history_max_messages);
        },
        |ui| {
            multiline_field(ui, "监听白名单", &mut view.whitelist_rooms, 5);
            ui.label("匹配模式");
            egui::ComboBox::from_id_salt("match-mode")
                .selected_text(&view.match_mode)
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut view.match_mode, "prefix".to_string(), "prefix");
                    ui.selectable_value(&mut view.match_mode, "contains".to_string(), "contains");
                    ui.selectable_value(&mut view.match_mode, "regex".to_string(), "regex");
                });
        },
    );
}

fn schedule_tab(ui: &mut egui::Ui, view: &mut ConfigView) {
    ui.heading("定时总结");
    ui.checkbox(&mut view.scheduled_enabled, "启用定时总结");
    ui.horizontal(|ui| {
        number_u32(ui, "小时", &mut view.scheduled_hour);
        number_u32(ui, "分钟", &mut view.scheduled_minute);
        number_i64(ui, "范围小时", &mut view.scheduled_range_hours);
    });
    ui.horizontal(|ui| {
        ui.checkbox(&mut view.scheduled_send_text, "发送文字总结");
        ui.checkbox(&mut view.scheduled_send_image, "发送图片总结");
    });
    multiline_field(ui, "定时房间/频道", &mut view.scheduled_rooms, 7);
}

fn model_tab(ui: &mut egui::Ui, view: &mut ConfigView) {
    ui.heading("模型与图片");
    ui.columns(3, |columns| {
        let ui = &mut columns[0];
        ui.heading("文字总结");
        text_field(ui, "LLM Provider", &mut view.llm_provider);
        text_field(ui, "LLM API Key 环境变量/直接值", &mut view.llm_api_key_env);
        text_field(
            ui,
            "LLM Base URL 环境变量/直接值",
            &mut view.llm_base_url_env,
        );
        text_field(ui, "模型名称", &mut view.llm_model);
        text_field(ui, "模型环境变量", &mut view.llm_model_env);
        number_u64(ui, "LLM 超时秒数", &mut view.llm_timeout);
        number_u32(ui, "5xx 重试次数", &mut view.llm_retry_5xx_attempts);
        number_u32(ui, "最大输出 Token", &mut view.llm_max_tokens);
        number_u32(
            ui,
            "超长分段最大并发请求数",
            &mut view.llm_max_concurrent_chunk_requests,
        );
        number_u32(ui, "LLM 最大输入字符数", &mut view.llm_max_input_chars);
        multiline_field(
            ui,
            "LLM 请求体覆盖(JSON)",
            &mut view.llm_request_body_overrides,
            6,
        );

        let ui = &mut columns[1];
        ui.heading("图片生成");
        ui.checkbox(&mut view.image_enabled, "启用图片生成");
        text_field(ui, "图片 Provider", &mut view.image_provider);
        text_field(
            ui,
            "图片 API Key 环境变量/直接值",
            &mut view.image_api_key_env,
        );
        text_field(
            ui,
            "图片 Base URL 环境变量/直接值",
            &mut view.image_base_url_env,
        );
        text_field(ui, "图片 Model 环境变量/直接值", &mut view.image_model_env);
        text_field(ui, "图片尺寸", &mut view.image_size);
        text_field(ui, "图片分辨率", &mut view.image_resolution);
        number_u64(ui, "图片超时秒数", &mut view.image_timeout);
        number_u32(ui, "5xx 重试次数", &mut view.image_retry_5xx_attempts);

        let ui = &mut columns[2];
        ui.heading("图片转述");
        ui.checkbox(&mut view.image_caption_enabled, "启用图片转述");
        text_field(ui, "转述 Provider", &mut view.image_caption_provider);
        text_field(
            ui,
            "转述 API Key 环境变量/直接值",
            &mut view.image_caption_api_key_env,
        );
        text_field(
            ui,
            "转述 Base URL 环境变量/直接值",
            &mut view.image_caption_base_url_env,
        );
        text_field(ui, "转述模型名称", &mut view.image_caption_model);
        text_field(ui, "转述模型环境变量", &mut view.image_caption_model_env);
        number_u64(ui, "转述超时秒数", &mut view.image_caption_timeout);
        number_u32(
            ui,
            "5xx 重试次数",
            &mut view.image_caption_retry_5xx_attempts,
        );
        number_u32(ui, "转述最大输出 Token", &mut view.image_caption_max_tokens);
        number_u32(ui, "每次最多转述图片数", &mut view.image_caption_max_images);
        number_u32(
            ui,
            "转述最大并发请求数",
            &mut view.image_caption_max_concurrent_requests,
        );
        multiline_field(
            ui,
            "转述请求体覆盖(JSON)",
            &mut view.image_caption_request_body_overrides,
            6,
        );
        ui.separator();
        ui.heading("语音转写");
        ui.checkbox(&mut view.voice_transcription_enabled, "启用语音转写");
        text_field(ui, "语音 Provider", &mut view.voice_transcription_provider);
        text_field(
            ui,
            "语音 API Key 环境变量/直接值",
            &mut view.voice_transcription_api_key_env,
        );
        text_field(
            ui,
            "语音 Base URL 环境变量/直接值",
            &mut view.voice_transcription_base_url_env,
        );
        text_field(ui, "语音模型名称", &mut view.voice_transcription_model);
        text_field(
            ui,
            "语音模型环境变量",
            &mut view.voice_transcription_model_env,
        );
        number_u64(ui, "语音超时秒数", &mut view.voice_transcription_timeout);
        number_u32(
            ui,
            "5xx 重试次数",
            &mut view.voice_transcription_retry_5xx_attempts,
        );
        text_field(ui, "语音语言", &mut view.voice_transcription_language);
        text_field(ui, "语音提示词", &mut view.voice_transcription_prompt);
        text_field(
            ui,
            "语音响应格式",
            &mut view.voice_transcription_response_format,
        );
        ui.checkbox(
            &mut view.voice_transcription_transcode_to_mp3,
            "转写前统一转码为 MP3",
        );
        text_field(
            ui,
            "ffmpeg 可执行文件",
            &mut view.voice_transcription_ffmpeg_executable,
        );
        text_field(ui, "MP3 码率", &mut view.voice_transcription_mp3_bitrate);
        number_u32(
            ui,
            "每次最多转写语音数",
            &mut view.voice_transcription_max_voices,
        );
        number_u32(
            ui,
            "语音最大并发请求数",
            &mut view.voice_transcription_max_concurrent_requests,
        );
        multiline_field(
            ui,
            "语音请求体覆盖(JSON)",
            &mut view.voice_transcription_request_body_overrides,
            4,
        );
    });
}

fn runtime_tab(
    ui: &mut egui::Ui,
    view: &mut ConfigView,
    log_tail: &mut String,
    log_path_label: &str,
    terminal_output: &mut String,
    agent_running: bool,
) {
    ui.heading("运行信息");
    two_columns(
        ui,
        |ui| {
            text_field(ui, "wxdb 命令", &mut view.wx_cli_executable);
            number_u64(ui, "wxdb 命令超时秒数", &mut view.wx_cli_timeout);
            number_u64(ui, "历史查询总超时秒数", &mut view.wx_cli_history_timeout);
            text_field(ui, "wxdb 临时目录", &mut view.wx_cli_temp_dir);
        },
        |ui| {
            text_field(ui, "输出目录", &mut view.runtime_output_dir);
            text_field(ui, "日志等级", &mut view.runtime_log_level);
            number_u32(ui, "清理天数", &mut view.runtime_cleanup_days);
        },
    );
    ui.separator();
    let remaining_height = ui.available_height().max(420.0);
    let pane_height = ((remaining_height - 42.0) / 2.0).clamp(190.0, 340.0);
    ui.horizontal(|ui| {
        ui.label("GUI 终端");
        ui.small(if agent_running {
            "主程序运行中"
        } else {
            "主程序未托管运行"
        });
    });
    readonly_scroll_text(
        ui,
        "gui-terminal-scroll",
        "gui-terminal-text",
        terminal_output,
        pane_height,
        true,
        TextRenderMode::Ansi,
    );
    ui.add_space(8.0);
    ui.horizontal(|ui| {
        ui.label("日志文件尾部");
        ui.small(log_path_label);
    });
    readonly_scroll_text(
        ui,
        "log-tail-scroll",
        "log-tail-text",
        log_tail,
        pane_height,
        false,
        TextRenderMode::Plain,
    );
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TextRenderMode {
    Plain,
    Ansi,
}

fn readonly_scroll_text(
    ui: &mut egui::Ui,
    scroll_id: &'static str,
    label_id: &'static str,
    text: &str,
    height: f32,
    stick_to_bottom: bool,
    render_mode: TextRenderMode,
) {
    let scroll_area = egui::ScrollArea::vertical()
        .id_salt(scroll_id)
        .auto_shrink([false, false])
        .max_height(height)
        .stick_to_bottom(stick_to_bottom);
    egui::Frame::group(ui.style()).show(ui, |ui| {
        ui.set_height(height);
        scroll_area.show(ui, |ui| {
            ui.push_id(label_id, |ui| {
                let label = match render_mode {
                    TextRenderMode::Plain => {
                        egui::Label::new(egui::RichText::new(text).monospace())
                    }
                    TextRenderMode::Ansi => egui::Label::new(terminal_layout_job(ui, text)),
                }
                .wrap()
                .selectable(true);
                ui.add(label);
            });
        });
    });
}

#[derive(Debug, Clone, Copy, Default)]
struct AnsiTextStyle {
    foreground: Option<egui::Color32>,
    bold: bool,
}

fn terminal_layout_job(ui: &egui::Ui, text: &str) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    for line in text.split_inclusive('\n') {
        let plain_line = strip_ansi_sgr(line);
        let fallback_color = terminal_line_color(&plain_line);
        append_ansi_text(ui, &mut job, line, fallback_color);
    }
    job
}

fn append_ansi_text(
    ui: &egui::Ui,
    job: &mut egui::text::LayoutJob,
    text: &str,
    fallback_color: Option<egui::Color32>,
) {
    let mut style = AnsiTextStyle::default();
    let mut index = 0;
    let mut segment_start = 0;
    let bytes = text.as_bytes();

    while index < bytes.len() {
        if bytes[index] == b'\x1b' && index + 1 < bytes.len() && bytes[index + 1] == b'[' {
            if let Some(end_offset) = text[index + 2..].bytes().position(|byte| byte == b'm') {
                append_ansi_segment(ui, job, &style, fallback_color, &text[segment_start..index]);
                let params = &text[index + 2..index + 2 + end_offset];
                apply_ansi_sgr(params, &mut style);
                index += 2 + end_offset + 1;
                segment_start = index;
                continue;
            }
        }

        index += text[index..]
            .chars()
            .next()
            .map(char::len_utf8)
            .unwrap_or(1);
    }

    append_ansi_segment(ui, job, &style, fallback_color, &text[segment_start..]);
}

fn append_ansi_segment(
    ui: &egui::Ui,
    job: &mut egui::text::LayoutJob,
    style: &AnsiTextStyle,
    fallback_color: Option<egui::Color32>,
    text: &str,
) {
    if text.is_empty() {
        return;
    }

    let mut color = style
        .foreground
        .or(fallback_color)
        .unwrap_or_else(|| ui.visuals().text_color());
    if style.bold && style.foreground.is_none() && fallback_color.is_none() {
        color = egui::Color32::from_rgb(31, 41, 55);
    }

    job.append(
        text,
        0.0,
        egui::TextFormat {
            font_id: egui::TextStyle::Monospace.resolve(ui.style()),
            color,
            ..Default::default()
        },
    );
}

fn terminal_line_color(line: &str) -> Option<egui::Color32> {
    let upper = line.to_ascii_uppercase();
    if upper.contains(" ERROR ")
        || upper.contains(" ERROR:")
        || upper.contains(" - ERROR -")
        || upper.contains("[ERROR]")
        || upper.contains("失败")
    {
        return Some(egui::Color32::from_rgb(220, 38, 38));
    }
    if upper.contains(" WARN ")
        || upper.contains(" WARNING ")
        || upper.contains(" - WARNING -")
        || upper.contains("[WARN]")
        || upper.contains("警告")
    {
        return Some(egui::Color32::from_rgb(217, 119, 6));
    }
    if line.starts_with("[gui]") {
        return Some(egui::Color32::from_rgb(37, 99, 235));
    }
    if upper.contains(" INFO ") || upper.contains(" - INFO -") || upper.contains("[INFO]") {
        return Some(egui::Color32::from_rgb(22, 163, 74));
    }
    if upper.contains(" DEBUG ")
        || upper.contains(" TRACE ")
        || upper.contains("[DEBUG]")
        || upper.contains("[TRACE]")
    {
        return Some(egui::Color32::from_rgb(107, 114, 128));
    }
    if line.starts_with("[stderr]") || line.starts_with("[wxdb stderr]") {
        return Some(egui::Color32::from_rgb(217, 119, 6));
    }
    None
}

fn strip_ansi_sgr(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut index = 0;
    let bytes = text.as_bytes();
    while index < bytes.len() {
        if bytes[index] == b'\x1b' && index + 1 < bytes.len() && bytes[index + 1] == b'[' {
            if let Some(end_offset) = text[index + 2..].bytes().position(|byte| byte == b'm') {
                index += 2 + end_offset + 1;
                continue;
            }
        }
        let next = text[index..]
            .chars()
            .next()
            .map(char::len_utf8)
            .unwrap_or(1);
        output.push_str(&text[index..index + next]);
        index += next;
    }
    output
}

fn apply_ansi_sgr(params: &str, style: &mut AnsiTextStyle) {
    let codes: Vec<i32> = if params.trim().is_empty() {
        vec![0]
    } else {
        params
            .split(';')
            .map(|part| part.parse::<i32>().unwrap_or(0))
            .collect()
    };

    let mut index = 0;
    while index < codes.len() {
        let code = codes[index];
        match code {
            0 => *style = AnsiTextStyle::default(),
            1 => style.bold = true,
            22 => style.bold = false,
            30..=37 | 90..=97 => {
                style.foreground = Some(ansi_basic_color(code, style.bold));
            }
            38 if index + 1 < codes.len() => match codes[index + 1] {
                2 if index + 4 < codes.len() => {
                    style.foreground = Some(egui::Color32::from_rgb(
                        codes[index + 2].clamp(0, 255) as u8,
                        codes[index + 3].clamp(0, 255) as u8,
                        codes[index + 4].clamp(0, 255) as u8,
                    ));
                    index += 4;
                }
                5 if index + 2 < codes.len() => {
                    style.foreground = Some(ansi_256_color(codes[index + 2].clamp(0, 255) as u8));
                    index += 2;
                }
                _ => {}
            },
            39 => style.foreground = None,
            _ => {}
        }
        index += 1;
    }
}

fn ansi_basic_color(code: i32, bold: bool) -> egui::Color32 {
    let bright = bold || code >= 90;
    match code % 10 {
        0 => {
            if bright {
                egui::Color32::from_rgb(107, 114, 128)
            } else {
                egui::Color32::from_rgb(55, 65, 81)
            }
        }
        1 => {
            if bright {
                egui::Color32::from_rgb(239, 68, 68)
            } else {
                egui::Color32::from_rgb(220, 38, 38)
            }
        }
        2 => {
            if bright {
                egui::Color32::from_rgb(34, 197, 94)
            } else {
                egui::Color32::from_rgb(22, 163, 74)
            }
        }
        3 => {
            if bright {
                egui::Color32::from_rgb(234, 179, 8)
            } else {
                egui::Color32::from_rgb(202, 138, 4)
            }
        }
        4 => {
            if bright {
                egui::Color32::from_rgb(59, 130, 246)
            } else {
                egui::Color32::from_rgb(37, 99, 235)
            }
        }
        5 => {
            if bright {
                egui::Color32::from_rgb(217, 70, 239)
            } else {
                egui::Color32::from_rgb(192, 38, 211)
            }
        }
        6 => {
            if bright {
                egui::Color32::from_rgb(6, 182, 212)
            } else {
                egui::Color32::from_rgb(8, 145, 178)
            }
        }
        7 => {
            if bright {
                egui::Color32::from_rgb(243, 244, 246)
            } else {
                egui::Color32::from_rgb(156, 163, 175)
            }
        }
        _ => egui::Color32::from_rgb(31, 41, 55),
    }
}

fn ansi_256_color(index: u8) -> egui::Color32 {
    const BASIC: [egui::Color32; 16] = [
        egui::Color32::from_rgb(55, 65, 81),
        egui::Color32::from_rgb(220, 38, 38),
        egui::Color32::from_rgb(22, 163, 74),
        egui::Color32::from_rgb(202, 138, 4),
        egui::Color32::from_rgb(37, 99, 235),
        egui::Color32::from_rgb(192, 38, 211),
        egui::Color32::from_rgb(8, 145, 178),
        egui::Color32::from_rgb(156, 163, 175),
        egui::Color32::from_rgb(107, 114, 128),
        egui::Color32::from_rgb(239, 68, 68),
        egui::Color32::from_rgb(34, 197, 94),
        egui::Color32::from_rgb(234, 179, 8),
        egui::Color32::from_rgb(59, 130, 246),
        egui::Color32::from_rgb(217, 70, 239),
        egui::Color32::from_rgb(6, 182, 212),
        egui::Color32::from_rgb(243, 244, 246),
    ];

    match index {
        0..=15 => BASIC[index as usize],
        16..=231 => {
            let cube_index = index - 16;
            let levels = [0, 95, 135, 175, 215, 255];
            let red = levels[(cube_index / 36) as usize];
            let green = levels[((cube_index % 36) / 6) as usize];
            let blue = levels[(cube_index % 6) as usize];
            egui::Color32::from_rgb(red, green, blue)
        }
        232..=255 => {
            let value = 8 + (index - 232) * 10;
            egui::Color32::from_gray(value)
        }
    }
}

fn two_columns(
    ui: &mut egui::Ui,
    left: impl FnOnce(&mut egui::Ui),
    right: impl FnOnce(&mut egui::Ui),
) {
    ui.columns(2, |columns| {
        left(&mut columns[0]);
        right(&mut columns[1]);
    });
}

fn text_field(ui: &mut egui::Ui, label: &str, value: &mut String) {
    ui.label(label);
    ui.text_edit_singleline(value);
}

fn multiline_field(ui: &mut egui::Ui, label: &str, value: &mut String, rows: usize) {
    ui.label(label);
    ui.add(
        egui::TextEdit::multiline(value)
            .desired_rows(rows)
            .desired_width(f32::INFINITY),
    );
}

fn number_u64(ui: &mut egui::Ui, label: &str, value: &mut u64) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(egui::DragValue::new(value).range(0..=u64::MAX).speed(1));
    });
}

fn number_u32(ui: &mut egui::Ui, label: &str, value: &mut u32) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(egui::DragValue::new(value).range(0..=u32::MAX).speed(1));
    });
}

fn number_i64(ui: &mut egui::Ui, label: &str, value: &mut i64) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(egui::DragValue::new(value).range(0..=i64::MAX).speed(1));
    });
}

fn split_llm_model_fields(model: Option<String>, model_env: String) -> (String, String) {
    split_model_fields(model, model_env, "LLM_MODEL")
}

fn split_model_fields(
    model: Option<String>,
    model_env: String,
    default_model_env: &str,
) -> (String, String) {
    let model = model.unwrap_or_default().trim().to_string();
    if !model.is_empty() {
        return (model, model_env);
    }

    let trimmed_env = model_env.trim();
    if should_treat_model_env_as_model_name(trimmed_env, default_model_env) {
        (trimmed_env.to_string(), default_model_env.to_string())
    } else {
        (String::new(), model_env)
    }
}

fn should_treat_model_env_as_model_name(value: &str, default_model_env: &str) -> bool {
    if value.is_empty() || value == default_model_env || env::var(value).is_ok() {
        return false;
    }

    value.chars().any(|ch| ch.is_ascii_lowercase())
}

fn non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn load_config_view(state: &AppState) -> Result<(ConfigView, String)> {
    let text = fs::read_to_string(&state.config_path)
        .with_context(|| format!("reading {}", state.config_path.display()))?;
    let doc = text
        .parse::<DocumentMut>()
        .with_context(|| format!("parsing {}", state.config_path.display()))?;
    let validation = match AgentConfig::from_toml_str(&text) {
        Ok(_) => "配置有效".to_string(),
        Err(error) => format!("配置无效：{error}"),
    };
    Ok((config_view_from_doc(&doc, &text), validation))
}

fn config_view_from_doc(doc: &DocumentMut, text: &str) -> ConfigView {
    if let Ok(config) = AgentConfig::from_toml_str(text) {
        let history_max_messages = config.history_message_limit().min(u32::MAX as usize) as u32;
        let llm_request_body_overrides =
            request_body_overrides_to_json(&config.llm.request_body_overrides);
        let (llm_model, llm_model_env) =
            split_llm_model_fields(config.llm.model, config.llm.model_env);
        let image_caption_request_body_overrides =
            request_body_overrides_to_json(&config.image_caption.request_body_overrides);
        let (image_caption_model, image_caption_model_env) = split_model_fields(
            config.image_caption.model,
            config.image_caption.model_env,
            "IMAGE_CAPTION_MODEL",
        );
        let voice_transcription_request_body_overrides =
            request_body_overrides_to_json(&config.voice_transcription.request_body_overrides);
        let (voice_transcription_model, voice_transcription_model_env) = split_model_fields(
            config.voice_transcription.model,
            config.voice_transcription.model_env,
            "VOICE_TRANSCRIPTION_MODEL",
        );
        return ConfigView {
            platform_kind: config.platform.kind.as_str().to_string(),
            wx_groups: join_lines(&config.wx4py.groups),
            wx_python: config.wx4py.python_executable,
            wx_sidecar: config.wx4py.sidecar_script,
            wx_ready_timeout: config.wx4py.ready_timeout_seconds,
            discord_channels: join_lines(&config.discord.channels),
            discord_token_env: config.discord.token_env,
            triggers: join_lines(&config.listen.triggers),
            match_mode: format!("{:?}", config.listen.match_mode).to_ascii_lowercase(),
            whitelist_rooms: join_lines(&config.listen.whitelist_rooms),
            ignore_self: config.listen.ignore_self,
            rate_limit_enabled: config.rate_limit.enabled,
            summary_cooldown_seconds: config.rate_limit.successful_request_cooldown_seconds,
            image_cooldown_seconds: config.rate_limit.successful_image_cooldown_seconds,
            manual_image_by_default: config.manual_summary.image_by_default,
            scheduled_enabled: config.scheduled_summary.enabled,
            scheduled_hour: config.scheduled_summary.local_hour,
            scheduled_minute: config.scheduled_summary.local_minute,
            scheduled_range_hours: config.scheduled_summary.range_hours,
            scheduled_rooms: join_lines(&config.scheduled_summary.rooms),
            scheduled_send_text: config.scheduled_summary.send_text,
            scheduled_send_image: config.scheduled_summary.send_image,
            history_max_messages,
            wx_cli_executable: config.wx_cli.executable,
            wx_cli_timeout: config.wx_cli.timeout_seconds,
            wx_cli_history_timeout: config.wx_cli.history_query_timeout_seconds,
            wx_cli_temp_dir: config.wx_cli.temp_dir,
            llm_provider: config.llm.provider,
            llm_api_key_env: config.llm.api_key_env,
            llm_base_url_env: config.llm.base_url_env,
            llm_model,
            llm_model_env,
            llm_timeout: config.llm.timeout_seconds,
            llm_retry_5xx_attempts: config.llm.retry_5xx_attempts.min(u32::MAX as usize) as u32,
            llm_max_tokens: config.llm.max_output_tokens,
            llm_max_concurrent_chunk_requests: config
                .llm
                .max_concurrent_chunk_requests
                .min(u32::MAX as usize) as u32,
            llm_max_input_chars: config.privacy.max_chars_to_llm.min(u32::MAX as usize) as u32,
            llm_request_body_overrides,
            image_enabled: config.image_gen.enabled,
            image_provider: config.image_gen.provider,
            image_api_key_env: config.image_gen.api_key_env,
            image_base_url_env: config.image_gen.base_url_env,
            image_model_env: config.image_gen.model_env,
            image_size: config.image_gen.size,
            image_resolution: config.image_gen.resolution.unwrap_or_default(),
            image_timeout: config.image_gen.timeout_seconds,
            image_retry_5xx_attempts: config.image_gen.retry_5xx_attempts.min(u32::MAX as usize)
                as u32,
            image_caption_enabled: config.image_caption.enabled,
            image_caption_provider: config.image_caption.provider,
            image_caption_api_key_env: config.image_caption.api_key_env,
            image_caption_base_url_env: config.image_caption.base_url_env,
            image_caption_model,
            image_caption_model_env,
            image_caption_timeout: config.image_caption.timeout_seconds,
            image_caption_retry_5xx_attempts: config
                .image_caption
                .retry_5xx_attempts
                .min(u32::MAX as usize) as u32,
            image_caption_max_tokens: config.image_caption.max_output_tokens,
            image_caption_max_images: config
                .image_caption
                .max_images_per_summary
                .min(u32::MAX as usize) as u32,
            image_caption_max_concurrent_requests: config
                .image_caption
                .max_concurrent_requests
                .min(u32::MAX as usize) as u32,
            image_caption_request_body_overrides,
            voice_transcription_enabled: config.voice_transcription.enabled,
            voice_transcription_provider: config.voice_transcription.provider,
            voice_transcription_api_key_env: config.voice_transcription.api_key_env,
            voice_transcription_base_url_env: config.voice_transcription.base_url_env,
            voice_transcription_model,
            voice_transcription_model_env,
            voice_transcription_timeout: config.voice_transcription.timeout_seconds,
            voice_transcription_retry_5xx_attempts: config
                .voice_transcription
                .retry_5xx_attempts
                .min(u32::MAX as usize) as u32,
            voice_transcription_language: config.voice_transcription.language,
            voice_transcription_prompt: config.voice_transcription.prompt,
            voice_transcription_response_format: config.voice_transcription.response_format,
            voice_transcription_transcode_to_mp3: config.voice_transcription.transcode_to_mp3,
            voice_transcription_ffmpeg_executable: config.voice_transcription.ffmpeg_executable,
            voice_transcription_mp3_bitrate: config.voice_transcription.mp3_bitrate,
            voice_transcription_max_voices: config
                .voice_transcription
                .max_voices_per_summary
                .min(u32::MAX as usize) as u32,
            voice_transcription_max_concurrent_requests: config
                .voice_transcription
                .max_concurrent_requests
                .min(u32::MAX as usize)
                as u32,
            voice_transcription_request_body_overrides,
            runtime_output_dir: config.runtime.output_dir,
            runtime_log_level: config.runtime.log_level,
            runtime_cleanup_days: config.runtime.cleanup_after_days,
        };
    }

    let (llm_model, llm_model_env) = split_llm_model_fields(
        non_empty_string(get_str(doc, "llm", "model", "")),
        get_str(doc, "llm", "model_env", "LLM_MODEL"),
    );
    let (image_caption_model, image_caption_model_env) = split_model_fields(
        non_empty_string(get_str(doc, "image_caption", "model", "")),
        get_str(doc, "image_caption", "model_env", "IMAGE_CAPTION_MODEL"),
        "IMAGE_CAPTION_MODEL",
    );
    let (voice_transcription_model, voice_transcription_model_env) = split_model_fields(
        non_empty_string(get_str(doc, "voice_transcription", "model", "")),
        get_str(
            doc,
            "voice_transcription",
            "model_env",
            "VOICE_TRANSCRIPTION_MODEL",
        ),
        "VOICE_TRANSCRIPTION_MODEL",
    );

    ConfigView {
        platform_kind: get_str(doc, "platform", "kind", "wx"),
        wx_groups: join_lines(&get_array(doc, "wx4py", "groups")),
        wx_python: get_str(
            doc,
            "wx4py",
            "python_executable",
            "..\\.venv\\Scripts\\python.exe",
        ),
        wx_sidecar: get_str(
            doc,
            "wx4py",
            "sidecar_script",
            "..\\scripts\\wx4py_sidecar.py",
        ),
        wx_ready_timeout: get_u64(doc, "wx4py", "ready_timeout_seconds", 60),
        discord_channels: join_lines(&get_array(doc, "discord", "channels")),
        discord_token_env: get_str(doc, "discord", "token_env", "DISCORD_BOT_TOKEN"),
        triggers: join_lines(&get_array(doc, "listen", "triggers")),
        match_mode: get_str(doc, "listen", "match_mode", "prefix"),
        whitelist_rooms: join_lines(&get_array(doc, "listen", "whitelist_rooms")),
        ignore_self: get_bool(doc, "listen", "ignore_self", true),
        rate_limit_enabled: get_bool(doc, "rate_limit", "enabled", true),
        summary_cooldown_seconds: get_i64(
            doc,
            "rate_limit",
            "successful_request_cooldown_seconds",
            300,
        ),
        image_cooldown_seconds: get_i64(doc, "rate_limit", "successful_image_cooldown_seconds", 0),
        manual_image_by_default: get_bool(doc, "manual_summary", "image_by_default", false),
        scheduled_enabled: get_bool(doc, "scheduled_summary", "enabled", true),
        scheduled_hour: get_u64(doc, "scheduled_summary", "local_hour", 22) as u32,
        scheduled_minute: get_u64(doc, "scheduled_summary", "local_minute", 0) as u32,
        scheduled_range_hours: get_i64(doc, "scheduled_summary", "range_hours", 24),
        scheduled_rooms: join_lines(&get_array(doc, "scheduled_summary", "rooms")),
        scheduled_send_text: get_bool(doc, "scheduled_summary", "send_text", true),
        scheduled_send_image: get_bool(doc, "scheduled_summary", "send_image", true),
        history_max_messages: get_history_max_messages(doc).min(u32::MAX as u64) as u32,
        wx_cli_executable: get_str_alias(doc, "wxdb", "wx_cli", "executable", "builtin"),
        wx_cli_timeout: get_u64_alias(doc, "wxdb", "wx_cli", "timeout_seconds", 20),
        wx_cli_history_timeout: get_u64_alias(
            doc,
            "wxdb",
            "wx_cli",
            "history_query_timeout_seconds",
            60,
        ),
        wx_cli_temp_dir: get_str_alias(doc, "wxdb", "wx_cli", "temp_dir", ".\\runtime\\wx-exports"),
        llm_provider: get_str(doc, "llm", "provider", "openai_compatible"),
        llm_api_key_env: get_str(doc, "llm", "api_key_env", "LLM_API_KEY"),
        llm_base_url_env: get_str(doc, "llm", "base_url_env", "LLM_BASE_URL"),
        llm_model,
        llm_model_env,
        llm_timeout: get_u64(doc, "llm", "timeout_seconds", 120),
        llm_retry_5xx_attempts: get_u64(doc, "llm", "retry_5xx_attempts", 5).min(u32::MAX as u64)
            as u32,
        llm_max_tokens: get_u64(doc, "llm", "max_output_tokens", 2000) as u32,
        llm_max_concurrent_chunk_requests: get_u64(doc, "llm", "max_concurrent_chunk_requests", 4)
            .min(u32::MAX as u64) as u32,
        llm_max_input_chars: get_u64(doc, "privacy", "max_chars_to_llm", 20_000)
            .min(u32::MAX as u64) as u32,
        llm_request_body_overrides: request_body_overrides_from_doc(doc),
        image_enabled: get_bool(doc, "image_gen", "enabled", true),
        image_provider: get_str(doc, "image_gen", "provider", "openai"),
        image_api_key_env: get_str(doc, "image_gen", "api_key_env", "IMAGE_API_KEY"),
        image_base_url_env: get_str(doc, "image_gen", "base_url_env", "IMAGE_BASE_URL"),
        image_model_env: get_str(doc, "image_gen", "model_env", "IMAGE_MODEL"),
        image_size: get_str(doc, "image_gen", "size", "2:3"),
        image_resolution: get_str(doc, "image_gen", "resolution", "1k"),
        image_timeout: get_u64(doc, "image_gen", "timeout_seconds", 300),
        image_retry_5xx_attempts: get_u64(doc, "image_gen", "retry_5xx_attempts", 5)
            .min(u32::MAX as u64) as u32,
        image_caption_enabled: get_bool(doc, "image_caption", "enabled", false),
        image_caption_provider: get_str(doc, "image_caption", "provider", "openai_compatible"),
        image_caption_api_key_env: get_str(
            doc,
            "image_caption",
            "api_key_env",
            "IMAGE_CAPTION_API_KEY",
        ),
        image_caption_base_url_env: get_str(
            doc,
            "image_caption",
            "base_url_env",
            "IMAGE_CAPTION_BASE_URL",
        ),
        image_caption_model,
        image_caption_model_env,
        image_caption_timeout: get_u64(doc, "image_caption", "timeout_seconds", 120),
        image_caption_retry_5xx_attempts: get_u64(doc, "image_caption", "retry_5xx_attempts", 5)
            .min(u32::MAX as u64) as u32,
        image_caption_max_tokens: get_u64(doc, "image_caption", "max_output_tokens", 500) as u32,
        image_caption_max_images: get_u64(doc, "image_caption", "max_images_per_summary", 20)
            .min(u32::MAX as u64) as u32,
        image_caption_max_concurrent_requests: get_u64(
            doc,
            "image_caption",
            "max_concurrent_requests",
            4,
        )
        .min(u32::MAX as u64) as u32,
        image_caption_request_body_overrides: request_body_overrides_from_table_doc(
            doc,
            "image_caption",
        ),
        voice_transcription_enabled: get_bool(doc, "voice_transcription", "enabled", false),
        voice_transcription_provider: get_str(
            doc,
            "voice_transcription",
            "provider",
            "openai_compatible",
        ),
        voice_transcription_api_key_env: get_str(
            doc,
            "voice_transcription",
            "api_key_env",
            "VOICE_TRANSCRIPTION_API_KEY",
        ),
        voice_transcription_base_url_env: get_str(
            doc,
            "voice_transcription",
            "base_url_env",
            "VOICE_TRANSCRIPTION_BASE_URL",
        ),
        voice_transcription_model,
        voice_transcription_model_env,
        voice_transcription_timeout: get_u64(doc, "voice_transcription", "timeout_seconds", 120),
        voice_transcription_retry_5xx_attempts: get_u64(
            doc,
            "voice_transcription",
            "retry_5xx_attempts",
            5,
        )
        .min(u32::MAX as u64) as u32,
        voice_transcription_language: get_str(doc, "voice_transcription", "language", "zh"),
        voice_transcription_prompt: get_str(doc, "voice_transcription", "prompt", ""),
        voice_transcription_response_format: get_str(
            doc,
            "voice_transcription",
            "response_format",
            "json",
        ),
        voice_transcription_transcode_to_mp3: get_bool(
            doc,
            "voice_transcription",
            "transcode_to_mp3",
            true,
        ),
        voice_transcription_ffmpeg_executable: get_str(
            doc,
            "voice_transcription",
            "ffmpeg_executable",
            "ffmpeg",
        ),
        voice_transcription_mp3_bitrate: get_str(doc, "voice_transcription", "mp3_bitrate", "64k"),
        voice_transcription_max_voices: get_u64(
            doc,
            "voice_transcription",
            "max_voices_per_summary",
            20,
        )
        .min(u32::MAX as u64) as u32,
        voice_transcription_max_concurrent_requests: get_u64(
            doc,
            "voice_transcription",
            "max_concurrent_requests",
            2,
        )
        .min(u32::MAX as u64) as u32,
        voice_transcription_request_body_overrides: request_body_overrides_from_table_doc(
            doc,
            "voice_transcription",
        ),
        runtime_output_dir: get_str(doc, "runtime", "output_dir", ".\\runtime\\rust-output"),
        runtime_log_level: get_str(doc, "runtime", "log_level", "info"),
        runtime_cleanup_days: get_u64(doc, "runtime", "cleanup_after_days", 7) as u32,
    }
}

fn save_config_update(state: &AppState, update: ConfigView) -> Result<()> {
    let text = fs::read_to_string(&state.config_path)
        .with_context(|| format!("reading {}", state.config_path.display()))?;
    let mut doc = text.parse::<DocumentMut>().context("parsing config TOML")?;

    set_str(
        table_mut(&mut doc, "platform"),
        "kind",
        &update.platform_kind,
    );

    let wx4py = table_mut(&mut doc, "wx4py");
    set_str(wx4py, "python_executable", &update.wx_python);
    set_str(wx4py, "sidecar_script", &update.wx_sidecar);
    set_int(
        wx4py,
        "ready_timeout_seconds",
        update.wx_ready_timeout as i64,
    );
    set_array(wx4py, "groups", &split_lines(&update.wx_groups));

    let discord = table_mut(&mut doc, "discord");
    set_str(discord, "token_env", &update.discord_token_env);
    set_array(discord, "channels", &split_lines(&update.discord_channels));

    let listen = table_mut(&mut doc, "listen");
    set_array(listen, "triggers", &split_lines(&update.triggers));
    set_str(listen, "match_mode", &update.match_mode);
    set_array(
        listen,
        "whitelist_rooms",
        &split_lines(&update.whitelist_rooms),
    );
    set_bool(listen, "ignore_self", update.ignore_self);

    let rate_limit = table_mut(&mut doc, "rate_limit");
    set_bool(rate_limit, "enabled", update.rate_limit_enabled);
    set_int(
        rate_limit,
        "successful_request_cooldown_seconds",
        update.summary_cooldown_seconds,
    );
    set_int(
        rate_limit,
        "successful_image_cooldown_seconds",
        update.image_cooldown_seconds,
    );

    set_bool(
        table_mut(&mut doc, "manual_summary"),
        "image_by_default",
        update.manual_image_by_default,
    );

    let scheduled = table_mut(&mut doc, "scheduled_summary");
    set_bool(scheduled, "enabled", update.scheduled_enabled);
    set_int(scheduled, "local_hour", update.scheduled_hour as i64);
    set_int(scheduled, "local_minute", update.scheduled_minute as i64);
    set_int(scheduled, "range_hours", update.scheduled_range_hours);
    set_array(scheduled, "rooms", &split_lines(&update.scheduled_rooms));
    set_bool(scheduled, "send_text", update.scheduled_send_text);
    set_bool(scheduled, "send_image", update.scheduled_send_image);

    let history = table_mut(&mut doc, "history");
    set_int(history, "max_messages", update.history_max_messages as i64);

    migrate_legacy_table(&mut doc, "wx_cli", "wxdb");
    let wxdb = table_mut(&mut doc, "wxdb");
    set_str(wxdb, "executable", &update.wx_cli_executable);
    remove_key(wxdb, "max_messages");
    set_int(wxdb, "timeout_seconds", update.wx_cli_timeout as i64);
    set_int(
        wxdb,
        "history_query_timeout_seconds",
        update.wx_cli_history_timeout as i64,
    );
    set_str(wxdb, "temp_dir", &update.wx_cli_temp_dir);
    remove_table(&mut doc, "wx_cli");

    let privacy = table_mut(&mut doc, "privacy");
    remove_key(privacy, "max_messages_to_llm");
    set_int(
        privacy,
        "max_chars_to_llm",
        update.llm_max_input_chars as i64,
    );

    let llm = table_mut(&mut doc, "llm");
    set_str(llm, "provider", &update.llm_provider);
    set_str(llm, "api_key_env", &update.llm_api_key_env);
    set_str(llm, "base_url_env", &update.llm_base_url_env);
    if update.llm_model.trim().is_empty() {
        remove_key(llm, "model");
    } else {
        set_str(llm, "model", update.llm_model.trim());
    }
    set_str(llm, "model_env", &update.llm_model_env);
    set_int(llm, "timeout_seconds", update.llm_timeout as i64);
    set_int(
        llm,
        "retry_5xx_attempts",
        update.llm_retry_5xx_attempts as i64,
    );
    set_int(llm, "max_output_tokens", update.llm_max_tokens as i64);
    set_int(
        llm,
        "max_concurrent_chunk_requests",
        update.llm_max_concurrent_chunk_requests.max(1) as i64,
    );
    set_json_object_table(
        llm,
        "request_body_overrides",
        &update.llm_request_body_overrides,
    )?;

    let image_gen = table_mut(&mut doc, "image_gen");
    set_bool(image_gen, "enabled", update.image_enabled);
    set_str(image_gen, "provider", &update.image_provider);
    set_str(image_gen, "api_key_env", &update.image_api_key_env);
    set_str(image_gen, "base_url_env", &update.image_base_url_env);
    set_str(image_gen, "model_env", &update.image_model_env);
    set_str(image_gen, "size", &update.image_size);
    set_str(image_gen, "resolution", &update.image_resolution);
    set_int(image_gen, "timeout_seconds", update.image_timeout as i64);
    set_int(
        image_gen,
        "retry_5xx_attempts",
        update.image_retry_5xx_attempts as i64,
    );

    let image_caption = table_mut(&mut doc, "image_caption");
    set_bool(image_caption, "enabled", update.image_caption_enabled);
    set_str(image_caption, "provider", &update.image_caption_provider);
    set_str(
        image_caption,
        "api_key_env",
        &update.image_caption_api_key_env,
    );
    set_str(
        image_caption,
        "base_url_env",
        &update.image_caption_base_url_env,
    );
    if update.image_caption_model.trim().is_empty() {
        remove_key(image_caption, "model");
    } else {
        set_str(image_caption, "model", update.image_caption_model.trim());
    }
    set_str(image_caption, "model_env", &update.image_caption_model_env);
    set_int(
        image_caption,
        "timeout_seconds",
        update.image_caption_timeout as i64,
    );
    set_int(
        image_caption,
        "retry_5xx_attempts",
        update.image_caption_retry_5xx_attempts as i64,
    );
    set_int(
        image_caption,
        "max_output_tokens",
        update.image_caption_max_tokens as i64,
    );
    set_int(
        image_caption,
        "max_images_per_summary",
        update.image_caption_max_images as i64,
    );
    set_int(
        image_caption,
        "max_concurrent_requests",
        update.image_caption_max_concurrent_requests.max(1) as i64,
    );
    set_json_object_table(
        image_caption,
        "request_body_overrides",
        &update.image_caption_request_body_overrides,
    )?;

    let voice_transcription = table_mut(&mut doc, "voice_transcription");
    set_bool(
        voice_transcription,
        "enabled",
        update.voice_transcription_enabled,
    );
    set_str(
        voice_transcription,
        "provider",
        &update.voice_transcription_provider,
    );
    set_str(
        voice_transcription,
        "api_key_env",
        &update.voice_transcription_api_key_env,
    );
    set_str(
        voice_transcription,
        "base_url_env",
        &update.voice_transcription_base_url_env,
    );
    if update.voice_transcription_model.trim().is_empty() {
        remove_key(voice_transcription, "model");
    } else {
        set_str(
            voice_transcription,
            "model",
            update.voice_transcription_model.trim(),
        );
    }
    set_str(
        voice_transcription,
        "model_env",
        &update.voice_transcription_model_env,
    );
    set_int(
        voice_transcription,
        "timeout_seconds",
        update.voice_transcription_timeout as i64,
    );
    set_int(
        voice_transcription,
        "retry_5xx_attempts",
        update.voice_transcription_retry_5xx_attempts as i64,
    );
    set_str(
        voice_transcription,
        "language",
        &update.voice_transcription_language,
    );
    set_str(
        voice_transcription,
        "prompt",
        &update.voice_transcription_prompt,
    );
    set_str(
        voice_transcription,
        "response_format",
        &update.voice_transcription_response_format,
    );
    set_bool(
        voice_transcription,
        "transcode_to_mp3",
        update.voice_transcription_transcode_to_mp3,
    );
    set_str(
        voice_transcription,
        "ffmpeg_executable",
        &update.voice_transcription_ffmpeg_executable,
    );
    set_str(
        voice_transcription,
        "mp3_bitrate",
        &update.voice_transcription_mp3_bitrate,
    );
    set_int(
        voice_transcription,
        "max_voices_per_summary",
        update.voice_transcription_max_voices as i64,
    );
    set_int(
        voice_transcription,
        "max_concurrent_requests",
        update.voice_transcription_max_concurrent_requests.max(1) as i64,
    );
    set_json_object_table(
        voice_transcription,
        "request_body_overrides",
        &update.voice_transcription_request_body_overrides,
    )?;

    let runtime = table_mut(&mut doc, "runtime");
    set_str(runtime, "output_dir", &update.runtime_output_dir);
    set_str(runtime, "log_level", &update.runtime_log_level);
    set_int(
        runtime,
        "cleanup_after_days",
        update.runtime_cleanup_days as i64,
    );

    let new_text = doc.to_string();
    AgentConfig::from_toml_str(&new_text).context("updated config is invalid")?;
    fs::write(&state.config_path, new_text)
        .with_context(|| format!("writing {}", state.config_path.display()))?;
    Ok(())
}

fn table<'a>(doc: &'a DocumentMut, name: &str) -> Option<&'a Table> {
    doc.get(name).and_then(Item::as_table)
}

fn table_mut<'a>(doc: &'a mut DocumentMut, name: &str) -> &'a mut Table {
    if !matches!(doc.get(name), Some(Item::Table(_))) {
        doc[name] = Item::Table(Table::new());
    }
    doc[name].as_table_mut().expect("table must exist")
}

fn get_str(doc: &DocumentMut, table_name: &str, key: &str, default: &str) -> String {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_str)
        .unwrap_or(default)
        .to_string()
}

fn get_bool(doc: &DocumentMut, table_name: &str, key: &str, default: bool) -> bool {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_bool)
        .unwrap_or(default)
}

fn get_i64(doc: &DocumentMut, table_name: &str, key: &str, default: i64) -> i64 {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_integer)
        .unwrap_or(default)
}

fn get_u64(doc: &DocumentMut, table_name: &str, key: &str, default: u64) -> u64 {
    get_i64(doc, table_name, key, default as i64).max(0) as u64
}

fn get_str_alias(
    doc: &DocumentMut,
    preferred_table: &str,
    legacy_table: &str,
    key: &str,
    default: &str,
) -> String {
    table(doc, preferred_table)
        .and_then(|table| table.get(key))
        .and_then(Item::as_str)
        .or_else(|| {
            table(doc, legacy_table)
                .and_then(|table| table.get(key))
                .and_then(Item::as_str)
        })
        .unwrap_or(default)
        .to_string()
}

fn get_u64_alias(
    doc: &DocumentMut,
    preferred_table: &str,
    legacy_table: &str,
    key: &str,
    default: u64,
) -> u64 {
    get_u64_opt(doc, preferred_table, key)
        .or_else(|| get_u64_opt(doc, legacy_table, key))
        .unwrap_or(default)
}

fn get_u64_opt(doc: &DocumentMut, table_name: &str, key: &str) -> Option<u64> {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_integer)
        .map(|value| value.max(0) as u64)
}

fn get_history_max_messages(doc: &DocumentMut) -> u64 {
    get_u64_opt(doc, "history", "max_messages")
        .or_else(|| get_u64_opt(doc, "privacy", "max_messages_to_llm"))
        .or_else(|| get_u64_opt(doc, "wxdb", "max_messages"))
        .or_else(|| get_u64_opt(doc, "wx_cli", "max_messages"))
        .unwrap_or(10_000)
}

fn request_body_overrides_to_json(overrides: &impl serde::Serialize) -> String {
    match serde_json::to_value(overrides) {
        Ok(JsonValue::Object(object)) if object.is_empty() => "{}".to_string(),
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string()),
        Err(_) => "{}".to_string(),
    }
}

fn request_body_overrides_from_doc(doc: &DocumentMut) -> String {
    request_body_overrides_from_table_doc(doc, "llm")
}

fn request_body_overrides_from_table_doc(doc: &DocumentMut, table_name: &str) -> String {
    let Some(item) = table(doc, table_name).and_then(|table| table.get("request_body_overrides"))
    else {
        return "{}".to_string();
    };
    toml_item_to_json(item)
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| "{}".to_string())
}

fn toml_item_to_json(item: &Item) -> Option<JsonValue> {
    match item {
        Item::Table(table) => Some(JsonValue::Object(toml_table_to_json_map(table))),
        Item::Value(value) => toml_value_to_json(value),
        _ => None,
    }
}

fn toml_table_to_json_map(table: &Table) -> JsonMap<String, JsonValue> {
    table
        .iter()
        .filter_map(|(key, item)| toml_item_to_json(item).map(|value| (key.to_string(), value)))
        .collect()
}

fn toml_value_to_json(value: &TomlValue) -> Option<JsonValue> {
    if let Some(value) = value.as_bool() {
        return Some(JsonValue::Bool(value));
    }
    if let Some(value) = value.as_integer() {
        return Some(JsonValue::Number(value.into()));
    }
    if let Some(value) = value.as_float() {
        return serde_json::Number::from_f64(value).map(JsonValue::Number);
    }
    if let Some(value) = value.as_str() {
        return Some(JsonValue::String(value.to_string()));
    }
    if let Some(array) = value.as_array() {
        return Some(JsonValue::Array(
            array.iter().filter_map(toml_value_to_json).collect(),
        ));
    }
    if let Some(table) = value.as_inline_table() {
        let object = table
            .iter()
            .filter_map(|(key, value)| {
                toml_value_to_json(value).map(|value| (key.to_string(), value))
            })
            .collect();
        return Some(JsonValue::Object(object));
    }
    None
}

fn get_array(doc: &DocumentMut, table_name: &str, key: &str) -> Vec<String> {
    table(doc, table_name)
        .and_then(|table| table.get(key))
        .and_then(Item::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn set_str(table: &mut Table, key: &str, new_value: &str) {
    table[key] = value(new_value);
}

fn set_bool(table: &mut Table, key: &str, new_value: bool) {
    table[key] = value(new_value);
}

fn set_int(table: &mut Table, key: &str, new_value: i64) {
    table[key] = value(new_value);
}

fn remove_key(table: &mut Table, key: &str) {
    table.remove(key);
}

fn migrate_legacy_table(doc: &mut DocumentMut, legacy: &str, current: &str) {
    if !matches!(doc.get(current), Some(Item::Table(_))) {
        if let Some(item) = doc.get(legacy).cloned() {
            doc[current] = item;
        }
    }
}

fn remove_table(doc: &mut DocumentMut, name: &str) {
    doc.remove(name);
}

fn set_array(table: &mut Table, key: &str, values: &[String]) {
    let mut array = Array::default();
    for item in values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        array.push(item);
    }
    table[key] = value(array);
}

fn set_json_object_table(table: &mut Table, key: &str, json_text: &str) -> Result<()> {
    let trimmed = json_text.trim();
    if trimmed.is_empty() {
        table.remove(key);
        return Ok(());
    }
    let json = serde_json::from_str::<JsonValue>(trimmed)
        .with_context(|| format!("parsing {key} JSON object"))?;
    let JsonValue::Object(object) = json else {
        bail!("{key} must be a JSON object");
    };
    if object.is_empty() {
        table.remove(key);
        return Ok(());
    }

    let mut toml_table = Table::new();
    for (field, value) in &object {
        toml_table[field] = json_to_toml_item(value)
            .with_context(|| format!("converting request body override field {field:?}"))?;
    }
    table[key] = Item::Table(toml_table);
    Ok(())
}

fn json_to_toml_item(value: &JsonValue) -> Result<Item> {
    match value {
        JsonValue::Object(object) => {
            let mut table = Table::new();
            for (key, value) in object {
                table[key] = json_to_toml_item(value)
                    .with_context(|| format!("converting nested JSON field {key:?}"))?;
            }
            Ok(Item::Table(table))
        }
        _ => json_to_toml_value(value).map(Item::Value),
    }
}

fn json_to_toml_value(value: &JsonValue) -> Result<TomlValue> {
    match value {
        JsonValue::Null => bail!("JSON null is not supported in TOML request body overrides"),
        JsonValue::Bool(value) => Ok(TomlValue::from(*value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(TomlValue::from(value))
            } else if let Some(value) = value.as_u64() {
                let value = i64::try_from(value)
                    .context("unsigned integer exceeds TOML signed integer range")?;
                Ok(TomlValue::from(value))
            } else if let Some(value) = value.as_f64() {
                Ok(TomlValue::from(value))
            } else {
                bail!("unsupported JSON number")
            }
        }
        JsonValue::String(value) => Ok(TomlValue::from(value.as_str())),
        JsonValue::Array(values) => {
            let mut array = Array::new();
            for value in values {
                array.push(json_to_toml_value(value)?);
            }
            Ok(TomlValue::Array(array))
        }
        JsonValue::Object(object) => {
            let mut table = InlineTable::new();
            for (key, value) in object {
                table.insert(key, json_to_toml_value(value)?);
            }
            Ok(TomlValue::InlineTable(table))
        }
    }
}

fn join_lines(values: &[String]) -> String {
    values.join("\n")
}

fn split_lines(value: &str) -> Vec<String> {
    value
        .split(|ch| ch == '\n' || ch == '\r' || ch == ',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn open_path(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        if path.is_dir() {
            Command::new("explorer.exe").arg(path).spawn()?;
        } else {
            Command::new("notepad.exe").arg(path).spawn()?;
        }
        return Ok(());
    }
    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg(path).spawn()?;
        return Ok(());
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Command::new("xdg-open").arg(path).spawn()?;
        return Ok(());
    }
    #[allow(unreachable_code)]
    Ok(())
}

fn start_agent(state: &AppState) -> Result<AgentProcess> {
    let app = find_exe("wechat-summary-app");
    if !app.exists() {
        return Err(anyhow!("主程序不存在: {}", app.display()));
    }
    let mut command = Command::new(app);
    command
        .arg("--config")
        .arg(&state.config_path)
        .current_dir(&state.working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    hide_command_window(&mut command);

    let mut child = command.spawn().context("starting wechat-summary-app")?;
    let pid = child.id();
    let stdout = child
        .stdout
        .take()
        .context("capturing wechat-summary-app stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("capturing wechat-summary-app stderr")?;
    let (sender, output) = mpsc::channel();
    spawn_output_reader("stdout", stdout, sender.clone());
    spawn_output_reader("stderr", stderr, sender);

    Ok(AgentProcess { child, output, pid })
}

fn start_wxdb_init(state: &AppState) -> Result<WxdbInitProcess> {
    let wxdb = find_exe("wxdb");
    if !wxdb.exists() {
        return Err(anyhow!("wxdb 不存在: {}", wxdb.display()));
    }

    let mut command = Command::new(wxdb);
    command
        .arg("init")
        .current_dir(&state.working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    hide_command_window(&mut command);

    let mut child = command.spawn().context("starting wxdb init")?;
    let pid = child.id();
    let stdout = child.stdout.take().context("capturing wxdb init stdout")?;
    let stderr = child.stderr.take().context("capturing wxdb init stderr")?;
    let (sender, output) = mpsc::channel();
    spawn_output_reader("wxdb stdout", stdout, sender.clone());
    spawn_output_reader("wxdb stderr", stderr, sender);

    Ok(WxdbInitProcess { child, output, pid })
}

fn spawn_output_reader<R>(label: &'static str, reader: R, sender: mpsc::Sender<String>)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let text = line.trim_end_matches(&['\r', '\n'][..]);
                    let _ = sender.send(format!("[{label}] {text}\n"));
                }
                Err(error) => {
                    let _ = sender.send(format!("[gui] 读取 {label} 失败：{error}\n"));
                    break;
                }
            }
        }
    });
}

fn stop_existing_agent_processes() -> Result<Option<String>> {
    #[cfg(windows)]
    {
        let image_name = "wechat-summary-app.exe";
        if !windows_process_exists(image_name) {
            return Ok(None);
        }

        let mut command = Command::new("taskkill.exe");
        command
            .args(["/IM", image_name, "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        hide_command_window(&mut command);
        let output = command
            .output()
            .context("stopping existing wechat-summary-app.exe processes")?;
        if output.status.success() {
            return Ok(Some("已清理旧主程序实例，避免重复监听".to_string()));
        }

        if !windows_process_exists(image_name) {
            return Ok(None);
        }

        let code = output
            .status
            .code()
            .map_or_else(|| "unknown".to_string(), |code| code.to_string());
        bail!(
            "无法停止旧主程序 {image_name}，taskkill 退出码 {code}；请确认 GUI 已以管理员身份运行"
        );
    }

    #[cfg(not(windows))]
    {
        let output = Command::new("pkill")
            .args(["-f", "wechat-summary-app"])
            .output()
            .context("stopping existing wechat-summary-app processes")?;
        if output.status.success() {
            return Ok(Some("已清理旧主程序实例，避免重复监听".to_string()));
        }
        Ok(None)
    }
}

#[cfg(windows)]
fn windows_process_exists(image_name: &str) -> bool {
    let mut command = Command::new("tasklist.exe");
    command
        .args([
            "/FI",
            &format!("IMAGENAME eq {image_name}"),
            "/FO",
            "CSV",
            "/NH",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    hide_command_window(&mut command);
    let Ok(output) = command.output() else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    String::from_utf8_lossy(&output.stdout)
        .to_ascii_lowercase()
        .contains(&image_name.to_ascii_lowercase())
}

fn hide_command_window(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    {
        let _ = command;
    }
}

fn start_agent_elevated(state: &AppState) -> Result<()> {
    let app = find_exe("wechat-summary-app");
    if !app.exists() {
        return Err(anyhow!("主程序不存在: {}", app.display()));
    }

    #[cfg(windows)]
    {
        let mut command = Command::new("powershell.exe");
        command
            .arg("-NoProfile")
            .arg("-ExecutionPolicy")
            .arg("Bypass")
            .arg("-Command")
            .arg(
                "Start-Process -FilePath $args[0] -ArgumentList @('--config', $args[1]) -WorkingDirectory $args[2] -WindowStyle Hidden -Verb RunAs",
            )
            .arg(&app)
            .arg(&state.config_path)
            .arg(&state.working_dir);
        hide_command_window(&mut command);
        command
            .spawn()
            .context("starting wechat-summary-app as administrator")?;
        return Ok(());
    }

    #[cfg(not(windows))]
    {
        let _agent = start_agent(state)?;
        Ok(())
    }
}

fn install_runtime(state: &AppState) -> Result<()> {
    let script = state.working_dir.join("install.ps1");
    if !script.exists() {
        return Err(anyhow!("安装脚本不存在: {}", script.display()));
    }
    let mut command = Command::new("powershell.exe");
    command
        .arg("-NoProfile")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(&script)
        .current_dir(&state.working_dir);
    hide_command_window(&mut command);
    command.spawn().context("starting install.ps1")?;
    Ok(())
}

fn resolve_config_path(config_arg: Option<&Path>) -> Result<PathBuf> {
    let requested = config_arg.unwrap_or_else(|| Path::new("config/agent.toml"));
    if requested.is_absolute() {
        return Ok(requested.to_path_buf());
    }

    let cwd_candidate = env::current_dir()?.join(requested);
    if cwd_candidate.exists() {
        return Ok(cwd_candidate);
    }

    if let Some(exe_dir) = env::current_exe()?.parent().map(Path::to_path_buf) {
        let exe_candidate = exe_dir.join(requested);
        if exe_candidate.exists() {
            return Ok(exe_candidate);
        }
        if exe_dir.file_name().is_some_and(|name| name == "bin") {
            if let Some(root) = exe_dir.parent() {
                let root_candidate = root.join(requested);
                if root_candidate.exists() {
                    return Ok(root_candidate);
                }
            }
        }
    }

    Ok(cwd_candidate)
}

fn infer_working_dir(config_path: &Path) -> Result<PathBuf> {
    if config_path
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "config")
    {
        if let Some(root) = config_path.parent().and_then(Path::parent) {
            return Ok(root.to_path_buf());
        }
    }
    env::current_dir().context("reading current directory")
}

fn resolve_working_path(state: &AppState, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        state.working_dir.join(path)
    }
}

fn find_exe(name: &str) -> PathBuf {
    let exe_name = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    };
    if let Ok(current) = env::current_exe() {
        if let Some(dir) = current.parent() {
            for candidate in [
                dir.join(&exe_name),
                dir.join("bin").join(&exe_name),
                dir.parent()
                    .map(|root| root.join("bin").join(&exe_name))
                    .unwrap_or_else(|| dir.join(&exe_name)),
            ] {
                if candidate.exists() {
                    return candidate;
                }
            }
        }
    }
    PathBuf::from(exe_name)
}

fn tail_file(path: &Path, max_bytes: u64) -> Result<String> {
    let file = fs::File::open(path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(max_bytes);
    let mut reader = BufReader::new(file);
    if start > 0 {
        reader.seek_relative(start as i64)?;
    }
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn env_present(name: &str) -> bool {
    env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

fn env_or_direct_value_present(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        false
    } else if is_safe_env_var_name(trimmed) {
        env_present(trimmed)
    } else {
        true
    }
}

fn is_safe_env_var_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn redact_secret_like_tokens(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while let Some(relative) = input[index..].find("sk-") {
        let start = index + relative;
        output.push_str(&input[index..start]);
        let mut end = start + 3;
        for (offset, ch) in input[end..].char_indices() {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                end = start + 3 + offset + ch.len_utf8();
            } else {
                break;
            }
        }
        if end - start >= 12 {
            output.push_str("<redacted-secret>");
        } else {
            output.push_str(&input[start..end]);
        }
        index = end;
    }
    output.push_str(&input[index..]);
    output
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "OK"
    } else {
        "missing"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_semantic_colors_plain_log_levels() {
        assert_eq!(
            terminal_line_color("[stdout] 2026-06-05T12:00:00Z INFO wx4py_client: ready"),
            Some(egui::Color32::from_rgb(22, 163, 74))
        );
        assert_eq!(
            terminal_line_color("[stderr] 2026-06-05 20:00:00,000 - wx4py - WARNING - slow"),
            Some(egui::Color32::from_rgb(217, 119, 6))
        );
        assert_eq!(
            terminal_line_color("[stdout] ERROR wechat_summary_app: failed"),
            Some(egui::Color32::from_rgb(220, 38, 38))
        );
        assert_eq!(
            terminal_line_color("[gui] 主程序已启动"),
            Some(egui::Color32::from_rgb(37, 99, 235))
        );
    }

    #[test]
    fn terminal_semantic_colors_strip_ansi_before_matching() {
        let line = "\u{1b}[2m2026-06-05T12:00:00Z\u{1b}[0m \u{1b}[32mINFO\u{1b}[0m app";
        assert_eq!(strip_ansi_sgr(line), "2026-06-05T12:00:00Z INFO app");
        assert_eq!(
            terminal_line_color(&strip_ansi_sgr(line)),
            Some(egui::Color32::from_rgb(22, 163, 74))
        );
    }
}
