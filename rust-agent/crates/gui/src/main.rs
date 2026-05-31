#![cfg_attr(windows, windows_subsystem = "windows")]

use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use eframe::egui;
use toml_edit::{value, Array, DocumentMut, Item, Table};
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
    llm_model_env: String,
    llm_timeout: u64,
    llm_max_tokens: u32,
    image_enabled: bool,
    image_provider: String,
    image_api_key_env: String,
    image_base_url_env: String,
    image_model_env: String,
    image_size: String,
    image_resolution: String,
    image_timeout: u64,
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
    terminal_output: String,
    agent: Option<AgentProcess>,
    status: StatusView,
    tab: Tab,
    message: Option<String>,
}

struct AgentProcess {
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
            terminal_output: "GUI 已就绪，主程序终端输出会显示在这里。\n".to_string(),
            agent: None,
            status: StatusView::default(),
            tab: Tab::Dashboard,
            message: None,
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
        self.log_tail = tail_file(&log_path, 16 * 1024)
            .map(|text| redact_secret_like_tokens(&text))
            .unwrap_or_else(|_| "暂无日志".to_string());
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
        if self.agent.is_some() {
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
                egui::Frame::group(ui.style()).show(ui, |ui| model_tab(ui, &mut self.view));
            }
            Tab::Runtime => {
                egui::Frame::group(ui.style()).show(ui, |ui| {
                    runtime_tab(
                        ui,
                        &mut self.view,
                        &mut self.log_tail,
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
    two_columns(
        ui,
        |ui| {
            text_field(ui, "LLM Provider", &mut view.llm_provider);
            text_field(ui, "LLM API Key 环境变量/直接值", &mut view.llm_api_key_env);
            text_field(
                ui,
                "LLM Base URL 环境变量/直接值",
                &mut view.llm_base_url_env,
            );
            text_field(ui, "LLM Model 环境变量/直接值", &mut view.llm_model_env);
            number_u64(ui, "LLM 超时秒数", &mut view.llm_timeout);
            number_u32(ui, "最大输出 Token", &mut view.llm_max_tokens);
        },
        |ui| {
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
        },
    );
}

fn runtime_tab(
    ui: &mut egui::Ui,
    view: &mut ConfigView,
    log_tail: &mut String,
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
    two_columns(
        ui,
        |ui| {
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
                310.0,
                true,
            );
        },
        |ui| {
            ui.label("日志文件尾部");
            readonly_scroll_text(
                ui,
                "log-tail-scroll",
                "log-tail-text",
                log_tail,
                310.0,
                false,
            );
        },
    );
}

fn readonly_scroll_text(
    ui: &mut egui::Ui,
    scroll_id: &'static str,
    label_id: &'static str,
    text: &str,
    height: f32,
    stick_to_bottom: bool,
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
                ui.add(
                    egui::Label::new(egui::RichText::new(text).monospace())
                        .wrap()
                        .selectable(true),
                );
            });
        });
    });
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
            llm_model_env: config.llm.model_env,
            llm_timeout: config.llm.timeout_seconds,
            llm_max_tokens: config.llm.max_output_tokens,
            image_enabled: config.image_gen.enabled,
            image_provider: config.image_gen.provider,
            image_api_key_env: config.image_gen.api_key_env,
            image_base_url_env: config.image_gen.base_url_env,
            image_model_env: config.image_gen.model_env,
            image_size: config.image_gen.size,
            image_resolution: config.image_gen.resolution.unwrap_or_default(),
            image_timeout: config.image_gen.timeout_seconds,
            runtime_output_dir: config.runtime.output_dir,
            runtime_log_level: config.runtime.log_level,
            runtime_cleanup_days: config.runtime.cleanup_after_days,
        };
    }

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
        llm_model_env: get_str(doc, "llm", "model_env", "LLM_MODEL"),
        llm_timeout: get_u64(doc, "llm", "timeout_seconds", 120),
        llm_max_tokens: get_u64(doc, "llm", "max_output_tokens", 2000) as u32,
        image_enabled: get_bool(doc, "image_gen", "enabled", true),
        image_provider: get_str(doc, "image_gen", "provider", "openai"),
        image_api_key_env: get_str(doc, "image_gen", "api_key_env", "IMAGE_API_KEY"),
        image_base_url_env: get_str(doc, "image_gen", "base_url_env", "IMAGE_BASE_URL"),
        image_model_env: get_str(doc, "image_gen", "model_env", "IMAGE_MODEL"),
        image_size: get_str(doc, "image_gen", "size", "2:3"),
        image_resolution: get_str(doc, "image_gen", "resolution", "1k"),
        image_timeout: get_u64(doc, "image_gen", "timeout_seconds", 300),
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

    remove_key(table_mut(&mut doc, "privacy"), "max_messages_to_llm");

    let llm = table_mut(&mut doc, "llm");
    set_str(llm, "provider", &update.llm_provider);
    set_str(llm, "api_key_env", &update.llm_api_key_env);
    set_str(llm, "base_url_env", &update.llm_base_url_env);
    set_str(llm, "model_env", &update.llm_model_env);
    set_int(llm, "timeout_seconds", update.llm_timeout as i64);
    set_int(llm, "max_output_tokens", update.llm_max_tokens as i64);

    let image_gen = table_mut(&mut doc, "image_gen");
    set_bool(image_gen, "enabled", update.image_enabled);
    set_str(image_gen, "provider", &update.image_provider);
    set_str(image_gen, "api_key_env", &update.image_api_key_env);
    set_str(image_gen, "base_url_env", &update.image_base_url_env);
    set_str(image_gen, "model_env", &update.image_model_env);
    set_str(image_gen, "size", &update.image_size);
    set_str(image_gen, "resolution", &update.image_resolution);
    set_int(image_gen, "timeout_seconds", update.image_timeout as i64);

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
    let mut text = String::new();
    reader.read_to_string(&mut text)?;
    Ok(text)
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
