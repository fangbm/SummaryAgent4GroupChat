fn main() {
    if std::env::var("CARGO_CFG_WINDOWS").is_ok() {
        println!("cargo:rustc-link-arg-bin=wechat-summary-gui=/MANIFEST:EMBED");
        println!(
            "cargo:rustc-link-arg-bin=wechat-summary-gui=/MANIFESTUAC:level='requireAdministrator' uiAccess='false'"
        );
    }
}
