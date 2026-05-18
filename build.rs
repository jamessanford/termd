fn has_utempter() -> bool {
    if pkg_config::probe_library("libutempter").is_ok() {
        return true;
    }
    if let Ok(out) = std::process::Command::new("ldconfig").arg("-p").output() {
        if String::from_utf8_lossy(&out.stdout).contains("libutempter") {
            println!("cargo:rustc-link-lib=utempter");
            return true;
        }
    }
    false
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/terminal.proto"], &["proto"])?;

    if has_utempter() {
        println!("cargo:rustc-cfg=has_utempter");
    }

    Ok(())
}
