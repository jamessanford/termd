fn has_utempter() -> bool {
    if pkg_config::probe_library("libutempter").is_ok() {
        return true;
    }
    #[cfg(target_os = "linux")]
    if let Ok(out) = std::process::Command::new("ldconfig").arg("-p").output() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        // Each line: "    libutempter.so.0 (libc6,x86-64) => /path/to/libutempter.so.0"
        if let Some(line) = stdout.lines().find(|l| l.contains("libutempter.so")) {
            if let Some(path) = line.split("=>").nth(1).map(str::trim) {
                if let Some(dir) = std::path::Path::new(path).parent() {
                    println!("cargo:rustc-link-search=native={}", dir.display());
                }
            }
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

    println!("cargo::rustc-check-cfg=cfg(has_utempter)");

    if has_utempter() {
        println!("cargo:rustc-cfg=has_utempter");
    }

    Ok(())
}
