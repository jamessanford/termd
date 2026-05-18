fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/terminal.proto"], &["proto"])?;

    if pkg_config::probe_library("libutempter").is_ok() {
        println!("cargo:rustc-cfg=has_utempter");
    }

    Ok(())
}
