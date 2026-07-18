fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prefer system `protoc` when present; otherwise use the vendored binary so
    // developers/CI do not need a Homebrew/apt install.
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(path) = protoc_bin_vendored::protoc_bin_path() {
            // SAFETY: build script is single-threaded before compile.
            unsafe { std::env::set_var("PROTOC", path) };
        }
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/multiraft.proto"], &["proto"])?;
    Ok(())
}
