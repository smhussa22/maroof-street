fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic-build compiles the proto and generates Rust code at build time
    tonic_build::configure()
        .build_server(false)
        .compile(
            &["../../../proto/orders.proto"],
            &["../../../proto"],
        )?;
    Ok(())
}
