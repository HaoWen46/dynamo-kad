use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // CARGO_MANIFEST_DIR points to crates/proto/
    // specs are at ../../specs/v1 (repo root)
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto_dir = manifest_dir.join("..").join("..").join("specs").join("v1");

    let proto_dir = proto_dir.canonicalize()?;

    let protos: Vec<PathBuf> = ["common.proto", "kad.proto", "kv.proto", "admin.proto"]
        .iter()
        .map(|f| proto_dir.join(f))
        .collect();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &protos.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
            &[proto_dir.as_path()],
        )?;

    Ok(())
}
