use anyhow::Result;

fn main() -> Result<()> {
    tara_store::telemetry::init_telemetry("tara")?;
    tracing::info!("tara-server started");
    println!("tara-server is running");
    Ok(())
}
