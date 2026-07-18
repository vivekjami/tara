use anyhow::Result;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_telemetry(service_name: &str) -> Result<()> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_resource(Resource::builder().with_service_name(service_name.to_string()).build())
        .with_batch_exporter(exporter)
        .build();

    global::set_tracer_provider(provider.clone());

    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer(service_name.to_string()));

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .try_init()?;

    Ok(())
}