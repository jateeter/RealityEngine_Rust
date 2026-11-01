#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    reality_engine::pe::serve().await
}
