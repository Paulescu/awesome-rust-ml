use anyhow::Result;
use taxi_trip_duration_prediction::{download_raw_data, merge_and_clean_raw_files};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Let's start scraping!");

    // download raw data
    let raw_data_dir = download_raw_data().await?;

    // TODO: load and filter data
    let clean_data_path = merge_and_clean_raw_files(raw_data_dir).await?;

    // TODO: Train xgboost model
    

    // TODO: Push model to the registry

    Ok(())
}
