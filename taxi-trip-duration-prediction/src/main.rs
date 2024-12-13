use anyhow::Result;
use taxi_trip_duration_prediction::{
    download_raw_data,
    // scrape_links_to_parquet_files,
    // download_parquet_file
    // Percentage,
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Let's start scraping!");

    // download raw data
    let url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page";
    let pattern = "yellow_tripdata";
    let output_dir = download_raw_data(url, Some(pattern)).await?;

    // load, filter and compact into single file
    // let percentage = Percentage::new(0.01)?;

    // train xgboost model

    Ok(())
}
