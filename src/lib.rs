use anyhow::Result;
use futures::future::join_all;
use std::path::Path;

pub async fn download_raw_data(url: &str, pattern: Option<&str>) -> Result<String> {
    // scrape the links to the parquet files
    let links = scrape_links_to_parquet_files(url, pattern).await?;
    println!("{} parquet files found", links.len());

    // create the output directory
    use std::path::Path;
    let output_dir = Path::new("nyc_taxi_data");
    tokio::fs::create_dir_all(output_dir).await?;

    // create the vector of async tasks
    // each task downloads one parquet file
    let download_tasks: Vec<_> = links
        .iter()
        .map(|link| download_parquet_file(link, output_dir))
        .collect();

    // Run all download tasks concurrently
    join_all(download_tasks).await;

    Ok(output_dir.display().to_string())
}

/// Finds and returns all links to parquet files in the given `url`
/// https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
async fn scrape_links_to_parquet_files(url: &str, pattern: Option<&str>) -> Result<Vec<String>> {
    let response = reqwest::get(url).await?;
    let body = response.text().await?;

    // println!("{}", body);

    let links: Vec<String> = body
        .split("<a href=\"")
        .skip(1)
        .filter_map(|s| {
            s.split('"')
                .next()
                .filter(|s| s.contains(".parquet") && pattern.map_or(true, |p| s.contains(p)))
                .map(|s| s.to_string())
        })
        .collect();

    Ok(links)
}

/// Download the parquet file at `link` and saves it locally to `output_dir`
async fn download_parquet_file(link: &str, output_dir: &Path) -> Result<()> {
    // Filename
    let filename = link
        .split("/")
        .last()
        .ok_or_else(|| anyhow::anyhow!("Invalid link format"))?;

    // Full output path
    let output_path = output_dir.join(filename);

    // Exit if the file already exists
    if output_path.exists() {
        println!(
            "File {} already exists. Skip download",
            output_path.display()
        );
        return Ok(());
    }

    // Download the file
    println!("Downloading {}", output_path.display());

    let response = reqwest::get(link).await?;
    let bytes = response.bytes().await?;

    // Write bytes to disk
    tokio::fs::write(&output_path, bytes).await?;

    Ok(())
}

pub struct Percentage(f64);

impl Percentage {
    pub fn new(value: f64) -> Result<Self> {
        if value >= 0.0 && value <= 1.0 {
            Ok(Percentage(value))
        } else {
            Err(anyhow::anyhow!("Percentage must be between 0 and 1"))
        }
    }

    pub fn value(&self) -> f64 {
        self.0
    }
}

pub async fn merge_and_clean_raw_files(
    input_dir: &Path,
    percentage_rows_to_keep: Option<Percentage>,
) -> Result<String> {
    // Get list of raw files to process
    let mut entries = tokio::fs::read_dir(input_dir).await?;
    let mut files = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            files.push(entry.path());
        }
    }
    println!("{} raw files to process", files.len());

    Ok("my_path".to_string())
}
