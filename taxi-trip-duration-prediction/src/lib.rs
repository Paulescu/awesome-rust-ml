use anyhow::Result;
use futures::future::join_all;
use std::path::Path;
use polars::prelude::*;

pub async fn download_raw_data() -> Result<String> {
    
    // url and pattern to match yellow_tripdata taxi data
    let url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page";
    let pattern = Some("yellow_tripdata_2022-");

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
    println!("Start downloading {}...", output_path.display());
    let response = reqwest::get(link).await?;
    let bytes = response.bytes().await?;

    // Write bytes to disk
    tokio::fs::write(&output_path, bytes).await?;

    println!("{} completed!", output_path.display());

    Ok(())
}

pub async fn merge_and_clean_raw_files(
    input_dir: String,
) -> Result<String> {
    // Get list of raw files to process
    let mut entries = tokio::fs::read_dir(input_dir).await?;
    let mut paths = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            paths.push(entry.path());
        }
    }
    println!("{} raw files to process", paths.len());

    // load files async, filter rows, compact into single fil
    // let compact_file = tokio::fs::File::create(compact_file_path).await?;

    let mut dataframes = Vec::new();
    for path in paths {
        println!("Processing file: {}", path.display());
        let lazy_df = LazyFrame::scan_parquet(
            path, ScanArgsParquet::default())?
            .select([
                col("tpep_pickup_datetime").cast(DataType::Date),
                col("tpep_dropoff_datetime").cast(DataType::Date),
                col("PULocationID").cast(DataType::Int64),
                col("DOLocationID").cast(DataType::Int64),
            ]);
        
        // append this datafame vertically
        dataframes.push(lazy_df);
    }

    println!("Merging {} dataframes", dataframes.len());
    let mut merged_df = concat(
        &dataframes,
        UnionArgs::default()
    )?.collect()?;

    // size of the merged dataframe
    // println!("Merged dataframe size: {} rows", merged_df.height());
    // shape of the merged dataframe
    // println!("Merged dataframe shape: {:?}", merged_df.shape());
    let (rows, cols) = merged_df.shape();
    println!("Merged dataframe shape: {} rows, {} cols", rows, cols);

    // save the merged dataframe to disk
    let file_path = "final.parquet";
    use polars::io::parquet::ParquetWriter;

    // Then change the write line to:
    ParquetWriter::new(
        std::fs::File::create(file_path)?
    ).finish(&mut merged_df)?;
    
    Ok(file_path.to_string())
}
