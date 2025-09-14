use std::io;
use std::process::ExitCode;

use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use rs_splited2parquet::opts2stdin2lines2splited2batch2parquet2stdout;

fn env2delim() -> char {
    let o: String = std::env::var("ENV_DELIM")
        .ok()
        .unwrap_or_else(|| ",".into());
    let s: &str = &o;
    let mut chars = s.chars();
    chars.next().unwrap_or(',')
}

fn env2compression() -> Compression {
    let cs: String = std::env::var("ENV_COMPRESSION")
        .ok()
        .unwrap_or_else(|| "UNCOMPRESSED".into());
    str::parse(&cs).ok().unwrap_or(Compression::UNCOMPRESSED)
}

fn env2colsz() -> Result<usize, io::Error> {
    std::env::var("ENV_COLUMN_SIZE")
        .ok()
        .and_then(|s| str::parse(&s).ok())
        .ok_or("missing/invalid column size ENV_COLUMN_SIZE")
        .map_err(io::Error::other)
}

async fn sub() -> Result<(), io::Error> {
    let colsz: usize = env2colsz()?;
    let delim: char = env2delim();
    let comp: Compression = env2compression();
    let opts = WriterProperties::builder().set_compression(comp).build();
    opts2stdin2lines2splited2batch2parquet2stdout(colsz, true, delim, 1024, Some(opts)).await
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
