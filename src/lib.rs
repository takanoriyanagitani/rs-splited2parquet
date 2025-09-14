use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::TryStreamExt;

use tokio::io::AsyncWrite;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::StringArray;
use arrow::array::StringBuilder;

use arrow::record_batch::RecordBatch;

use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;

pub fn builders2batch(
    sch: Arc<Schema>,
    bldrs: &mut [StringBuilder],
) -> Result<RecordBatch, io::Error> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(bldrs.len());
    for bldr in bldrs.iter_mut() {
        let sa: StringArray = bldr.finish();
        arrays.push(Arc::new(sa));
    }
    RecordBatch::try_new(sch.clone(), arrays).map_err(io::Error::other)
}

pub fn lines2splited2batch<L>(
    sch: Arc<Schema>,
    mut lines: L,
    delim: char,
    bsize: usize,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, io::Error>>>>
where
    L: Unpin + Stream<Item = Result<String, io::Error>> + 'static,
{
    let s: &Schema = &sch;
    let fields: &Fields = &s.fields;
    let sz: usize = fields.len();

    let strm = async_stream::try_stream! {
        (0 < sz).then_some(()).ok_or("no columns defined").map_err(io::Error::other)?;

        let mut builders: Vec<StringBuilder> = (0..sz).map(|_| StringBuilder::new()).collect();

        loop {
            for _ in 0..bsize {
                let oline: Option<String> = lines.try_next().await?;
                if oline.is_none() && builders[0].is_empty(){
                    return
                }

                if oline.is_none(){
                    let rb = builders2batch(sch.clone(), &mut builders)?;
                    yield rb;
                    return
                }

                let line: String = oline.ok_or("must not be none").map_err(io::Error::other)?;

                let mut splited = line.splitn(sz, delim);
                for bldr in builders.iter_mut(){
                    let col: &str = splited.next().ok_or("column missing").map_err(io::Error::other)?;
                    bldr.append_value(col);
                }
            }

            if builders[0].is_empty(){
                return
            }

            let rb = builders2batch(sch.clone(), &mut builders)?;
            yield rb;
        }
    };
    Box::pin(strm)
}

pub async fn batch2parquet<W>(
    b: &RecordBatch,
    wtr: &mut AsyncArrowWriter<W>,
) -> Result<(), io::Error>
where
    W: Unpin + Send + tokio::io::AsyncWrite,
{
    wtr.write(b).await.map_err(io::Error::other)
}

pub async fn write_all<B, W>(b: B, wtr: &mut AsyncArrowWriter<W>) -> Result<(), io::Error>
where
    B: Stream<Item = Result<RecordBatch, io::Error>>,
    W: Unpin + Send + tokio::io::AsyncWrite,
{
    b.try_fold(wtr, |wtr, next| async move {
        let rb: &RecordBatch = &next;
        batch2parquet(rb, wtr).await?;
        Ok(wtr)
    })
    .await
    .map(|_| ())
}

pub async fn lines2splited2batch2parquet<L, W>(
    sch: Arc<Schema>,
    lines: L,
    delim: char,
    bsize: usize,
    wtr: &mut AsyncArrowWriter<W>,
) -> Result<(), io::Error>
where
    L: Unpin + Stream<Item = Result<String, io::Error>> + 'static,
    W: Unpin + Send + tokio::io::AsyncWrite,
{
    let strm = lines2splited2batch(sch, lines, delim, bsize);
    write_all(strm, wtr).await
}

/// Writes a parquet from the splited lines using the opts.
pub async fn opts2lines2splited2batch2parquet<L, W>(
    sch: Arc<Schema>,
    lines: L,
    delim: char,
    bsize: usize,
    wtr: W,
    opts: Option<WriterProperties>,
) -> Result<W, io::Error>
where
    L: Unpin + Stream<Item = Result<String, io::Error>> + 'static,
    W: Unpin + Send + AsyncWrite,
{
    let mut aw = AsyncArrowWriter::try_new(wtr, sch.clone(), opts)?;
    lines2splited2batch2parquet(sch, lines, delim, bsize, &mut aw).await?;
    aw.flush().await?;
    aw.finish().await?;
    Ok(aw.into_inner())
}

pub fn fields2schema(v: Vec<Field>) -> Schema {
    Schema::new(v)
}

pub fn colindex2field(colix: usize, nullable: bool) -> Field {
    let name = format!("column_{colix}");
    Field::new(name, DataType::Utf8, nullable)
}

pub fn colsz2schema(colsz: usize, nullable: bool) -> Schema {
    let v: Vec<Field> = (0..colsz).map(|sz| colindex2field(sz, nullable)).collect();
    fields2schema(v)
}

pub const NULLABLE_DEFAULT: bool = true;
pub const DELIM_DEFAULT: char = ',';
pub const BATSZ_DEFAULT: usize = 1024;

/// Writes a parquet from the splited stdin using the opts.
pub async fn opts2stdin2lines2splited2batch2parquet2stdout(
    colsz: usize,
    nullable: bool,
    delim: char,
    bsize: usize,
    opts: Option<WriterProperties>,
) -> Result<(), io::Error> {
    let i = tokio::io::stdin();
    let bi = tokio::io::BufReader::new(i);
    let lines = tokio::io::AsyncBufReadExt::lines(bi);
    let lstrm = tokio_stream::wrappers::LinesStream::new(lines);

    let sch: Schema = colsz2schema(colsz, nullable);

    let mut o = tokio::io::stdout();
    let bw = tokio::io::BufWriter::new(&mut o);
    let mut bw =
        opts2lines2splited2batch2parquet(sch.into(), lstrm, delim, bsize, bw, opts).await?;
    tokio::io::AsyncWriteExt::flush(&mut bw).await?;
    tokio::io::AsyncWriteExt::flush(&mut o).await?;
    Ok(())
}

pub async fn opts2stdin2lines2splited2batch2parquet2stdout_default(
    colsz: usize,
) -> Result<(), io::Error> {
    opts2stdin2lines2splited2batch2parquet2stdout(
        colsz,
        NULLABLE_DEFAULT,
        DELIM_DEFAULT,
        BATSZ_DEFAULT,
        None,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::io;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use futures::stream;

    use super::*;

    fn vec_to_stream(
        v: Vec<Result<String, io::Error>>,
    ) -> impl Stream<Item = Result<String, io::Error>> {
        stream::iter(v)
    }

    fn simple_schema() -> Arc<Schema> {
        let fields = vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Utf8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_basic_split_and_batch() {
        let lines = vec![
            Ok("a1,b1".to_string()),
            Ok("a2,b2".to_string()),
            Ok("a3,b3".to_string()),
            Ok("a4,b4".to_string()),
            Ok("a5,b5".to_string()),
        ];

        let stream = lines2splited2batch(simple_schema(), vec_to_stream(lines), ',', 3);

        let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>())
            .expect("stream should produce record batches");

        assert_eq!(batches.len(), 2);

        let rb1 = &batches[0];
        assert_eq!(rb1.num_columns(), 2);
        assert_eq!(rb1.num_rows(), 3);

        let col1 = rb1
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col1 should be a StringArray");
        assert_eq!(col1.value(0), "a1");
        assert_eq!(col1.value(1), "a2");
        assert_eq!(col1.value(2), "a3");

        let col2 = rb1
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col2 should be a StringArray");
        assert_eq!(col2.value(0), "b1");
        assert_eq!(col2.value(1), "b2");
        assert_eq!(col2.value(2), "b3");

        let rb2 = &batches[1];
        assert_eq!(rb2.num_columns(), 2);
        assert_eq!(rb2.num_rows(), 2);

        let col1 = rb2
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col1 should be a StringArray");
        assert_eq!(col1.value(0), "a4");
        assert_eq!(col1.value(1), "a5");

        let col2 = rb2
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col2 should be a StringArray");
        assert_eq!(col2.value(0), "b4");
        assert_eq!(col2.value(1), "b5");
    }

    #[test]
    fn test_empty_input() {
        let lines: Vec<Result<String, io::Error>> = vec![];

        let stream = lines2splited2batch(simple_schema(), vec_to_stream(lines), ',', 3);

        let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>())
            .expect("stream should produce record batches");

        assert!(batches.is_empty(), "empty input → no batches");
    }

    #[test]
    fn test_missing_column_error() {
        let lines = vec![Ok("only_one_column".to_string())];

        let stream = lines2splited2batch(simple_schema(), vec_to_stream(lines), ',', 3);

        let res = futures::executor::block_on(stream.try_collect::<Vec<_>>());

        assert!(
            res.is_err(),
            "stream should error out when a column is missing"
        );

        let err = res.unwrap_err();
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("column missing"),
            "error message should mention the missing column: {msg}"
        );
    }

    #[test]
    fn test_delimiter_variation() {
        let lines = vec![Ok("foo;bar".to_string()), Ok("baz;qux".to_string())];

        let stream = lines2splited2batch(simple_schema(), vec_to_stream(lines), ';', 10);

        let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>())
            .expect("stream should produce record batches");

        assert_eq!(batches.len(), 1);
        let rb = &batches[0];
        assert_eq!(rb.num_rows(), 2);

        let col1 = rb
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col1 should be a StringArray");
        let col2 = rb
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col2 should be a StringArray");

        assert_eq!(col1.value(0), "foo");
        assert_eq!(col1.value(1), "baz");
        assert_eq!(col2.value(0), "bar");
        assert_eq!(col2.value(1), "qux");
    }
}
