use std::io::{self};

use crate::utils::read;

#[cfg(feature = "backup")]
/// Streams a backup of the database as uncompressed data.
///
/// This function retrieves all pages of the database and accumulates them into a
/// `Vec<u8>`. It returns the accumulated data wrapped in an `io::Cursor` for streaming.
///
/// Be aware that this function will try to load the entire database into memory
/// all at once, which may not be feasible for very large databases and could lead
/// to high memory usage or out-of-memory errors. It is particularly useful for
/// cases where you need to stream the database backup without compressing it.
///
/// # Returns
///
/// Returns an `io::Result<impl Read>` where the `impl Read` is an `io::Cursor` over a
/// `Vec<u8>` containing the uncompressed database data. This allows for streaming the
/// data as needed.
///
/// # Errors
///
/// This function returns an `io::Error` if:
/// - The database connection is not properly initialized or locked.
/// - The transaction cannot be started or committed.
/// - Page data cannot be read from the virtual file system or written to the buffer.
///
/// # Panics
///
/// This function may panic if:
/// - The transaction cannot be started or committed.
/// - Page data cannot be read or written to the buffer.
///
/// # Example
///
/// ```
/// use std::io::Read;
/// use ic_sqlite_features::stream_db_backup;
///
/// let mut backup_stream = stream_db_backup().expect("Failed to create backup stream");
/// let mut buffer = Vec::new();
/// backup_stream.read_to_end(&mut buffer).expect("Failed to read backup data");
/// // Use `buffer` as needed
/// ```
pub fn stream_db_backup() -> Result<impl Read, io::Error> {
    let mut conn = CONN.lock().unwrap();

    // Begin a transaction to ensure consistency
    let tx = conn.transaction().unwrap();

    // Create a buffer to store uncompressed data
    let mut buffer = Vec::new();

    let page_count: i64 = tx
        .query_row("PRAGMA page_count;", [], |row| row.get(0))
        .unwrap();

    for page_number in 1..=page_count {
        let page_data = read_page_from_vfs(page_number, 4096)?;
        buffer.write_all(&page_data)?;
    }

    // Commit the transaction
    tx.commit().unwrap();

    // Return the uncompressed data as a cursor for streaming
    Ok(io::Cursor::new(buffer))
}

#[cfg(feature = "backup")]
/// Performs a backup of the database by loading it entirely into memory.
///
/// This function retrieves all pages of the database and accumulates them into a
/// `Vec<u8>`. Be aware that this will try to bring the entire database into memory
/// all at once, which may not be feasible for very large databases and could lead
/// to high memory usage or out-of-memory errors.
///
/// # Returns
///
/// Returns a `Vec<u8>` containing the entire database's raw data. The data is
/// concatenated from each page of the database.
///
/// # Panics
///
/// This function may panic if:
/// - The database connection is not properly initialized or locked.
/// - The transaction cannot be started or committed.
/// - Page data cannot be read from the virtual file system.
///
/// # Example
///
/// ```
/// use ic_sqlite_features::backup::db_backup_on_memory;
/// let backup_data = db_backup_on_memory();
/// // Use `backup_data` as needed.
/// ```
pub fn db_backup_on_memory() -> Vec<u8> {
    use crate::CONN;

    let mut conn = CONN.lock().unwrap();
    let mut output = Vec::new();

    let tx = conn.transaction().unwrap();

    let page_count: i64 = tx
        .query_row("PRAGMA page_count;", [], |row| row.get(0))
        .unwrap();
    for page_number in 1..=page_count {
        let page_data = read_page_from_vfs(page_number, 4096).unwrap();
        output.extend_from_slice(&page_data);
    }

    tx.commit().unwrap();

    output
}

pub fn read_page_from_vfs(page_number: i64, page_size: usize) -> Result<Vec<u8>, io::Error> {
    let offset = (page_number - 1) * page_size as i64;

    let mut buffer = vec![0u8; page_size];

    read(&mut buffer, offset as u64)?;

    Result::Ok(buffer)
}