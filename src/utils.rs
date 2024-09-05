use std::io;

use ic_cdk::api::stable::{stable64_read, stable64_size};

pub fn read(buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
    if stable64_size() > 0 {
        stable64_read(offset + 8, buf);
    }
    Result::Ok(())
}