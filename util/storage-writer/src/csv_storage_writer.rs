// Copyright 2015-2018 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::io;
use std::fs;
use std::fs::File;
use std::sync::{Arc, Mutex};

use super::StorageWriter;
use csv::Writer;
use dir::helpers::replace_home;
use dir::default_data_path;
use ethereum_types::{Address, H256};

#[derive(Clone)]
pub struct CsvStorageWriter {
    watched_contracts: Vec<Address>,
    writer: Arc<Mutex<Writer<File>>>
}

impl CsvStorageWriter {
    pub fn new(watched_contracts: Vec<Address>) -> CsvStorageWriter {
        let path = replace_home(&default_data_path(), "$BASE/watched_storage");

        // TODO: handle error differently on file creation
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .expect("Error creating csv file.");

        let wtr = csv::Writer::from_writer(file);

        CsvStorageWriter {
            watched_contracts: watched_contracts,
            writer: Arc::new(Mutex::new(wtr))
        }
    }

    fn write_storage_node(&mut self, contract: Address, block_hash: H256, block_number: u64, key: H256, value: H256) -> io::Result<()> {
        let mut wtr = self.writer.lock().unwrap();
        wtr.write_record(&[format!("{:x}", contract), format!("{:x}", block_hash), format!("{}", block_number), format!("{:x}", key), format!("{:x}", value)])?;
        wtr.flush()?;
        Ok(())
    }
}

impl StorageWriter for CsvStorageWriter {
    fn boxed_clone(&self) -> Box<StorageWriter> {
        Box::new(CsvStorageWriter::new(self.watched_contracts.to_vec()))
    }

    fn enabled(&self) -> bool {
        true
    }

    fn write_storage_diffs(&mut self, header_hash: H256, header_number: u64, accounts_storage_diffs: HashMap<Address, HashMap<H256, H256>>) -> io::Result<()> {
        for (addr, diffs) in accounts_storage_diffs {
            if self.watched_contracts.contains(&addr) {
                for (k, v) in diffs {
                    self.write_storage_node(addr, header_hash, header_number, k, v)?;
                }
            }
        }
        Ok(())
    }
}

