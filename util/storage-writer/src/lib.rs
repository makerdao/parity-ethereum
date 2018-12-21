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

//! Storage writing utils

extern crate csv;
extern crate dir;
extern crate ethereum_types;
extern crate vm;

use std::io;
use std::{fmt, str};

mod csv_storage_writer;
mod noop;
mod pg;

use ethereum_types::{H256, Address};

/// Something that can write storage values to disk
pub trait StorageWriter: Send + Sync {
    /// Return a copy of ourself, in a box.
    fn boxed_clone(&self) -> Box<StorageWriter>;

    /// Whether storage writing is enabled
    fn enabled(&self) -> bool;

    /// Write storage node to disk
    fn write_storage_node(&mut self, contract: Address, block_hash: H256, block_number: u64, key: H256, value: H256) -> io::Result<()>;
}

impl Clone for Box<StorageWriter> {
    fn clone(&self) -> Box<StorageWriter> {
        self.boxed_clone()
    }
}

/// Create a new `StorageWriter` trait object.
pub fn new(database: Database) -> Box<StorageWriter> {
    match database {
        Database::Csv => Box::new(csv_storage_writer::CsvStorageWriter::new()),
        Database::None => Box::new(noop::NoopStorageWriter::new()),
        Database::Postgres => Box::new(pg::PostgresStorageWriter::new()),
    }
}


// Storage writer database.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Database {
    /// CSV file
    Csv,
    /// No Storage Writer
    None,
    /// Postgres
    Postgres,

}

impl str::FromStr for Database {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(Database::Csv),
            "none" => Ok(Database::None),
            "postgres" => Ok(Database::Postgres),
            e => Err(format!("Invalid storage writing database: {}", e)),
        }
    }
}

impl Database {
    /// Returns static str describing database.
    pub fn as_str(&self) -> &'static str {
        match *self {
            Database::Csv => "csv",
            Database::None => "none",
            Database::Postgres => "postgres",
        }
    }

    /// Returns all algorithm types.
    pub fn all_types() -> Vec<Database> {
        vec![Database::Csv, Database::None, Database::Postgres]
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}


/// Configurtion for writing storage diffs from watched accounts
#[derive(PartialEq, Debug, Clone)]
pub struct StorageWriterConfig {
    /// Accounts to be watched
    pub watched_accounts: Vec<Address>,
    /// Database used for persisting account storage diffs
    pub database: Database,
}

impl Default for StorageWriterConfig {
    fn default() -> Self {
        StorageWriterConfig {
            watched_accounts: Vec::new(),
            database: Database::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    #[test]
    fn test_storage_writer_database_parsing() {
        assert_eq!(Database::Csv, "csv".parse().unwrap());
        assert_eq!(Database::None, "none".parse().unwrap());
        assert_eq!(Database::Postgres, "postgres".parse().unwrap());
    }

    #[test]
    fn test_storage_writer_database_printing() {
        assert_eq!(Database::Csv.to_string(), "csv".to_owned());
        assert_eq!(Database::None.to_string(), "none".to_owned());
        assert_eq!(Database::Postgres.to_string(), "postgres".to_owned());
    }

    #[test]
    fn test_storage_writer_database_all_types() {
        // compiling should fail if some cases are not covered
        let mut csv = 0;
        let mut none = 0;
        let mut postgres = 0;

        for db in &Database::all_types() {
            match *db {
                Database::Csv => csv += 1,
                Database::None => none += 1,
                Database::Postgres => postgres += 1,
            }
        }

        assert_eq!(csv, 1);
        assert_eq!(none, 1);
        assert_eq!(postgres, 1);
    }
}