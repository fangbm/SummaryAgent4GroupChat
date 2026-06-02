use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("timestamp parse error: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("state store mutex is poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct SqliteStateStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, StorageError> {
        let store = Self {
            conn: Arc::new(Mutex::new(Connection::open_in_memory()?)),
        };
        store.init()?;
        Ok(store)
    }

    pub fn get_last_trigger(&self, room_id: &str) -> Result<Option<DateTime<Utc>>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let value = conn
            .query_row(
                "select last_trigger_at from room_state where room_id = ?1",
                params![room_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        value
            .map(|text| Ok(DateTime::parse_from_rfc3339(&text)?.with_timezone(&Utc)))
            .transpose()
    }

    pub fn get_last_image(&self, room_id: &str) -> Result<Option<DateTime<Utc>>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let value = conn
            .query_row(
                "select last_image_at from room_state where room_id = ?1",
                params![room_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();

        value
            .map(|text| Ok(DateTime::parse_from_rfc3339(&text)?.with_timezone(&Utc)))
            .transpose()
    }

    pub fn set_last_trigger(
        &self,
        room_id: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute(
            r#"
            insert into room_state(room_id, last_trigger_at)
            values (?1, ?2)
            on conflict(room_id) do update set last_trigger_at = excluded.last_trigger_at
            "#,
            params![room_id, timestamp.to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn set_last_image(
        &self,
        room_id: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute(
            r#"
            insert into room_state(room_id, last_trigger_at, last_image_at)
            values (?1, ?2, ?2)
            on conflict(room_id) do update set last_image_at = excluded.last_image_at
            "#,
            params![room_id, timestamp.to_rfc3339()],
        )?;
        Ok(())
    }

    fn init(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute_batch(
            r#"
            create table if not exists room_state (
                room_id text primary key,
                last_trigger_at text not null,
                last_image_at text
            );
            "#,
        )?;
        let has_last_image_at = {
            let mut stmt = conn.prepare("pragma table_info(room_state)")?;
            let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
            let mut found = false;
            for column in columns {
                if column? == "last_image_at" {
                    found = true;
                    break;
                }
            }
            found
        };
        if !has_last_image_at {
            conn.execute_batch("alter table room_state add column last_image_at text;")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn persists_last_trigger() {
        let store = SqliteStateStore::in_memory().unwrap();
        let ts = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        store.set_last_trigger("room@chatroom", ts).unwrap();
        assert_eq!(store.get_last_trigger("room@chatroom").unwrap(), Some(ts));
    }

    #[test]
    fn persists_last_image() {
        let store = SqliteStateStore::in_memory().unwrap();
        let ts = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        store.set_last_image("room@chatroom", ts).unwrap();
        assert_eq!(store.get_last_image("room@chatroom").unwrap(), Some(ts));
    }
}
