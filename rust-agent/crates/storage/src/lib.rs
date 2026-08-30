use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TaskState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl TaskState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Queued,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub id: String,
    pub room_id: String,
    pub source: String,
    pub state: TaskState,
    pub stage: String,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub summary: Option<String>,
    pub error: Option<String>,
    pub retry_of: Option<String>,
    pub config_revision: String,
    pub message_count: u64,
    pub media_count: u64,
}

#[derive(Debug, Clone)]
pub struct NewTask<'a> {
    pub room_id: &'a str,
    pub source: &'a str,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub config_revision: &'a str,
    pub retry_of: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeliveryState {
    Pending,
    Sending,
    Delivered,
    Failed,
    Uncertain,
    Cancelled,
}

impl DeliveryState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sending => "sending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::Uncertain => "uncertain",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "sending" => Self::Sending,
            "delivered" => Self::Delivered,
            "failed" => Self::Failed,
            "uncertain" => Self::Uncertain,
            "cancelled" => Self::Cancelled,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeliveryRecord {
    pub id: String,
    pub task_id: Option<String>,
    pub room_id: String,
    pub kind: String,
    pub payload: String,
    pub state: DeliveryState,
    pub attempts: u32,
    pub next_attempt_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
    pub idempotency_key: String,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SourceReference {
    pub task_id: String,
    pub point_index: u32,
    pub source_id: String,
    pub occurred_at: DateTime<Utc>,
    pub sender_label: String,
    pub message_index: u32,
}

#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub capability: String,
    pub provider_key: String,
    pub consecutive_failures: u32,
    pub circuit_open_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WeeklyMetric {
    pub report_id: String,
    pub group_name: String,
    pub room_id: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub summary: Option<String>,
    pub chart_path: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DailyUsage {
    pub tasks: u32,
    pub images: u32,
    pub media: u32,
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

    pub fn create_task(&self, task: NewTask<'_>) -> Result<TaskRecord, StorageError> {
        let now = Utc::now();
        let record = TaskRecord {
            id: Uuid::new_v4().to_string(),
            room_id: task.room_id.to_string(),
            source: task.source.to_string(),
            state: TaskState::Queued,
            stage: "queued".to_string(),
            since: task.since,
            until: task.until,
            created_at: now,
            started_at: None,
            completed_at: None,
            summary: None,
            error: None,
            retry_of: task.retry_of.map(ToOwned::to_owned),
            config_revision: task.config_revision.to_string(),
            message_count: 0,
            media_count: 0,
        };
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute(
            "insert into summary_task(id, room_id, source, state, stage, since_at, until_at, created_at, retry_of, config_revision, message_count, media_count) values(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,0)",
            params![record.id, record.room_id, record.source, record.state.as_str(), record.stage, record.since.to_rfc3339(), record.until.to_rfc3339(), record.created_at.to_rfc3339(), record.retry_of, record.config_revision],
        )?;
        Ok(record)
    }

    pub fn task(&self, id: &str) -> Result<Option<TaskRecord>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(conn.query_row("select id, room_id, source, state, stage, since_at, until_at, created_at, started_at, completed_at, summary, error, retry_of, config_revision, message_count, media_count from summary_task where id=?1", params![id], row_to_task)
            .optional()?)
    }

    pub fn tasks(
        &self,
        limit: usize,
        room_id: Option<&str>,
        state: Option<TaskState>,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select id, room_id, source, state, stage, since_at, until_at, created_at, started_at, completed_at, summary, error, retry_of, config_revision, message_count, media_count from summary_task where (?1 is null or room_id=?1) and (?2 is null or state=?2) order by created_at desc limit ?3")?;
        let rows = statement.query_map(
            params![room_id, state.map(TaskState::as_str), limit.max(1) as i64],
            row_to_task,
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn queued_retry_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select id, room_id, source, state, stage, since_at, until_at, created_at, started_at, completed_at, summary, error, retry_of, config_revision, message_count, media_count from summary_task where state='queued' and source='manual_retry' order by created_at limit ?1")?;
        let rows = statement.query_map(params![limit.max(1) as i64], row_to_task)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Recover only stages which cannot have reached the platform yet. Tasks
    /// which may have sent a message are deliberately left for an operator so
    /// a restart never turns an ambiguous delivery into group spam.
    pub fn recover_interrupted_tasks(&self) -> Result<(usize, usize), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let resumable = conn.execute(
            "update summary_task set state='queued', source='manual_retry', stage='recovered_after_restart', error='agent interrupted before delivery; resumed automatically' where state='running' and stage in ('accepted','history','input_preparation','text_summary')",
            [],
        )?;
        let requires_review = conn.execute(
            "update summary_task set state='failed', stage='interrupted_requires_retry', completed_at=?1, error='agent interrupted after summary output may have been prepared; use task-center retry after confirming delivery' where state='running'",
            params![Utc::now().to_rfc3339()],
        )?;
        Ok((resumable, requires_review))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_task(
        &self,
        id: &str,
        state: TaskState,
        stage: &str,
        summary: Option<&str>,
        error: Option<&str>,
        message_count: u64,
        media_count: u64,
    ) -> Result<(), StorageError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let finished = matches!(
            state,
            TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled
        );
        conn.execute("update summary_task set state=?2, stage=?3, started_at=case when ?2='running' and started_at is null then ?4 else started_at end, completed_at=case when ?5 then ?4 else completed_at end, summary=coalesce(?6, summary), error=?7, message_count=?8, media_count=?9 where id=?1", params![id, state.as_str(), stage, now, finished, summary, error, message_count as i64, media_count as i64])?;
        Ok(())
    }

    pub fn add_source_references(
        &self,
        references: &[SourceReference],
    ) -> Result<(), StorageError> {
        let mut conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = conn.transaction()?;
        for reference in references {
            transaction.execute("insert or replace into task_source_reference(task_id, point_index, source_id, occurred_at, sender_label, message_index) values(?1,?2,?3,?4,?5,?6)", params![reference.task_id, reference.point_index as i64, reference.source_id, reference.occurred_at.to_rfc3339(), reference.sender_label, reference.message_index as i64])?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn source_references(&self, task_id: &str) -> Result<Vec<SourceReference>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select task_id, point_index, source_id, occurred_at, sender_label, message_index from task_source_reference where task_id=?1 order by point_index, message_index")?;
        let rows = statement.query_map(params![task_id], |row| {
            Ok(SourceReference {
                task_id: row.get(0)?,
                point_index: row.get::<_, i64>(1)? as u32,
                source_id: row.get(2)?,
                occurred_at: parse_time(row.get::<_, String>(3)?)?,
                sender_label: row.get(4)?,
                message_index: row.get::<_, i64>(5)? as u32,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn enqueue_delivery(
        &self,
        task_id: Option<&str>,
        room_id: &str,
        kind: &str,
        payload: &str,
    ) -> Result<DeliveryRecord, StorageError> {
        let now = Utc::now();
        let idempotency_key = delivery_idempotency_key(task_id, room_id, kind, payload);
        let record = DeliveryRecord {
            id: Uuid::new_v4().to_string(),
            task_id: task_id.map(ToOwned::to_owned),
            room_id: room_id.to_string(),
            kind: kind.to_string(),
            payload: payload.to_string(),
            state: DeliveryState::Pending,
            attempts: 0,
            next_attempt_at: now,
            created_at: now,
            updated_at: now,
            error: None,
            idempotency_key: idempotency_key.clone(),
            lease_expires_at: None,
        };
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute("insert or ignore into outbox_delivery(id, task_id, room_id, kind, payload, state, attempts, next_attempt_at, created_at, updated_at, idempotency_key) values(?1,?2,?3,?4,?5,?6,0,?7,?7,?7,?8)", params![record.id, record.task_id, record.room_id, record.kind, record.payload, record.state.as_str(), record.next_attempt_at.to_rfc3339(), record.idempotency_key])?;
        Ok(conn.query_row(
            "select id, task_id, room_id, kind, payload, state, attempts, next_attempt_at, created_at, updated_at, error, idempotency_key, lease_expires_at from outbox_delivery where idempotency_key=?1",
            params![idempotency_key],
            row_to_delivery,
        )?)
    }

    pub fn due_deliveries(&self, limit: usize) -> Result<Vec<DeliveryRecord>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select id, task_id, room_id, kind, payload, state, attempts, next_attempt_at, created_at, updated_at, error, idempotency_key, lease_expires_at from outbox_delivery where state='pending' and next_attempt_at<=?1 order by created_at limit ?2")?;
        let rows = statement.query_map(
            params![Utc::now().to_rfc3339(), limit.max(1) as i64],
            row_to_delivery,
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn deliveries(&self, limit: usize) -> Result<Vec<DeliveryRecord>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select id, task_id, room_id, kind, payload, state, attempts, next_attempt_at, created_at, updated_at, error, idempotency_key, lease_expires_at from outbox_delivery order by created_at desc limit ?1")?;
        let rows = statement.query_map(params![limit.max(1) as i64], row_to_delivery)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn update_delivery(
        &self,
        id: &str,
        state: DeliveryState,
        attempts: u32,
        next_attempt_at: DateTime<Utc>,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute("update outbox_delivery set state=?2, attempts=?3, next_attempt_at=?4, updated_at=?5, error=?6, lease_expires_at=case when ?2='sending' then lease_expires_at else null end where id=?1", params![id, state.as_str(), attempts as i64, next_attempt_at.to_rfc3339(), Utc::now().to_rfc3339(), error])?;
        Ok(())
    }

    pub fn deliveries_for_task(&self, task_id: &str) -> Result<Vec<DeliveryRecord>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select id, task_id, room_id, kind, payload, state, attempts, next_attempt_at, created_at, updated_at, error, idempotency_key, lease_expires_at from outbox_delivery where task_id=?1 order by created_at")?;
        let rows = statement.query_map(params![task_id], row_to_delivery)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn claim_delivery(
        &self,
        id: &str,
        attempts: u32,
        lease_seconds: i64,
    ) -> Result<bool, StorageError> {
        let now = Utc::now();
        let lease = now + chrono::Duration::seconds(lease_seconds.max(1));
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(conn.execute(
            "update outbox_delivery set state='sending', attempts=?2, next_attempt_at=?3, updated_at=?3, lease_expires_at=?4 where id=?1 and state='pending'",
            params![id, attempts as i64, now.to_rfc3339(), lease.to_rfc3339()],
        )? == 1)
    }

    pub fn recover_interrupted_deliveries(&self) -> Result<usize, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute(
            "update outbox_delivery set state='uncertain', updated_at=?1, lease_expires_at=null, error=coalesce(error, 'agent interrupted while platform delivery was in progress; confirm before retrying') where state='sending'",
            params![Utc::now().to_rfc3339()],
        ).map_err(StorageError::from)
    }

    pub fn retry_delivery(&self, id: &str) -> Result<(), StorageError> {
        self.update_delivery(id, DeliveryState::Pending, 0, Utc::now(), None)
    }

    pub fn update_provider_health(
        &self,
        capability: &str,
        provider_key: &str,
        consecutive_failures: u32,
        circuit_open_until: Option<DateTime<Utc>>,
        last_error: Option<&str>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute("insert into provider_health(capability, provider_key, consecutive_failures, circuit_open_until, last_error, updated_at) values(?1,?2,?3,?4,?5,?6) on conflict(capability,provider_key) do update set consecutive_failures=excluded.consecutive_failures,circuit_open_until=excluded.circuit_open_until,last_error=excluded.last_error,updated_at=excluded.updated_at", params![capability, provider_key, consecutive_failures as i64, circuit_open_until.map(|time| time.to_rfc3339()), last_error, Utc::now().to_rfc3339()])?;
        Ok(())
    }

    pub fn provider_health(&self) -> Result<Vec<ProviderHealth>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select capability, provider_key, consecutive_failures, circuit_open_until, last_error, updated_at from provider_health order by capability,provider_key")?;
        let rows = statement.query_map([], |row| {
            Ok(ProviderHealth {
                capability: row.get(0)?,
                provider_key: row.get(1)?,
                consecutive_failures: row.get::<_, i64>(2)? as u32,
                circuit_open_until: row
                    .get::<_, Option<String>>(3)?
                    .map(parse_time)
                    .transpose()?,
                last_error: row.get(4)?,
                updated_at: parse_time(row.get(5)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn create_weekly_metric(
        &self,
        group_name: &str,
        room_id: &str,
    ) -> Result<String, StorageError> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute("insert into weekly_report(id, group_name, room_id, state, created_at) values(?1,?2,?3,'queued',?4)", params![id, group_name, room_id, now.to_rfc3339()])?;
        Ok(id)
    }

    pub fn update_weekly_metric(
        &self,
        id: &str,
        state: &str,
        summary: Option<&str>,
        chart_path: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute(
            "update weekly_report set state=?2, summary=?3, chart_path=?4, error=?5 where id=?1",
            params![id, state, summary, chart_path, error],
        )?;
        Ok(())
    }

    pub fn weekly_metrics(&self, limit: usize) -> Result<Vec<WeeklyMetric>, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = conn.prepare("select id, group_name, room_id, state, created_at, summary, chart_path, error from weekly_report order by created_at desc limit ?1")?;
        let rows = statement.query_map(params![limit.max(1) as i64], |row| {
            Ok(WeeklyMetric {
                report_id: row.get(0)?,
                group_name: row.get(1)?,
                room_id: row.get(2)?,
                state: row.get(3)?,
                created_at: parse_time(row.get(4)?)?,
                summary: row.get(5)?,
                chart_path: row.get(6)?,
                error: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    pub fn cleanup_operational_data(&self, retention_days: i64) -> Result<(), StorageError> {
        let cutoff = (Utc::now() - chrono::Duration::days(retention_days.max(1))).to_rfc3339();
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        conn.execute("delete from task_source_reference where task_id in (select id from summary_task where created_at < ?1)", params![cutoff])?;
        conn.execute(
            "delete from outbox_delivery where created_at < ?1",
            params![cutoff],
        )?;
        conn.execute(
            "delete from summary_task where created_at < ?1",
            params![cutoff],
        )?;
        conn.execute(
            "delete from weekly_report where created_at < ?1",
            params![cutoff],
        )?;
        Ok(())
    }

    pub fn daily_usage(
        &self,
        room_id: &str,
        since: DateTime<Utc>,
    ) -> Result<DailyUsage, StorageError> {
        let conn = self.conn.lock().map_err(|_| StorageError::Poisoned)?;
        let since = since.to_rfc3339();
        let (tasks, media) = conn.query_row(
            "select count(*), coalesce(sum(media_count), 0) from summary_task where room_id=?1 and created_at>=?2 and state!='cancelled'",
            params![room_id, since],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        let images = conn.query_row(
            "select count(*) from outbox_delivery where room_id=?1 and kind='image' and created_at>=?2 and state!='cancelled'",
            params![room_id, since],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(DailyUsage {
            tasks: tasks.max(0) as u32,
            images: images.max(0) as u32,
            media: media.max(0) as u32,
        })
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
            create table if not exists summary_task (
                id text primary key, room_id text not null, source text not null, state text not null,
                stage text not null, since_at text not null, until_at text not null, created_at text not null,
                started_at text, completed_at text, summary text, error text, retry_of text,
                config_revision text not null, message_count integer not null default 0, media_count integer not null default 0
            );
            create index if not exists idx_summary_task_created_at on summary_task(created_at desc);
            create index if not exists idx_summary_task_room_state on summary_task(room_id, state);
            create table if not exists outbox_delivery (
                id text primary key, task_id text, room_id text not null, kind text not null, payload text not null,
                state text not null, attempts integer not null, next_attempt_at text not null, created_at text not null,
                updated_at text not null, error text, idempotency_key text, lease_expires_at text
            );
            create index if not exists idx_outbox_delivery_due on outbox_delivery(state, next_attempt_at);
            create table if not exists task_source_reference (
                task_id text not null, point_index integer not null, source_id text not null, occurred_at text not null,
                sender_label text not null, message_index integer not null,
                primary key(task_id, point_index, source_id)
            );
            create table if not exists provider_health (
                capability text not null, provider_key text not null, consecutive_failures integer not null,
                circuit_open_until text, last_error text, updated_at text not null,
                primary key(capability, provider_key)
            );
            create table if not exists weekly_report (
                id text primary key, group_name text not null, room_id text not null, state text not null,
                created_at text not null, summary text, chart_path text, error text
            );
            create index if not exists idx_weekly_report_created_at on weekly_report(created_at desc);
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
        ensure_column(&conn, "outbox_delivery", "idempotency_key", "text")?;
        ensure_column(&conn, "outbox_delivery", "lease_expires_at", "text")?;
        conn.execute_batch("create unique index if not exists idx_outbox_delivery_idempotency on outbox_delivery(idempotency_key) where idempotency_key is not null;")?;
        Ok(())
    }
}

fn parse_time(value: String) -> Result<DateTime<Utc>, rusqlite::Error> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}

fn row_to_task(row: &rusqlite::Row<'_>) -> Result<TaskRecord, rusqlite::Error> {
    Ok(TaskRecord {
        id: row.get(0)?,
        room_id: row.get(1)?,
        source: row.get(2)?,
        state: TaskState::parse(&row.get::<_, String>(3)?),
        stage: row.get(4)?,
        since: parse_time(row.get(5)?)?,
        until: parse_time(row.get(6)?)?,
        created_at: parse_time(row.get(7)?)?,
        started_at: row
            .get::<_, Option<String>>(8)?
            .map(parse_time)
            .transpose()?,
        completed_at: row
            .get::<_, Option<String>>(9)?
            .map(parse_time)
            .transpose()?,
        summary: row.get(10)?,
        error: row.get(11)?,
        retry_of: row.get(12)?,
        config_revision: row.get(13)?,
        message_count: row.get::<_, i64>(14)? as u64,
        media_count: row.get::<_, i64>(15)? as u64,
    })
}

fn row_to_delivery(row: &rusqlite::Row<'_>) -> Result<DeliveryRecord, rusqlite::Error> {
    Ok(DeliveryRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        room_id: row.get(2)?,
        kind: row.get(3)?,
        payload: row.get(4)?,
        state: DeliveryState::parse(&row.get::<_, String>(5)?),
        attempts: row.get::<_, i64>(6)? as u32,
        next_attempt_at: parse_time(row.get(7)?)?,
        created_at: parse_time(row.get(8)?)?,
        updated_at: parse_time(row.get(9)?)?,
        error: row.get(10)?,
        idempotency_key: row.get::<_, Option<String>>(11)?.unwrap_or_default(),
        lease_expires_at: row
            .get::<_, Option<String>>(12)?
            .map(parse_time)
            .transpose()?,
    })
}

fn delivery_idempotency_key(
    task_id: Option<&str>,
    room_id: &str,
    kind: &str,
    payload: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(task_id.unwrap_or_default().as_bytes());
    hasher.update([0]);
    hasher.update(room_id.as_bytes());
    hasher.update([0]);
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(payload.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    data_type: &str,
) -> Result<(), StorageError> {
    let mut statement = conn.prepare(&format!("pragma table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    if !columns
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == column)
    {
        conn.execute_batch(&format!(
            "alter table {table} add column {column} {data_type};"
        ))?;
    }
    Ok(())
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

    #[test]
    fn persists_task_sources_and_delivery() {
        let store = SqliteStateStore::in_memory().unwrap();
        let now = Utc::now();
        let task = store
            .create_task(NewTask {
                room_id: "room-a",
                source: "manual",
                since: now - chrono::Duration::hours(1),
                until: now,
                config_revision: "test",
                retry_of: None,
            })
            .unwrap();
        store
            .update_task(&task.id, TaskState::Running, "history", None, None, 4, 1)
            .unwrap();
        store
            .add_source_references(&[SourceReference {
                task_id: task.id.clone(),
                point_index: 1,
                source_id: "m-1".into(),
                occurred_at: now,
                sender_label: "用户 #1".into(),
                message_index: 3,
            }])
            .unwrap();
        let delivery = store
            .enqueue_delivery(Some(&task.id), "room-a", "text", "summary")
            .unwrap();
        assert_eq!(store.tasks(10, Some("room-a"), None).unwrap().len(), 1);
        assert_eq!(
            store.source_references(&task.id).unwrap()[0].source_id,
            "m-1"
        );
        assert_eq!(store.due_deliveries(10).unwrap()[0].id, delivery.id);
        store
            .update_delivery(&delivery.id, DeliveryState::Delivered, 1, now, None)
            .unwrap();
        assert_eq!(
            store.deliveries(10).unwrap()[0].state,
            DeliveryState::Delivered
        );
    }

    #[test]
    fn outbox_is_idempotent_and_marks_interrupted_sends_uncertain() {
        let store = SqliteStateStore::in_memory().unwrap();
        let first = store
            .enqueue_delivery(None, "room-a", "text", "same payload")
            .unwrap();
        let second = store
            .enqueue_delivery(None, "room-a", "text", "same payload")
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(store.deliveries(10).unwrap().len(), 1);

        assert!(store.claim_delivery(&first.id, 1, 60).unwrap());
        assert_eq!(store.recover_interrupted_deliveries().unwrap(), 1);
        let delivery = store.deliveries(10).unwrap().pop().unwrap();
        assert_eq!(delivery.state, DeliveryState::Uncertain);
        assert!(delivery.lease_expires_at.is_none());
    }

    #[test]
    fn interrupted_tasks_only_resume_before_delivery_stages() {
        let store = SqliteStateStore::in_memory().unwrap();
        let now = Utc::now();
        let resumable = store
            .create_task(NewTask {
                room_id: "room-a",
                source: "manual",
                since: now - chrono::Duration::hours(1),
                until: now,
                config_revision: "test",
                retry_of: None,
            })
            .unwrap();
        let requires_retry = store
            .create_task(NewTask {
                room_id: "room-b",
                source: "manual",
                since: now - chrono::Duration::hours(1),
                until: now,
                config_revision: "test",
                retry_of: None,
            })
            .unwrap();
        store
            .update_task(
                &resumable.id,
                TaskState::Running,
                "history",
                None,
                None,
                0,
                0,
            )
            .unwrap();
        store
            .update_task(
                &requires_retry.id,
                TaskState::Running,
                "text_summary_completed",
                None,
                None,
                0,
                0,
            )
            .unwrap();

        assert_eq!(store.recover_interrupted_tasks().unwrap(), (1, 1));
        let tasks = store.tasks(10, None, None).unwrap();
        let resumed = tasks.iter().find(|task| task.id == resumable.id).unwrap();
        let failed = tasks
            .iter()
            .find(|task| task.id == requires_retry.id)
            .unwrap();
        assert_eq!(resumed.state, TaskState::Queued);
        assert_eq!(resumed.stage, "recovered_after_restart");
        assert_eq!(failed.state, TaskState::Failed);
        assert_eq!(failed.stage, "interrupted_requires_retry");
    }

    #[test]
    fn provider_health_round_trips() {
        let store = SqliteStateStore::in_memory().unwrap();
        store
            .update_provider_health("llm", "primary", 3, Some(Utc::now()), Some("timeout"))
            .unwrap();
        let health = store.provider_health().unwrap();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].consecutive_failures, 3);
        assert_eq!(health[0].last_error.as_deref(), Some("timeout"));
    }
}
