//! Synchronous Diesel transaction notifications and owned-notification adapters.
//!
//! CRUD calls and Diesel's notification iterator may block and allocate internally.
//! Use a dedicated `PgListener` for the non-blocking, borrowed receive path.

use std::io::Write;

use diesel::{
    PgConnection, RunQueryDsl,
    pg::{Pg, PgNotification},
    query_builder::{AstPass, QueryFragment, QueryId},
    result::QueryResult,
    sql_types::Text,
};

use crate::{
    channel_table::ChannelTable,
    error::PgNotifyError,
    hook::NotifyRouter,
    notify_ref::NotificationRef,
    types::{NotifyFrame, PG_NOTIFY_MAX_PAYLOAD_LEN, PayloadBuf, validate_channel_name},
};

diesel::define_sql_function! {
    /// PostgreSQL's transaction-scoped notification function with bound text arguments.
    fn pg_notify(channel: Text, payload: Text);
}

/// Validation or codec failures remain distinct from Diesel query failures.
#[derive(Debug, thiserror::Error)]
pub enum DieselNotifyError {
    /// Rejected before issuing SQL on the CRUD connection.
    #[error(transparent)]
    Notify(#[from] PgNotifyError),
    /// The original Diesel error, including database error information.
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
}

/// Result of a synchronous Diesel notification operation.
pub type DieselNotifyResult<T> = core::result::Result<T, DieselNotifyError>;

/// Transactional notification helpers for the caller's synchronous CRUD connection.
pub trait DieselNotifyExt {
    /// Emit a raw UTF-8 payload; delivery follows the enclosing transaction's commit.
    ///
    /// # Errors
    /// Rejects invalid channels, oversized or NUL-containing payloads before SQL,
    /// and preserves Diesel execution errors.
    fn notify_channel(&mut self, channel: &str, payload: &str) -> DieselNotifyResult<()>;

    /// Encode and emit an event using the same router contract as the consumer.
    ///
    /// # Errors
    /// Returns channel validation, codec, payload-bound, or Diesel execution errors.
    fn notify_event<R: NotifyRouter>(&mut self, event: &R) -> DieselNotifyResult<()>;

    /// Subscribe using a quoted SQL identifier; this follows transaction semantics.
    ///
    /// # Errors
    /// Returns channel validation or Diesel execution errors.
    fn listen_channel(&mut self, channel: &str) -> DieselNotifyResult<()>;

    /// Remove a subscription using a quoted SQL identifier.
    ///
    /// # Errors
    /// Returns channel validation or Diesel execution errors.
    fn unlisten_channel(&mut self, channel: &str) -> DieselNotifyResult<()>;

    /// Remove every subscription on this connection.
    ///
    /// # Errors
    /// Returns Diesel execution errors.
    fn unlisten_all(&mut self) -> DieselNotifyResult<()>;

    /// Deliver at most `budget` currently available notifications to a borrowed callback.
    ///
    /// The channel ID is absent because a Diesel connection owns no `ChannelTable`.
    /// A callback view expires before the next notification is read. A zero budget
    /// does not poll the connection. Callback errors propagate after consuming that
    /// notification; later notifications remain queued.
    ///
    /// # Errors
    /// Returns Diesel receive errors or errors returned by the callback.
    fn drain_notifications<F>(&mut self, budget: usize, callback: F) -> DieselNotifyResult<usize>
    where
        F: FnMut(NotificationRef<'_>) -> crate::error::Result<()>;
}

fn validate_payload(payload: &str) -> crate::error::Result<()> {
    if payload.len() > PG_NOTIFY_MAX_PAYLOAD_LEN {
        return Err(PgNotifyError::PayloadTooLarge {
            len: payload.len(),
            max: PG_NOTIFY_MAX_PAYLOAD_LEN,
        });
    }
    if payload.as_bytes().contains(&0) {
        return Err(PgNotifyError::CodecError {
            reason: "payload contains NUL",
        });
    }
    Ok(())
}

fn buffer_error(buffer: &PayloadBuf) -> PgNotifyError {
    if buffer.overflowed() {
        PgNotifyError::PayloadTooLarge {
            len: PayloadBuf::CAPACITY + 1,
            max: PayloadBuf::CAPACITY,
        }
    } else {
        PgNotifyError::CodecError {
            reason: "payload encoding failed",
        }
    }
}

impl DieselNotifyExt for PgConnection {
    fn notify_channel(&mut self, channel: &str, payload: &str) -> DieselNotifyResult<()> {
        validate_channel_name(channel)?;
        validate_payload(payload)?;
        let mut out = PayloadBuf::new();
        out.clear();
        out.write_all(payload.as_bytes())
            .map_err(|_| buffer_error(&out))?;
        diesel::select(pg_notify(channel, out.as_str()?)).execute(self)?;
        Ok(())
    }

    fn notify_event<R: NotifyRouter>(&mut self, event: &R) -> DieselNotifyResult<()> {
        let channel = event.channel_name();
        validate_channel_name(channel)?;
        let mut out = PayloadBuf::new();
        out.clear();
        event.encode_event_payload(&mut out)?;
        if out.overflowed() {
            return Err(buffer_error(&out).into());
        }
        let payload = out.as_str()?;
        validate_payload(payload)?;
        diesel::select(pg_notify(channel, payload)).execute(self)?;
        Ok(())
    }

    fn listen_channel(&mut self, channel: &str) -> DieselNotifyResult<()> {
        validate_channel_name(channel)?;
        ChannelCommand {
            channel,
            subscribe: true,
        }
        .execute(self)?;
        Ok(())
    }

    fn unlisten_channel(&mut self, channel: &str) -> DieselNotifyResult<()> {
        validate_channel_name(channel)?;
        ChannelCommand {
            channel,
            subscribe: false,
        }
        .execute(self)?;
        Ok(())
    }

    fn unlisten_all(&mut self) -> DieselNotifyResult<()> {
        diesel::sql_query("UNLISTEN *").execute(self)?;
        Ok(())
    }

    fn drain_notifications<F>(
        &mut self,
        budget: usize,
        mut callback: F,
    ) -> DieselNotifyResult<usize>
    where
        F: FnMut(NotificationRef<'_>) -> crate::error::Result<()>,
    {
        let mut count = 0;
        for notification in self.notifications_iter().take(budget) {
            let notification = notification?;
            callback(NotificationRef::from(&notification))?;
            count += 1;
        }
        Ok(count)
    }
}

struct ChannelCommand<'a> {
    channel: &'a str,
    subscribe: bool,
}

impl QueryId for ChannelCommand<'_> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ChannelCommand<'_> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql(if self.subscribe {
            "LISTEN "
        } else {
            "UNLISTEN "
        });
        out.push_identifier(self.channel)
    }
}

impl RunQueryDsl<PgConnection> for ChannelCommand<'_> {}

impl From<NotificationRef<'_>> for PgNotification {
    fn from(value: NotificationRef<'_>) -> Self {
        Self {
            process_id: value.backend_pid,
            channel: value.channel.to_owned(),
            payload: value.payload.to_owned(),
        }
    }
}

impl<'a> From<&'a PgNotification> for NotificationRef<'a> {
    fn from(value: &'a PgNotification) -> Self {
        Self {
            backend_pid: value.process_id,
            channel_id: None,
            channel: &value.channel,
            payload: &value.payload,
        }
    }
}

impl<const N: usize> TryFrom<&PgNotification> for NotifyFrame<N> {
    type Error = PgNotifyError;

    fn try_from(value: &PgNotification) -> crate::error::Result<Self> {
        validate_channel_name(&value.channel)?;
        NotificationRef::from(value).to_frame()
    }
}

impl<const N: usize> TryFrom<(&PgNotification, &ChannelTable)> for NotifyFrame<N> {
    type Error = PgNotifyError;

    fn try_from(
        (notification, channels): (&PgNotification, &ChannelTable),
    ) -> crate::error::Result<Self> {
        let mut borrowed = NotificationRef::from(notification);
        validate_channel_name(borrowed.channel)?;
        borrowed.channel_id = channels.lookup(borrowed.channel);
        borrowed.to_frame()
    }
}
