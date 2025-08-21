use std::{collections::HashMap, io::Write, sync::Arc, time::Duration};

use antithesis_sdk::assert_sometimes;
use axum::Extension;
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::ToCompactString;
use http_body_util::StreamBody;
use klukai_types::{
    agent::Agent,
    api::NotifyEvent,
    tripwire::Tripwire,
    updates::{Handle, UpdateCreated, UpdateHandle, UpdatesManager},
};
use tokio::sync::{
    RwLock as TokioRwLock,
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::api::public::pubsub::MatcherUpsertError;

pub type UpdateBroadcastCache = HashMap<Uuid, broadcast::Sender<Bytes>>;
pub type SharedUpdateBroadcastCache = Arc<TokioRwLock<UpdateBroadcastCache>>;

// this should be a fraction of the MAX_UNSUB_TIME
const RECEIVERS_CHECK_INTERVAL: Duration = Duration::from_secs(30);

pub async fn api_v1_updates(
    Extension(agent): Extension<Agent>,
    Extension(bcast_cache): Extension<SharedUpdateBroadcastCache>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Path(table): axum::extract::Path<String>,
) -> hyper::Response<
    StreamBody<impl futures::Stream<Item = Result<hyper::body::Frame<Bytes>, std::io::Error>>>,
> {
    info!("Received update request for table: {table}");

    assert_sometimes!(true, "Corrosion receives requests for table updates");

    let mut bcast_write = bcast_cache.write().await;
    let updates = agent.updates_manager();

    let upsert_res = updates.get_or_insert(
        &table,
        &agent.schema().read(),
        agent.pool(),
        tripwire.clone(),
    );

    let (handle, maybe_created) = match upsert_res {
        Ok(res) => res,
        Err(e) => {
            // Create error stream
            let error_msg = format!("Error: {}", MatcherUpsertError::from(e));
            let error_json = serde_json::to_vec(&error_msg).expect("could not serialize error");
            let (error_tx, error_rx) =
                tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, std::io::Error>>(1);
            let _ = error_tx
                .send(Ok(hyper::body::Frame::data(Bytes::from(error_json))))
                .await;
            let error_stream = ReceiverStream::new(error_rx);
            let error_body = StreamBody::new(error_stream);

            return hyper::Response::builder()
                .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                .body(error_body)
                .expect("could not build error response");
        }
    };

    let (update_id, sub_rx) =
        match upsert_update(handle.clone(), maybe_created, updates, &mut bcast_write).await {
            Ok(id) => id,
            Err(e) => {
                // Create error stream
                let error_msg = format!("Error: {}", e);
                let error_json = serde_json::to_vec(&error_msg).expect("could not serialize error");
                let (error_tx, error_rx) = tokio::sync::mpsc::channel::<
                    Result<hyper::body::Frame<Bytes>, std::io::Error>,
                >(1);
                let _ = error_tx
                    .send(Ok(hyper::body::Frame::data(Bytes::from(error_json))))
                    .await;
                let error_stream = ReceiverStream::new(error_rx);
                let error_body = StreamBody::new(error_stream);

                return hyper::Response::builder()
                    .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(error_body)
                    .expect("could not build error response");
            }
        };

    // Create streaming body
    let (stream_tx, stream_rx) =
        tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, std::io::Error>>(10240);
    let stream = ReceiverStream::new(stream_rx);
    let body = StreamBody::new(stream);

    tokio::spawn(forward_update_bytes_to_body_sender(
        handle, sub_rx, stream_tx, tripwire,
    ));

    hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header("corro-query-id", update_id.to_string())
        .body(body)
        .expect("could not generate ok http response for update request")
}

pub async fn upsert_update(
    handle: UpdateHandle,
    maybe_created: Option<UpdateCreated>,
    updates: &UpdatesManager,
    bcast_write: &mut UpdateBroadcastCache,
) -> Result<(Uuid, broadcast::Receiver<Bytes>), MatcherUpsertError> {
    let sub_rx = if let Some(created) = maybe_created {
        let (sub_tx, sub_rx) = broadcast::channel(10240);
        bcast_write.insert(handle.id(), sub_tx.clone());
        tokio::spawn(process_update_channel(
            updates.clone(),
            handle.id(),
            sub_tx,
            created.evt_rx,
        ));

        sub_rx
    } else {
        let id = handle.id();
        let sub_tx = bcast_write
            .get(&id)
            .cloned()
            .ok_or(MatcherUpsertError::MissingBroadcaster)?;
        debug!("found update handle");

        sub_tx.subscribe()
    };

    Ok((handle.id(), sub_rx))
}

pub async fn process_update_channel(
    updates: UpdatesManager,
    id: Uuid,
    tx: broadcast::Sender<Bytes>,
    mut evt_rx: mpsc::Receiver<NotifyEvent>,
) {
    let mut buf = BytesMut::new();

    // interval check for receivers
    // useful for queries that don't change often so we can cleanup...
    let mut subs_check = tokio::time::interval(RECEIVERS_CHECK_INTERVAL);

    loop {
        tokio::select! {
            biased;
            Some(query_evt) = evt_rx.recv() => {
                match make_query_event_bytes(&mut buf, &query_evt) {
                    Ok(b) => {
                        if tx.send(b).is_err() {
                            break;
                        }
                    },
                    Err(e) => {
                        match make_query_event_bytes(&mut buf, &NotifyEvent::Error(e.to_compact_string())) {
                            Ok(b) => {
                                let _ = tx.send(b);
                            }
                            Err(e) => {
                                warn!(update_id = %id, "failed to send error in update channel: {e}");
                            }
                        }
                        break;
                    }
                };
            },
            _ = subs_check.tick() => {
                if tx.receiver_count() == 0 {
                    break;
                };
            },
        };
    }

    warn!(sub_id = %id, "updates channel done");

    // remove and get handle from the agent's "matchers"
    let handle = match updates.remove(&id) {
        Some(h) => {
            info!(update_id = %id, "Removed update handle from process_update_channel");
            h
        }
        None => {
            warn!(update_id = %id, "update handle was already gone. odd!");
            return;
        }
    };

    // clean up the subscription
    handle.cleanup().await;
}

fn make_query_event_bytes(
    buf: &mut BytesMut,
    query_evt: &NotifyEvent,
) -> serde_json::Result<Bytes> {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, query_evt)?;

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    Ok(buf.split().freeze())
}

async fn forward_update_bytes_to_body_sender(
    update: UpdateHandle,
    mut rx: broadcast::Receiver<Bytes>,
    tx: tokio::sync::mpsc::Sender<Result<hyper::body::Frame<Bytes>, std::io::Error>>,
    mut tripwire: Tripwire,
) {
    let mut buf = BytesMut::new();

    let send_deadline = tokio::time::sleep(Duration::from_millis(10));
    tokio::pin!(send_deadline);

    loop {
        tokio::select! {
            biased;
            res = rx.recv() => {
                match res {
                    Ok(event_buf) => {
                        buf.extend_from_slice(&event_buf);
                        if buf.len() >= 64 * 1024
                            && let Err(_) = tx.send(Ok(hyper::body::Frame::data(buf.split().freeze()))).await {
                                warn!(update_id = %update.id(), "could not forward update query event to receiver: stream closed");
                                return;
                            }
                    },
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(update_id = %update.id(), "update skipped {} events, aborting", skipped);
                        return;
                    },
                    Err(RecvError::Closed) => {
                        info!(update_id = %update.id(), "events subcription ran out");
                        return;
                    },
                }
            },
            _ = &mut send_deadline => {
                if !buf.is_empty() {
                    if tx.send(Ok(hyper::body::Frame::data(buf.split().freeze()))).await.is_err() {
                        warn!(update_id = %update.id(), "could not forward subscription query event to receiver: stream closed");
                        return;
                    }
                } else {
                    send_deadline.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(10));
                    continue;
                }
            },
            _ = update.cancelled() => {
                info!(update_id = %update.id(), "update cancelled, aborting forwarding bytes to subscriber");
                return;
            },
            _ = &mut tripwire => {
                break;
            }
        }
    }

    // Send any remaining data
    while let Ok(event_buf) = rx.try_recv() {
        buf.extend_from_slice(&event_buf);
        if tx
            .send(Ok(hyper::body::Frame::data(buf.split().freeze())))
            .await
            .is_err()
        {
            warn!(update_id = %update.id(), "could not forward subscription query event to receiver: stream closed");
            return;
        }
    }
}
