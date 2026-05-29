use std::{collections::HashSet, sync::Arc};

use prost_types::Timestamp;

use crate::{
    proto::{self, terminal_response::Response, *},
    pty::{MetadataReason, PtyEvent, PtyInfo, PtyMetadata, PtyRegistry, SubscriberInfo},
};

/// Smallest bounding box that fits every subscriber, so no client has its
/// content clipped. Subscribers that report an unknown (zero) size are ignored.
/// Returns None when no subscriber reports a usable size.
fn best_fit_size(subscribers: &[(String, SubscriberInfo)]) -> Option<(u32, u32)> {
    subscribers.iter()
        .filter(|(_, s)| s.cols > 0 && s.rows > 0)
        .map(|(_, s)| (s.cols, s.rows))
        .reduce(|(ac, ar), (c, r)| (ac.min(c), ar.min(r)))
}

pub fn pty_info_to_item(info: PtyInfo) -> PtyItem {
    let created = info.created_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    PtyItem {
        pty_id:      info.id,
        hostname:    info.hostname,
        pts_name:    info.pts_name,
        cols:        info.cols,
        rows:        info.rows,
        title:       info.title,
        created_at:  Some(Timestamp {
            seconds: created.as_secs() as i64,
            nanos:   created.subsec_nanos() as i32,
        }),
        last_subscribed_at: info.last_subscribed_at.map(|t| {
            let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
            Timestamp { seconds: d.as_secs() as i64, nanos: d.subsec_nanos() as i32 }
        }),
        subscribers: info.subscribers.unwrap_or_default().into_iter().map(|(id, s)| {
            let sub_created = s.created_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            proto::SubscriberInfo {
                subscriber_id: id,
                hostname:      s.hostname,
                cols:          s.cols,
                rows:          s.rows,
                created_at:    Some(Timestamp {
                    seconds: sub_created.as_secs() as i64,
                    nanos:   sub_created.subsec_nanos() as i32,
                }),
            }
        }).collect(),
        sort_order: info.sort_order,
    }
}

fn ok_response(pty_id: u64) -> TerminalResponse {
    TerminalResponse {
        response: Some(Response::Command(CommandResponse {
            pty_id,
            success: true,
            error: None,
        })),
    }
}

fn err_response(pty_id: u64, msg: String) -> TerminalResponse {
    TerminalResponse {
        response: Some(Response::Command(CommandResponse {
            pty_id,
            success: false,
            error: Some(msg),
        })),
    }
}

pub fn handle_list(registry: &PtyRegistry) -> TerminalResponse {
    let items = registry.list().into_iter()
        .map(|h| pty_info_to_item(h.info()))
        .collect();
    TerminalResponse {
        response: Some(Response::List(ListResponse { items })),
    }
}

pub fn handle_create(registry: &PtyRegistry, req: CreateRequest) -> TerminalResponse {
    match registry.create(req.cols, req.rows, req.command.as_deref()) {
        Ok(h) => TerminalResponse {
            response: Some(Response::Create(CreateResponse {
                item: Some(pty_info_to_item(h.info())),
            })),
        },
        Err(e) => err_response(0, e.to_string()),
    }
}

pub fn handle_destroy(
    registry:       &PtyRegistry,
    req:            DestroyRequest,
    subscriber_id:  &str,
    subscribed_ids: &mut HashSet<u64>,
    sub_tasks:      &mut std::collections::HashMap<u64, tokio::task::JoinHandle<()>>,
) -> TerminalResponse {
    let id = req.pty_id;
    if let Some(task) = sub_tasks.remove(&id) {
        task.abort();
    }
    subscribed_ids.remove(&id);
    if let Some(handle) = registry.get(id) {
        handle.remove_subscriber(subscriber_id);
        // No SUBSCRIBERS_CHANGED broadcast here — CLOSED (emitted by reader_thread on child exit)
        // is the terminal event for subscribers. Broadcasting SUBSCRIBERS_CHANGED on destroy would
        // race with the CLOSED event and add no useful information.
    }
    match registry.destroy(id) {
        Ok(_)  => ok_response(id),
        Err(e) => err_response(id, e.to_string()),
    }
}

pub fn handle_subscribe(
    registry:       &PtyRegistry,
    req:            SubscribeRequest,
    subscriber_id:  &str,
    subscribed_ids: &mut HashSet<u64>,
    sub_tasks:      &mut std::collections::HashMap<u64, tokio::task::JoinHandle<()>>,
    sub_tx:         &tokio::sync::mpsc::Sender<(u64, PtyEvent)>,
) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => TerminalResponse {
            response: Some(Response::Subscribe(SubscribeResponse {
                pty_id:        id,
                success:       false,
                error:         Some("PTY not found".into()),
                subscriber_id: String::new(),
            })),
        },
        Some(handle) => {
            let info = crate::pty::SubscriberInfo {
                hostname:   req.hostname,
                cols:       req.cols,
                rows:       req.rows,
                created_at: std::time::SystemTime::now(),
            };
            if !subscribed_ids.contains(&id) {
                let data_rx = handle.subscribe();
                let meta_rx = handle.meta_subscribe();
                let tx = sub_tx.clone();
                let pty_id_clone = id;
                let task = tokio::spawn(async move {
                    use tokio_stream::{StreamExt, wrappers::{BroadcastStream, errors::BroadcastStreamRecvError}};
                    let mut data_stream = BroadcastStream::new(data_rx);
                    let mut meta_stream = BroadcastStream::new(meta_rx);
                    loop {
                        tokio::select! {
                            item = data_stream.next() => match item {
                                Some(Ok(event)) => {
                                    if tx.send((pty_id_clone, event)).await.is_err() { break; }
                                }
                                Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                    tracing::warn!(pty_id = format!("{:016x}", pty_id_clone), skipped = n, "data broadcast lagged");
                                }
                                None => break,
                            },
                            item = meta_stream.next() => match item {
                                Some(Ok(meta)) => {
                                    if tx.send((pty_id_clone, PtyEvent::Metadata(meta))).await.is_err() { break; }
                                }
                                Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                    tracing::warn!(pty_id = format!("{:016x}", pty_id_clone), skipped = n, "meta broadcast lagged");
                                }
                                None => break,
                            },
                        }
                    }
                });
                sub_tasks.insert(id, task);
                subscribed_ids.insert(id);
            }
            // Upsert and broadcast unconditionally (covers both new and already-subscribed)
            handle.upsert_subscriber(subscriber_id, info);
            handle.touch_last_subscribed();
            // Refit the PTY to the smallest size that fits every subscriber, so no
            // client has content clipped. Recomputed on every subscribe, including the
            // re-subscribes a client sends (debounced) on its own SIGWINCH. resize()
            // broadcasts a Resize event of its own, so subscribers re-render.
            let snapshot = handle.info();
            if let Some((cols, rows)) = best_fit_size(snapshot.subscribers.as_deref().unwrap_or(&[])) {
                if (cols, rows) != (snapshot.cols, snapshot.rows) {
                    let _ = handle.resize(cols, rows);
                }
            }
            handle.broadcast_metadata(Arc::new(PtyMetadata {
                reason:     MetadataReason::SubscribersChanged,
                exit_code:  None,
                generation: handle.current_generation(),
                info:       handle.info(),
            }));
            TerminalResponse {
                response: Some(Response::Subscribe(SubscribeResponse {
                    pty_id:        id,
                    success:       true,
                    error:         None,
                    subscriber_id: subscriber_id.to_owned(),
                })),
            }
        }
    }
}

pub fn handle_unsubscribe(
    registry:       &PtyRegistry,
    req:            UnsubscribeRequest,
    subscriber_id:  &str,
    subscribed_ids: &mut HashSet<u64>,
    sub_tasks:      &mut std::collections::HashMap<u64, tokio::task::JoinHandle<()>>,
) -> TerminalResponse {
    let id = req.pty_id;
    if let Some(task) = sub_tasks.remove(&id) {
        task.abort();
    }
    subscribed_ids.remove(&id);
    if let Some(handle) = registry.get(id) {
        handle.remove_subscriber(subscriber_id);
        handle.broadcast_metadata(Arc::new(PtyMetadata {
            reason:     MetadataReason::SubscribersChanged,
            exit_code:  None,
            generation: handle.current_generation(),
            info:       handle.info(),
        }));
    }
    ok_response(id)
}

pub fn handle_write(registry: &PtyRegistry, req: WriteRequest) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.write(&req.data) {
            Ok(_) => ok_response(id),
            Err(e) => err_response(id, e.to_string()),
        },
    }
}

pub fn handle_resize(registry: &PtyRegistry, req: ResizeRequest) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.resize(req.cols, req.rows) {
            Ok(_) => ok_response(id),
            Err(e) => err_response(id, e.to_string()),
        },
    }
}

pub fn handle_set_title(registry: &PtyRegistry, req: SetTitleRequest) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => {
            h.set_title(&req.title);
            ok_response(id)
        }
    }
}

pub async fn handle_refresh(registry: &PtyRegistry, req: RefreshRequest) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.refresh().await {
            Ok(data) => TerminalResponse {
                response: Some(Response::Refresh(RefreshResponse {
                    pty_id: id,
                    generation: data.generation,
                    data: data.data.to_vec(),
                    cols: data.cols,
                    rows: data.rows,
                })),
            },
            Err(e) => err_response(id, e.to_string()),
        },
    }
}

pub async fn handle_scrollback(
    registry: &PtyRegistry,
    req: ScrollbackRequest,
) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.scrollback(req.row_offset, req.row_count).await {
            Ok(data) => TerminalResponse {
                response: Some(Response::Scrollback(ScrollbackResponse {
                    pty_id:                id,
                    generation:            data.generation,
                    data:                  data.data.to_vec(),
                    total_scrollback_rows: data.total_scrollback_rows,
                })),
            },
            Err(e) => err_response(id, e.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(cols: u32, rows: u32) -> (String, SubscriberInfo) {
        ("s".into(), SubscriberInfo {
            hostname:   String::new(),
            cols,
            rows,
            created_at: std::time::SystemTime::now(),
        })
    }

    #[test]
    fn best_fit_none_when_empty() {
        assert_eq!(best_fit_size(&[]), None);
    }

    #[test]
    fn best_fit_single_subscriber_is_its_own_size() {
        assert_eq!(best_fit_size(&[sub(80, 24)]), Some((80, 24)));
    }

    #[test]
    fn best_fit_takes_min_of_each_dimension_independently() {
        // Neither subscriber's size wins outright: cols from one, rows from the other.
        assert_eq!(best_fit_size(&[sub(100, 30), sub(80, 40)]), Some((80, 30)));
    }

    #[test]
    fn best_fit_ignores_zero_size_subscribers() {
        // A subscriber that hasn't reported a usable size must not collapse the box to 0.
        assert_eq!(best_fit_size(&[sub(80, 24), sub(0, 0)]), Some((80, 24)));
    }

    #[test]
    fn best_fit_none_when_all_sizes_unknown() {
        assert_eq!(best_fit_size(&[sub(0, 24), sub(80, 0)]), None);
    }
}
