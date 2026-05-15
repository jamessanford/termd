use std::{collections::HashSet, sync::Arc};

use prost_types::Timestamp;

use crate::{
    proto::{terminal_response::Response, *},
    pty::{PtyInfo, PtyRegistry},
};

fn pty_info_to_item(info: PtyInfo, subscribed: bool) -> PtyItem {
    let created = info.created_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    PtyItem {
        pty_id: info.id,
        hostname: info.hostname,
        pts_name: info.pts_name,
        cols: info.cols,
        rows: info.rows,
        title: info.title,
        subscribed,
        created_at: Some(Timestamp {
            seconds: created.as_secs() as i64,
            nanos: created.subsec_nanos() as i32,
        }),
    }
}

fn ok_response(pty_id: String) -> TerminalResponse {
    TerminalResponse {
        response: Some(Response::Command(CommandResponse {
            pty_id,
            success: true,
            error: None,
        })),
    }
}

fn err_response(pty_id: String, msg: String) -> TerminalResponse {
    TerminalResponse {
        response: Some(Response::Command(CommandResponse {
            pty_id,
            success: false,
            error: Some(msg),
        })),
    }
}

pub fn handle_list(registry: &PtyRegistry, subscribed_ids: &HashSet<String>) -> TerminalResponse {
    let items = registry.list().into_iter().map(|h| {
        let subscribed = subscribed_ids.contains(h.id());
        pty_info_to_item(h.info(), subscribed)
    }).collect();
    TerminalResponse {
        response: Some(Response::List(ListResponse { items })),
    }
}

pub fn handle_create(registry: &PtyRegistry, req: CreateRequest) -> TerminalResponse {
    match registry.create(req.cols, req.rows, req.command.as_deref()) {
        Ok(h) => TerminalResponse {
            response: Some(Response::Create(CreateResponse {
                item: Some(pty_info_to_item(h.info(), false)),
            })),
        },
        Err(e) => err_response(String::new(), e.to_string()),
    }
}

pub fn handle_destroy(registry: &PtyRegistry, req: DestroyRequest) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.destroy(&req.pty_id) {
        Ok(_) => ok_response(id),
        Err(e) => err_response(id, e.to_string()),
    }
}

pub fn handle_subscribe(
    registry: &PtyRegistry,
    req: SubscribeRequest,
    subscribed_ids: &mut HashSet<String>,
    sub_tasks: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
    sub_tx: &tokio::sync::mpsc::Sender<(String, Arc<crate::pty::PtyChunk>)>,
) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(handle) => {
            if !subscribed_ids.contains(&id) {
                let rx = handle.subscribe();
                let tx = sub_tx.clone();
                let pty_id = id.clone();
                let task = tokio::spawn(async move {
                    use tokio_stream::StreamExt;
                    let mut stream = tokio_stream::wrappers::BroadcastStream::new(rx);
                    while let Some(Ok(chunk)) = stream.next().await {
                        if tx.send((pty_id.clone(), chunk)).await.is_err() {
                            break;
                        }
                    }
                });
                sub_tasks.insert(id.clone(), task);
                subscribed_ids.insert(id.clone());
            }
            ok_response(id)
        }
    }
}

pub fn handle_unsubscribe(
    req: UnsubscribeRequest,
    subscribed_ids: &mut HashSet<String>,
    sub_tasks: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
) -> TerminalResponse {
    let id = req.pty_id.clone();
    if let Some(task) = sub_tasks.remove(&id) {
        task.abort();
    }
    subscribed_ids.remove(&id);
    ok_response(id)
}

pub fn handle_write(registry: &PtyRegistry, req: WriteRequest) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.write(&req.data) {
            Ok(_) => ok_response(id),
            Err(e) => err_response(id, e.to_string()),
        },
    }
}

pub fn handle_resize(registry: &PtyRegistry, req: ResizeRequest) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.resize(req.cols, req.rows) {
            Ok(_) => ok_response(id),
            Err(e) => err_response(id, e.to_string()),
        },
    }
}

pub fn handle_set_title(registry: &PtyRegistry, req: SetTitleRequest) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => {
            h.set_title(&req.title);
            ok_response(id)
        }
    }
}

pub async fn handle_refresh(registry: &PtyRegistry, req: RefreshRequest) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.refresh().await {
            Ok(data) => TerminalResponse {
                response: Some(Response::Refresh(RefreshResponse {
                    pty_id: id,
                    generation: data.generation,
                    data: data.data.to_vec(),
                    cursor_x: data.cursor_x,
                    cursor_y: data.cursor_y,
                })),
            },
            Err(e) => err_response(id, e.to_string()),
        },
    }
}
