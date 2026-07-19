use std::sync::Arc;

use prost_types::Timestamp;
use tonic::Status;

use crate::{
    proto::{self, *},
    pty::{MetadataReason, PtyInfo, PtyMetadata, PtyRegistry},
};

/// Smallest bounding box that fits every subscriber, so no client has its
/// content clipped. Subscribers that report an unknown (zero) size are ignored.
/// Returns None when no subscriber reports a usable size.
fn best_fit_size(subscribers: &[(String, crate::pty::SubscriberInfo)]) -> Option<(u32, u32)> {
    subscribers.iter()
        .filter(|(_, s)| s.cols > 0 && s.rows > 0)
        .map(|(_, s)| (s.cols, s.rows))
        .reduce(|(ac, ar), (c, r)| (ac.min(c), ar.min(r)))
}

/// Given the current PTY size and the best-fit box across subscribers, returns the
/// size to resize to, or None to leave the PTY alone.
///
/// With multiple subscribers (`allow_shrink == false`) we only ever grow: the PTY
/// expands only when every subscriber can accommodate a larger box (neither
/// dimension would shrink). A smaller client never shrinks the PTY out from under
/// the others — it letterboxes via cell mode instead. Auto-shrinking under
/// multiple clients is disorienting (the shell reflows and content jumps).
///
/// With a single subscriber (`allow_shrink == true`) there are no other clients to
/// clip, so we track that one client exactly — growing or shrinking — to match its
/// window.
fn refit_target(current: (u32, u32), best_fit: (u32, u32), allow_shrink: bool) -> Option<(u32, u32)> {
    let (cur_cols, cur_rows) = current;
    let (fit_cols, fit_rows) = best_fit;
    if best_fit == current {
        None
    } else if allow_shrink || (fit_cols >= cur_cols && fit_rows >= cur_rows) {
        Some(best_fit)
    } else {
        None
    }
}

pub fn pty_info_to_item(info: PtyInfo) -> PtyItem {
    let created = info.created_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    PtyItem {
        pty_id:      info.id,
        hostname:    info.hostname,
        pts_name:    info.pts_name,
        size:        Some(Size { cols: info.cols, rows: info.rows }),
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
                size:          Some(Size { cols: s.cols, rows: s.rows }),
                created_at:    Some(Timestamp {
                    seconds: sub_created.as_secs() as i64,
                    nanos:   sub_created.subsec_nanos() as i32,
                }),
            }
        }).collect(),
        sort_order: info.sort_order,
    }
}

pub fn handle_list(registry: &PtyRegistry) -> ListResponse {
    let items = registry.list().into_iter()
        .map(|h| pty_info_to_item(h.info()))
        .collect();
    ListResponse { items }
}

pub fn handle_create(registry: &PtyRegistry, req: CreateRequest) -> Result<PtyItem, Status> {
    let size = req.size.unwrap_or_default();
    match registry.create(size.cols, size.rows, req.command.as_deref()) {
        Ok(h)  => Ok(pty_info_to_item(h.info())),
        Err(e) => Err(Status::internal(e.to_string())),
    }
}

pub fn handle_destroy(registry: &PtyRegistry, req: DestroyRequest) -> Result<(), Status> {
    match registry.destroy(req.pty_id) {
        Ok(_)  => Ok(()),
        Err(e) => Err(Status::not_found(e.to_string())),
    }
}

pub async fn handle_resize(registry: &PtyRegistry, req: ResizeRequest) -> Result<PtyItem, Status> {
    let id = req.pty_id;
    match registry.get(id) {
        None => Err(Status::not_found("PTY not found")),
        Some(h) => {
            let size = req.size.unwrap_or_default();
            h.resize(size.cols, size.rows).await.map_err(|e| Status::internal(e.to_string()))?;
            Ok(pty_info_to_item(h.info()))
        }
    }
}

pub fn handle_set_title(registry: &PtyRegistry, req: SetTitleRequest) -> Result<PtyItem, Status> {
    let id = req.pty_id;
    match registry.get(id) {
        None => Err(Status::not_found("PTY not found")),
        Some(h) => {
            h.set_title(&req.title);
            Ok(pty_info_to_item(h.info()))
        }
    }
}

pub async fn handle_refresh(registry: &PtyRegistry, req: RefreshRequest) -> Result<(), Status> {
    match registry.get(req.pty_id) {
        None => Err(Status::not_found("PTY not found")),
        Some(h) => {
            h.deliver_refresh(&req.subscriber_id).await.map_err(|e| Status::internal(e.to_string()))?;
            Ok(())
        }
    }
}

pub async fn handle_scrollback(
    registry: &PtyRegistry,
    req: ScrollbackRequest,
) -> Result<ScrollbackResponse, Status> {
    use crate::pty::ScrollbackOp;
    let id = req.pty_id;
    let op = match req.kind() {
        proto::ScrollbackOpKind::ScrollbackOpen  => ScrollbackOp::Open,
        proto::ScrollbackOpKind::ScrollbackMove  => ScrollbackOp::Move,
        proto::ScrollbackOpKind::ScrollbackClose => ScrollbackOp::Close,
    };
    match registry.get(id) {
        None => Err(Status::not_found("PTY not found")),
        Some(h) => match h.scrollback(&req.subscriber_id, op, req.amount, req.row_count).await {
            Ok(data) => Ok(ScrollbackResponse {
                data:                  data.data.to_vec(),
                total_scrollback_rows: data.total_scrollback_rows,
                row_offset:            data.row_offset,
            }),
            Err(e) => Err(Status::internal(e.to_string())),
        },
    }
}

/// Subscribe-time refit + broadcast side effects, shared by the Subscribe
/// stream handler. Upserts the subscriber's reported size, refits the PTY to
/// its subscribers (growing only when there are multiple, or tracking exactly
/// when there's just one — see `best_fit_size`/`refit_target`), and broadcasts
/// a `SubscribersChanged` metadata event. Does not build any proto response;
/// the stream side handles that.
///
/// The refit awaits the reader's acknowledgement (while ignoring the outcome)
/// so the resize has taken effect before Subscribe returns — a `List` racing
/// the subscribe must not observe stale dimensions.
pub async fn apply_subscribe(
    handle: &crate::pty::PtyHandle,
    subscriber_id: &str,
    info: crate::pty::SubscriberInfo,
) {
    handle.upsert_subscriber(subscriber_id, info);
    handle.touch_last_subscribed();
    // Refit the PTY to its subscribers. With multiple clients we only grow,
    // to fit every subscriber without clipping a smaller one (see
    // refit_target). With a single client we track it exactly, growing or
    // shrinking to match its window. Recomputed on every subscribe,
    // including the re-subscribes a client sends (debounced) on its own
    // SIGWINCH. resize() broadcasts a Resize event of its own, so
    // subscribers re-render.
    let snapshot = handle.info();
    let subs = snapshot.subscribers.as_deref().unwrap_or(&[]);
    if let Some(best) = best_fit_size(subs) {
        let allow_shrink = subs.len() == 1;
        if let Some((cols, rows)) = refit_target((snapshot.cols, snapshot.rows), best, allow_shrink) {
            let _ = handle.resize(cols, rows).await;
        }
    }
    handle.broadcast_metadata(Arc::new(PtyMetadata {
        reason:     MetadataReason::SubscribersChanged,
        exit_code:  None,
        generation: handle.current_generation(),
        info:       handle.info(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(cols: u32, rows: u32) -> (String, crate::pty::SubscriberInfo) {
        ("s".into(), crate::pty::SubscriberInfo {
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

    #[test]
    fn refit_grows_when_both_dimensions_larger() {
        assert_eq!(refit_target((80, 24), (100, 40), false), Some((100, 40)));
    }

    #[test]
    fn refit_grows_when_one_dimension_larger_other_equal() {
        assert_eq!(refit_target((80, 24), (100, 24), false), Some((100, 24)));
    }

    #[test]
    fn refit_none_when_equal() {
        assert_eq!(refit_target((80, 24), (80, 24), false), None);
    }

    #[test]
    fn refit_none_when_smaller() {
        // With multiple subscribers, a smaller client must not shrink the PTY out
        // from under the others.
        assert_eq!(refit_target((80, 24), (70, 20), false), None);
    }

    #[test]
    fn refit_none_when_one_grows_one_shrinks() {
        // Mixed, grow-only: width would grow but height would shrink — leave it
        // alone rather than shrink either dimension.
        assert_eq!(refit_target((80, 24), (100, 20), false), None);
    }

    #[test]
    fn refit_shrinks_when_allowed() {
        // A single subscriber owns the PTY: shrink to exactly its size.
        assert_eq!(refit_target((80, 24), (70, 20), true), Some((70, 20)));
    }

    #[test]
    fn refit_grows_when_shrink_allowed() {
        // Growing is still fine in the single-subscriber case.
        assert_eq!(refit_target((80, 24), (100, 40), true), Some((100, 40)));
    }

    #[test]
    fn refit_shrinks_one_dimension_when_allowed() {
        // Mixed dims collapse to the exact best-fit when shrinking is allowed.
        assert_eq!(refit_target((80, 24), (100, 20), true), Some((100, 20)));
    }

    #[test]
    fn refit_none_when_equal_even_if_shrink_allowed() {
        // No resize when the PTY already matches the subscriber exactly.
        assert_eq!(refit_target((80, 24), (80, 24), true), None);
    }
}
