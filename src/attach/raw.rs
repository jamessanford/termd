use anyhow::Result;

pub(super) struct RawHandler;

impl RawHandler {
    pub(super) fn new() -> Self {
        Self
    }
}

impl super::RenderModeHandler for RawHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        out.extend_from_slice(refresh_data);
        for (_gen, data) in buffered {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                out.extend_from_slice(data);
            }
            super::PtyEvent::Refresh { data, .. } => {
                out.extend_from_slice(data);
            }
            super::PtyEvent::Resize { .. } => {
                out.extend_from_slice(b"\x1b[2J");
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, _cols: u32, _rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        Ok(super::EventResult::RequestRefresh)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{PtyEvent, EventResult, RenderModeHandler};

    #[test]
    fn init_writes_refresh_data() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.init(b"hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"hello");
    }

    #[test]
    fn init_replays_buffered_chunks() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let buffered = vec![(2, b"world".to_vec())];
        let result = h.init(b"hello", &buffered, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"helloworld");
    }

    #[test]
    fn stream_copies_data_to_output() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_pty_event(PtyEvent::Stream { gen: 1, data: b"test" }, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"test");
    }

    #[test]
    fn refresh_copies_data_to_output() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 5, cols: 80, rows: 24, data: b"screen" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"screen");
    }

    #[test]
    fn resize_clears_screen() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        h.on_pty_event(PtyEvent::Resize { cols: 80, rows: 24 }, &mut out).unwrap();
        assert_eq!(out, b"\x1b[2J");
    }

    #[test]
    fn sigwinch_requests_refresh() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_sigwinch(100, 40, &mut out).unwrap();
        assert!(matches!(result, EventResult::RequestRefresh));
        assert!(out.is_empty());
    }

    #[test]
    fn closed_is_noop() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_pty_event(PtyEvent::Closed, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(out.is_empty());
    }
}
