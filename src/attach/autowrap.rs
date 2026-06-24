use anyhow::Result;

pub(super) struct AutowrapHandler {
    server_cols: u32,
    server_rows: u32,
}

impl AutowrapHandler {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self { server_cols, server_rows })
    }
}

impl super::RenderModeHandler for AutowrapHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        out.extend_from_slice(refresh_data);
        for (_gen, data) in buffered {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if let super::PtyEvent::Stream { data, .. } = event {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, _cols: u32, _rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        Ok(super::EventResult::Continue)
    }
}
