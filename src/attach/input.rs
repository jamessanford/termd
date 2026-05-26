#[derive(Clone, Copy)]
enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
    AfterCtrlA,
    Escape,
    InCsi,
}

pub(super) struct InputResult {
    pub write: Vec<u8>,
    pub action: Option<super::InputAction>,
}

pub(super) struct InputProcessor {
    state: EscapeState,
    seq_buf: Vec<u8>,
}

impl InputProcessor {
    pub fn new() -> Self {
        Self {
            state: EscapeState::AfterNewline,
            seq_buf: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.state = EscapeState::AfterNewline;
        self.seq_buf.clear();
    }

    pub fn process(&mut self, buf: &[u8]) -> InputResult {
        let mut write = Vec::new();
        for &byte in buf {
            if let Some(action) = self.process_byte(byte, &mut write) {
                self.state = EscapeState::AfterNewline;
                self.seq_buf.clear();
                return InputResult { write, action: Some(action) };
            }
        }
        InputResult { write, action: None }
    }

    fn flush_csi(&mut self, to_send: &mut Vec<u8>) {
        to_send.push(0x1B);
        to_send.push(b'[');
        to_send.extend_from_slice(&self.seq_buf);
        self.seq_buf.clear();
    }

    fn process_byte(
        &mut self,
        byte:    u8,
        to_send: &mut Vec<u8>,
    ) -> Option<super::InputAction> {
        use super::InputAction;
        match self.state {
            EscapeState::Normal => match byte {
                0x01 => { self.state = EscapeState::AfterCtrlA; None }
                0x1B => { self.state = EscapeState::Escape; None }
                b'\r' | b'\n' => {
                    to_send.push(byte);
                    self.state = EscapeState::AfterNewline;
                    None
                }
                _ => { to_send.push(byte); None }
            },
            EscapeState::AfterNewline => match byte {
                0x01 => { self.state = EscapeState::AfterCtrlA; None }
                0x1B => { self.state = EscapeState::Escape; None }
                b'~' => { self.state = EscapeState::AfterTilde; None }
                b'\r' | b'\n' => { to_send.push(byte); None }
                _ => { to_send.push(byte); self.state = EscapeState::Normal; None }
            },
            EscapeState::AfterTilde => match byte {
                b'.' => Some(InputAction::Detach),
                0x1B => {
                    to_send.push(b'~');
                    self.state = EscapeState::Escape;
                    None
                }
                b'\r' | b'\n' => {
                    to_send.push(b'~');
                    to_send.push(byte);
                    self.state = EscapeState::AfterNewline;
                    None
                }
                _ => {
                    to_send.push(b'~');
                    to_send.push(byte);
                    self.state = EscapeState::Normal;
                    None
                }
            },
            EscapeState::AfterCtrlA => match byte {
                0x00     => Some(InputAction::SwitchNext),
                0x01     => Some(InputAction::SwitchRecent),
                0x1B     => {
                    to_send.push(0x01);
                    self.state = EscapeState::Escape;
                    None
                }
                b'a'     => { to_send.push(0x01); self.state = EscapeState::Normal; None }
                b'c'     => Some(InputAction::Create),
                b'F'     => Some(InputAction::ForceResize),
                b'R'     => Some(InputAction::ForceRefresh),
                b'"'     => Some(InputAction::ShowList),
                b'i'     => Some(InputAction::ShowInfo),
                b'k'     => Some(InputAction::Destroy),
                b's'     => Some(InputAction::ShowScrollback),
                b'?'     => Some(InputAction::ShowHelp),
                b' '     => Some(InputAction::SwitchNext),
                b'p'     => Some(InputAction::SwitchPrevious),
                b'd'     => Some(InputAction::Detach),
                b'0'..=b'9' => Some(InputAction::SwitchIndex(byte - b'0')),
                _ => {
                    to_send.push(0x01);
                    to_send.push(byte);
                    self.state = EscapeState::Normal;
                    None
                }
            },
            EscapeState::Escape => match byte {
                b'[' => {
                    self.seq_buf.clear();
                    self.state = EscapeState::InCsi;
                    None
                }
                0x1B => {
                    to_send.push(0x1B);
                    None
                }
                _ => {
                    to_send.push(0x1B);
                    to_send.push(byte);
                    self.state = EscapeState::Normal;
                    None
                }
            },
            EscapeState::InCsi => match byte {
                0x20..=0x3F => {
                    self.seq_buf.push(byte);
                    if self.seq_buf.len() > 256 {
                        self.flush_csi(to_send);
                        self.state = EscapeState::Normal;
                    }
                    None
                }
                0x40..=0x7E => {
                    match classify_csi(&self.seq_buf, byte) {
                        CsiAction::CtrlA => {
                            self.seq_buf.clear();
                            self.state = EscapeState::AfterCtrlA;
                            None
                        }
                        CsiAction::Drop => {
                            self.seq_buf.clear();
                            self.state = EscapeState::Normal;
                            None
                        }
                        CsiAction::Forward => {
                            self.flush_csi(to_send);
                            to_send.push(byte);
                            self.state = EscapeState::Normal;
                            None
                        }
                    }
                }
                0x1B => {
                    self.flush_csi(to_send);
                    self.state = EscapeState::Escape;
                    None
                }
                _ => {
                    self.flush_csi(to_send);
                    to_send.push(byte);
                    self.state = EscapeState::Normal;
                    None
                }
            },
        }
    }
}

enum CsiAction {
    CtrlA,
    Drop,
    Forward,
}

fn classify_csi(content: &[u8], final_byte: u8) -> CsiAction {
    let (private, rest) = match content.first() {
        Some(&b @ (b'<' | b'=' | b'>' | b'?')) => (Some(b), &content[1..]),
        _ => (None, content),
    };

    let param_end = rest.iter()
        .position(|&b| !(0x30..=0x3F).contains(&b))
        .unwrap_or(rest.len());
    let params = &rest[..param_end];

    if final_byte == b'u' && private.is_none() && param_end == rest.len() {
        if is_csi_u_ctrl_a(params) {
            return CsiAction::CtrlA;
        }
    }

    match (private, final_byte) {
        (Some(b'?'), b'c') => CsiAction::Drop, // DA1
        (Some(b'>'), b'c') => CsiAction::Drop, // DA2
        (None, b'R')       => CsiAction::Drop, // CPR
        (Some(b'?'), b'y') => CsiAction::Drop, // DECRPM
        (Some(b'?'), b'u') => CsiAction::Drop, // kitty keyboard flags response
        _ => CsiAction::Forward,
    }
}

fn is_csi_u_ctrl_a(params: &[u8]) -> bool {
    let s = match std::str::from_utf8(params) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let mut parts = s.split(';');
    let codepoint = match parts.next().and_then(|s| s.parse::<u32>().ok()) {
        Some(n) => n,
        None => return false,
    };
    let modifiers = parts.next()
        .and_then(|s| s.split(':').next())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1);
    codepoint == 97 && modifiers.wrapping_sub(1) & 4 != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::InputAction;

    fn process(bytes: &[u8]) -> InputResult {
        InputProcessor::new().process(bytes)
    }

    #[test]
    fn ctrl_a_c_creates() {
        let r = process(&[0x01, b'c']);
        assert!(matches!(r.action, Some(InputAction::Create)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn ctrl_a_ctrl_a_switches_recent() {
        let r = process(&[0x01, 0x01]);
        assert!(matches!(r.action, Some(InputAction::SwitchRecent)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn ctrl_a_a_sends_literal() {
        let r = process(&[0x01, b'a']);
        assert!(r.action.is_none());
        assert_eq!(r.write, &[0x01]);
    }

    #[test]
    fn ctrl_a_d_detaches() {
        let r = process(&[0x01, b'd']);
        assert!(matches!(r.action, Some(InputAction::Detach)));
    }

    #[test]
    fn ctrl_a_space_switches_next() {
        let r = process(&[0x01, b' ']);
        assert!(matches!(r.action, Some(InputAction::SwitchNext)));
    }

    #[test]
    fn ctrl_a_p_switches_previous() {
        let r = process(&[0x01, b'p']);
        assert!(matches!(r.action, Some(InputAction::SwitchPrevious)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn ctrl_a_digit_switches_index() {
        for n in 0u8..=9 {
            let r = process(&[0x01, b'0' + n]);
            assert!(
                matches!(r.action, Some(InputAction::SwitchIndex(i)) if i == n),
                "^A {} should produce SwitchIndex({})", n, n,
            );
        }
    }

    #[test]
    fn ctrl_a_quote_shows_list() {
        let r = process(&[0x01, b'"']);
        assert!(matches!(r.action, Some(InputAction::ShowList)));
    }

    #[test]
    fn ctrl_a_shift_f_force_resize() {
        let r = process(&[0x01, b'F']);
        assert!(matches!(r.action, Some(InputAction::ForceResize)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn ctrl_a_k_destroys() {
        let r = process(&[0x01, b'k']);
        assert!(matches!(r.action, Some(InputAction::Destroy)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn ctrl_a_s_shows_scrollback() {
        let r = process(&[0x01, b's']);
        assert!(matches!(r.action, Some(InputAction::ShowScrollback)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn ctrl_a_unknown_passes_through() {
        let r = process(&[0x01, b'x']);
        assert!(r.action.is_none());
        assert_eq!(r.write, &[0x01, b'x']);
    }

    #[test]
    fn tilde_dot_detaches() {
        let r = process(&[b'\r', b'~', b'.']);
        assert!(matches!(r.action, Some(InputAction::Detach)));
        assert_eq!(r.write, &[b'\r']);
    }

    #[test]
    fn tilde_other_passes_through() {
        let r = process(&[b'\r', b'~', b'x']);
        assert!(r.action.is_none());
        assert_eq!(r.write, &[b'\r', b'~', b'x']);
    }

    #[test]
    fn ctrl_a_mid_stream_preserves_prior_bytes() {
        let r = process(&[b'h', b'i', 0x01, b'c']);
        assert!(matches!(r.action, Some(InputAction::Create)));
        assert_eq!(r.write, &[b'h', b'i']);
    }

    #[test]
    fn ctrl_a_works_from_after_newline_state() {
        let r = process(&[0x01, b'c']);
        assert!(matches!(r.action, Some(InputAction::Create)));
    }

    #[test]
    fn state_resets_after_action() {
        let mut proc = InputProcessor::new();
        let r = proc.process(&[0x01, b'"']);
        assert!(matches!(r.action, Some(InputAction::ShowList)));
        let r = proc.process(&[b'x']);
        assert!(r.action.is_none());
        assert_eq!(r.write, &[b'x']);
    }

    // --- CSI-u and escape sequence tests ---

    #[test]
    fn csi_u_ctrl_a_c_creates() {
        let r = process(b"\x1b[97;5uc");
        assert!(matches!(r.action, Some(InputAction::Create)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn csi_u_ctrl_a_with_event_type() {
        let r = process(b"\x1b[97;5:1uc");
        assert!(matches!(r.action, Some(InputAction::Create)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn csi_u_ctrl_shift_a_triggers_prefix() {
        let r = process(b"\x1b[97;6uc");
        assert!(matches!(r.action, Some(InputAction::Create)));
        assert!(r.write.is_empty());
    }

    #[test]
    fn csi_u_plain_key_passes_through() {
        let r = process(b"\x1b[97u");
        assert!(r.action.is_none());
        assert_eq!(r.write, b"\x1b[97u");
    }

    #[test]
    fn da1_response_dropped() {
        let r = process(b"\x1b[?64;1c");
        assert!(r.action.is_none());
        assert!(r.write.is_empty());
    }

    #[test]
    fn da2_response_dropped() {
        let r = process(b"\x1b[>1;2;3c");
        assert!(r.action.is_none());
        assert!(r.write.is_empty());
    }

    #[test]
    fn cpr_response_dropped() {
        let r = process(b"\x1b[24;80R");
        assert!(r.action.is_none());
        assert!(r.write.is_empty());
    }

    #[test]
    fn decrpm_response_dropped() {
        let r = process(b"\x1b[?2026;2$y");
        assert!(r.action.is_none());
        assert!(r.write.is_empty());
    }

    #[test]
    fn kitty_keyboard_flags_response_dropped() {
        let r = process(b"\x1b[?3u");
        assert!(r.action.is_none());
        assert!(r.write.is_empty());
    }

    #[test]
    fn arrow_key_passes_through() {
        let r = process(b"\x1b[A");
        assert!(r.action.is_none());
        assert_eq!(r.write, b"\x1b[A");
    }

    #[test]
    fn csi_preserves_prior_bytes() {
        let r = process(b"hi\x1b[A");
        assert!(r.action.is_none());
        assert_eq!(r.write, b"hi\x1b[A");
    }

    #[test]
    fn response_drops_preserve_prior_bytes() {
        let r = process(b"hello\x1b[?64;1c");
        assert!(r.action.is_none());
        assert_eq!(r.write, b"hello");
    }

    #[test]
    fn alt_key_passes_through() {
        let r = process(b"\x1bx");
        assert!(r.action.is_none());
        assert_eq!(r.write, b"\x1bx");
    }

    #[test]
    fn csi_split_across_reads() {
        let mut proc = InputProcessor::new();
        let r1 = proc.process(b"\x1b[");
        assert!(r1.action.is_none());
        assert!(r1.write.is_empty());
        let r2 = proc.process(b"A");
        assert!(r2.action.is_none());
        assert_eq!(r2.write, b"\x1b[A");
    }

    #[test]
    fn response_between_text_drops_only_response() {
        let mut proc = InputProcessor::new();
        let r1 = proc.process(b"hello\x1b[?64;1c");
        assert!(r1.action.is_none());
        assert_eq!(r1.write, b"hello");
        let r2 = proc.process(b"world");
        assert!(r2.action.is_none());
        assert_eq!(r2.write, b"world");
    }
}
