#[derive(Clone, Copy)]
enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
    AfterCtrlA,
}

pub(super) struct InputResult {
    pub write: Vec<u8>,
    pub action: Option<super::InputAction>,
}

pub(super) struct InputProcessor {
    state: EscapeState,
}

impl InputProcessor {
    pub fn new() -> Self {
        Self { state: EscapeState::AfterNewline }
    }

    pub fn reset(&mut self) {
        self.state = EscapeState::AfterNewline;
    }

    pub fn process(&mut self, buf: &[u8]) -> InputResult {
        let mut write = Vec::new();
        for &byte in buf {
            if let Some(action) = process_byte(&mut self.state, byte, &mut write) {
                self.state = EscapeState::AfterNewline;
                return InputResult { write, action: Some(action) };
            }
        }
        InputResult { write, action: None }
    }
}

fn process_byte(
    state:   &mut EscapeState,
    byte:    u8,
    to_send: &mut Vec<u8>,
) -> Option<super::InputAction> {
    use super::InputAction;
    match state {
        EscapeState::Normal => match byte {
            0x01 => { *state = EscapeState::AfterCtrlA; None }
            b'\r' | b'\n' => { to_send.push(byte); *state = EscapeState::AfterNewline; None }
            _ => { to_send.push(byte); None }
        },
        EscapeState::AfterNewline => match byte {
            0x01 => { *state = EscapeState::AfterCtrlA; None }
            b'~' => { *state = EscapeState::AfterTilde; None }
            b'\r' | b'\n' => { to_send.push(byte); None }
            _ => { to_send.push(byte); *state = EscapeState::Normal; None }
        },
        EscapeState::AfterTilde => match byte {
            b'.' => Some(InputAction::Detach),
            b'\r' | b'\n' => {
                to_send.push(b'~');
                to_send.push(byte);
                *state = EscapeState::AfterNewline;
                None
            }
            _ => {
                to_send.push(b'~');
                to_send.push(byte);
                *state = EscapeState::Normal;
                None
            }
        },
        EscapeState::AfterCtrlA => match byte {
            0x00     => Some(InputAction::SwitchNext), // treat "C-a C-space" as "C-a space"
            0x01     => Some(InputAction::SwitchRecent),
            b'a'     => { to_send.push(0x01); *state = EscapeState::Normal; None }
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
                *state = EscapeState::Normal;
                None
            }
        },
    }
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
        // Next input should not be in AfterCtrlA state
        let r = proc.process(&[b'x']);
        assert!(r.action.is_none());
        assert_eq!(r.write, &[b'x']);
    }
}
