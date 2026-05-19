use tokio::sync::mpsc;
use termd::proto::{terminal_command::Command, TerminalCommand, WriteRequest};

#[derive(Clone, Copy)]
pub(super) enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
    AfterCtrlA,
}

pub(super) fn process_byte(
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
            0x01     => { to_send.push(0x01); *state = EscapeState::Normal; None }
            b'c'     => Some(InputAction::Create),
            b'"'     => Some(InputAction::ShowList),
            b' '     => Some(InputAction::SwitchNext),
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

pub(super) async fn run_stdin(
    cmd_tx:    mpsc::Sender<TerminalCommand>,
    action_tx: mpsc::Sender<super::InputAction>,
    pty_id:    String,
) {
    use tokio::io::AsyncReadExt;
    let mut stdin = tokio::io::stdin();
    let mut state = EscapeState::AfterNewline;
    let mut buf = [0u8; 256];

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let mut to_send: Vec<u8> = Vec::new();
        for &byte in &buf[..n] {
            if let Some(action) = process_byte(&mut state, byte, &mut to_send) {
                if !to_send.is_empty() {
                    let _ = cmd_tx.send(TerminalCommand {
                        command: Some(Command::Write(WriteRequest {
                            pty_id: pty_id.clone(),
                            data: to_send,
                        })),
                    }).await;
                }
                let _ = action_tx.send(action).await;
                return;
            }
        }
        if !to_send.is_empty() {
            if cmd_tx.send(TerminalCommand {
                command: Some(Command::Write(WriteRequest {
                    pty_id: pty_id.clone(),
                    data: to_send,
                })),
            }).await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::InputAction;

    fn run_from(initial: EscapeState, bytes: &[u8]) -> (EscapeState, Vec<u8>, Option<InputAction>) {
        let mut state = initial;
        let mut to_send = Vec::new();
        let mut action = None;
        for &byte in bytes {
            if let Some(a) = process_byte(&mut state, byte, &mut to_send) {
                action = Some(a);
                break;
            }
        }
        (state, to_send, action)
    }

    #[test]
    fn ctrl_a_c_creates() {
        let (_, bytes, action) = run_from(EscapeState::Normal, &[0x01, b'c']);
        assert!(matches!(action, Some(InputAction::Create)));
        assert!(bytes.is_empty());
    }

    #[test]
    fn ctrl_a_ctrl_a_sends_literal() {
        let (state, bytes, action) = run_from(EscapeState::Normal, &[0x01, 0x01]);
        assert!(action.is_none());
        assert_eq!(bytes, &[0x01]);
        assert!(matches!(state, EscapeState::Normal));
    }

    #[test]
    fn ctrl_a_d_detaches() {
        let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b'd']);
        assert!(matches!(action, Some(InputAction::Detach)));
    }

    #[test]
    fn ctrl_a_space_switches_next() {
        let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b' ']);
        assert!(matches!(action, Some(InputAction::SwitchNext)));
    }

    #[test]
    fn ctrl_a_digit_switches_index() {
        for n in 0u8..=9 {
            let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b'0' + n]);
            assert!(
                matches!(action, Some(InputAction::SwitchIndex(i)) if i == n),
                "^A {} should produce SwitchIndex({})", n, n,
            );
        }
    }

    #[test]
    fn ctrl_a_quote_shows_list() {
        let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b'"']);
        assert!(matches!(action, Some(InputAction::ShowList)));
    }

    #[test]
    fn ctrl_a_unknown_passes_through() {
        let (state, bytes, action) = run_from(EscapeState::Normal, &[0x01, b'x']);
        assert!(action.is_none());
        assert_eq!(bytes, &[0x01, b'x']);
        assert!(matches!(state, EscapeState::Normal));
    }

    #[test]
    fn tilde_dot_detaches() {
        let (_, bytes, action) = run_from(EscapeState::AfterNewline, &[b'~', b'.']);
        assert!(matches!(action, Some(InputAction::Detach)));
        assert!(bytes.is_empty());
    }

    #[test]
    fn tilde_other_passes_through() {
        let (_, bytes, action) = run_from(EscapeState::AfterNewline, &[b'~', b'x']);
        assert!(action.is_none());
        assert_eq!(bytes, &[b'~', b'x']);
    }

    #[test]
    fn ctrl_a_mid_stream_preserves_prior_bytes() {
        // Bytes before ^A stay in to_send; only the action fires.
        let (_, bytes, action) = run_from(EscapeState::Normal, &[b'h', b'i', 0x01, b'c']);
        assert!(matches!(action, Some(InputAction::Create)));
        assert_eq!(bytes, &[b'h', b'i']);
    }

    #[test]
    fn ctrl_a_works_from_after_newline_state() {
        let (_, _, action) = run_from(EscapeState::AfterNewline, &[0x01, b'c']);
        assert!(matches!(action, Some(InputAction::Create)));
    }
}
