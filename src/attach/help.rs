use std::io::Write as IoWrite;
use tokio::io::AsyncReadExt;

// See input.rs

const HELP: &str = concat!(
    "\x1b[2J\x1b[H",
    "\r\n",
    "  termd keybindings\r\n",
    "\r\n",
    "  C-a c      create new PTY\r\n",
    "  C-a d      detach from termd\r\n",
    "\r\n",
    "  C-a \"      show list of PTYs\r\n",
    "  C-a space  switch to next PTY\r\n",
    "  C-a p      switch to previous PTY\r\n",
    "  C-a C-a    switch to recent PTY\r\n",
    "  C-a 0-9    switch to PTY by index\r\n",
    "  C-a k      destroy current PTY\r\n",
    "\r\n",
    "  C-a s      show scrollback\r\n",
    "  C-a i      show info\r\n",
    "  C-a F      force resize\r\n",
    "  C-a R      force refresh\r\n",
    "  C-a a      send literal C-a\r\n",
    "\r\n",
    "  C-a ?      show this help\r\n",
    "\r\n",
    "  (any key to exit)\r\n",
);

pub(super) async fn show_help(stdin: &mut tokio::io::Stdin) {
    let _ = std::io::stdout().write_all(b"\x1b[?1049h");
    let _ = std::io::stdout().flush();

    let _ = std::io::stdout().write_all(HELP.as_bytes());
    let _ = std::io::stdout().flush();

    let mut buf = [0u8; 8];
    let _ = stdin.read(&mut buf).await;

    let _ = std::io::stdout().write_all(b"\x1b[?1049l");
    let _ = std::io::stdout().flush();
}
