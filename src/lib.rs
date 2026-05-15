pub mod commands;
pub mod pty;
pub mod server;

pub mod proto {
    tonic::include_proto!("terminal");
}
