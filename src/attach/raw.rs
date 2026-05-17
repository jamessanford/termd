// Raw passthrough mode — no libghostty on the render path.

pub(super) async fn run(_ctx: super::RunContext) -> anyhow::Result<bool> {
    Ok(false)
}
