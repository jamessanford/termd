# TODO

## Attach: settings/options

There's no real way to set client options yet. When there is one (a config
file and/or an in-client settings UI), a first candidate is a default
`keep_on_exit` for newly created PTYs — today it's per-PTY only, off by
default, and flipped with `C-a o`.
