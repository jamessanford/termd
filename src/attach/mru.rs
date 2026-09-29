use termd::proto::PtyItem;

/// Most-recently-viewed PTY stack, most recent first. The current PTY is always
/// at the front once `touch`ed; ids of PTYs that are gone are pruned lazily by
/// `pick` (and eagerly by `remove` when we know one died).
#[derive(Default)]
pub(super) struct Mru {
    ids: Vec<u64>,
}

impl Mru {
    /// Record `id` as the most recently viewed PTY.
    pub fn touch(&mut self, id: u64) {
        self.ids.retain(|&x| x != id);
        self.ids.insert(0, id);
    }

    pub fn remove(&mut self, id: u64) {
        self.ids.retain(|&x| x != id);
    }

    /// The most recently viewed PTY in `list` other than `current`. Prunes ids
    /// no longer in `list`. With no usable history, falls back to the PTY the
    /// server says was most recently subscribed (by anyone), then created.
    /// None only when `list` holds nothing but `current`.
    pub fn pick<'a>(&mut self, list: &'a [PtyItem], current: u64) -> Option<&'a PtyItem> {
        self.ids.retain(|id| list.iter().any(|p| p.pty_id == *id));
        if let Some(item) = self.ids.iter()
            .filter(|&&id| id != current)
            .find_map(|&id| list.iter().find(|p| p.pty_id == id))
        {
            return Some(item);
        }
        list.iter()
            .filter(|p| p.pty_id != current)
            .max_by_key(|p| {
                let ts = p.last_subscribed_at.as_ref().or(p.created_at.as_ref());
                ts.map(|t| (t.seconds, t.nanos)).unwrap_or((0, 0))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::Timestamp;

    fn item(id: u64, subscribed: Option<i64>) -> PtyItem {
        PtyItem {
            pty_id: id,
            last_subscribed_at: subscribed.map(|s| Timestamp { seconds: s, nanos: 0 }),
            ..Default::default()
        }
    }

    #[test]
    fn picks_previous_not_current() {
        let list = [item(1, None), item(2, None), item(3, None)];
        let mut m = Mru::default();
        m.touch(1);
        m.touch(2);
        m.touch(3);
        assert_eq!(m.pick(&list, 3).unwrap().pty_id, 2);
    }

    #[test]
    fn walks_back_past_gone_ptys() {
        // 3 was current and died; 2 was also destroyed elsewhere: land on 1.
        let list = [item(1, None), item(4, Some(100))];
        let mut m = Mru::default();
        m.touch(1);
        m.touch(2);
        m.touch(3);
        m.remove(3);
        assert_eq!(m.pick(&list, 3).unwrap().pty_id, 1);
    }

    #[test]
    fn repeated_recent_toggles_between_two() {
        let list = [item(1, None), item(2, None), item(3, None)];
        let mut m = Mru::default();
        m.touch(1);
        m.touch(2);
        m.touch(3);
        let t = m.pick(&list, 3).unwrap().pty_id;
        m.touch(t);
        assert_eq!(t, 2);
        assert_eq!(m.pick(&list, 2).unwrap().pty_id, 3);
    }

    #[test]
    fn falls_back_to_server_recency() {
        let list = [item(1, Some(10)), item(2, Some(30)), item(3, Some(20))];
        let mut m = Mru::default();
        m.touch(1);
        assert_eq!(m.pick(&list, 1).unwrap().pty_id, 2);
    }

    #[test]
    fn none_when_only_current() {
        let list = [item(1, None)];
        let mut m = Mru::default();
        m.touch(1);
        assert!(m.pick(&list, 1).is_none());
        assert!(m.pick(&[], 1).is_none());
    }
}
