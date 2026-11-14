use pgnotify::{
    ChannelId, ChannelTable, FixedString, ListenCommand, ListenerOptions, NotificationRef,
    NotifyHook, PgListener,
};

#[test]
fn lookup_preserves_byte_boundaries() -> pgnotify::Result<()> {
    let mut table = ChannelTable::with_capacity(128)?;
    let mut names = Vec::new();
    for length in 1..=63 {
        let name = "x".repeat(length);
        let id = table.subscribe(&name)?;
        assert_eq!(table.lookup(&name), Some(id));
        let miss = format!("{}y", "x".repeat(length - 1));
        assert_eq!(table.lookup(&miss), None);
        names.push((name, id));
    }
    for length in 1..=31 {
        let name = "\u{e9}".repeat(length);
        let id = table.subscribe(&name)?;
        assert_eq!(table.lookup(&name), Some(id));
        names.push((name, id));
    }
    for (index, (name, id)) in names.iter().enumerate() {
        if index % 2 == 0 {
            table.remove(*id)?;
            assert_eq!(table.lookup(name), None);
        }
    }
    for (index, (name, id)) in names.iter().enumerate() {
        assert_eq!(
            table.lookup(name),
            if index % 2 == 0 { None } else { Some(*id) }
        );
    }
    Ok(())
}

#[test]
fn colliding_hints_survive_cached_removal() -> pgnotify::Result<()> {
    let mut table = ChannelTable::with_capacity(128)?;
    let mut names = Vec::new();
    for index in 0..64 {
        let name = format!("prefix__{index:04}__suffix");
        let id = table.subscribe(&name)?;
        names.push((name, id));
    }
    for (name, id) in &names {
        assert_eq!(table.lookup(name), Some(*id));
    }
    let removed = names.remove(0);
    table.remove(removed.1)?;
    assert_eq!(table.lookup(&removed.0), None);
    for (name, id) in &names {
        assert_eq!(table.lookup(name), Some(*id));
    }
    let replacement = "prefix__9999__suffix";
    assert_eq!(table.subscribe(replacement)?, removed.1);
    assert_eq!(table.lookup(replacement), Some(removed.1));
    assert_eq!(table.lookup("prefix__1000__suffix"), None);
    for (name, id) in &names {
        assert_eq!(table.lookup(name), Some(*id));
    }
    Ok(())
}

#[test]
fn highest_channel_id_remains_cacheable() -> pgnotify::Result<()> {
    struct Hook;
    impl NotifyHook for Hook {
        fn on_notify(&mut self, _: NotificationRef<'_>) {}
    }
    let mut listener = PgListener::new(
        ListenerOptions {
            channel_capacity: 65536,
            ..Default::default()
        },
        Hook,
    )?;
    let id = ChannelId(u16::MAX);
    listener.drain_commands(
        1,
        [ListenCommand::Subscribe {
            channel_id: id,
            channel: FixedString::try_from_str("last")?,
        }],
    )?;
    assert_eq!(listener.subscribe("last")?, id);
    listener.unsubscribe("last")?;
    listener.begin_shutdown();
    Ok(())
}

#[test]
fn nine_byte_collision_checks_suffix() -> pgnotify::Result<()> {
    let mut table = ChannelTable::with_capacity(32)?;
    let id = table.subscribe("abcdefgh#")?;
    // Both names share the cached prefix and fingerprint bucket, but differ beyond eight bytes.
    assert_eq!(table.lookup("abcdefghb"), None);
    assert_eq!(table.lookup("abcdefgh#"), Some(id));
    Ok(())
}
