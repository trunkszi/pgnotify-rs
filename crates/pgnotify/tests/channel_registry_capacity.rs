use pgnotify::{channel_table::ChannelTable, types::ChannelId};
#[test]
fn collision_removal_and_reuse_match_names() -> Result<(), Box<dyn std::error::Error>> {
    let mut table = ChannelTable::with_capacity(512)?;
    let names: Vec<_> = (0..512)
        .map(|i| format!("abcdefgh{i:08x}tailtail"))
        .collect();
    let mut ids = Vec::new();
    for name in &names {
        ids.push(table.subscribe(name)?);
    }
    for (name, id) in names.iter().zip(&ids) {
        assert_eq!(table.lookup(name), Some(*id));
    }
    for i in (0..512).step_by(2) {
        table.remove(ids[i])?;
        assert!(table.lookup(&names[i]).is_none());
    }
    for i in (1..512).step_by(2) {
        assert_eq!(table.lookup(&names[i]), Some(ids[i]));
    }
    for i in (0..512).step_by(2) {
        table.subscribe(&names[i])?;
    }
    for name in &names {
        assert_eq!(
            table
                .channel(table.lookup(name).ok_or("lost name")?)
                .map(|s| s.as_str()),
            Some(name.as_str())
        );
    }
    Ok(())
}
#[test]
fn full_capacity_preserves_u16_boundary() -> Result<(), Box<dyn std::error::Error>> {
    for capacity in [1, 2, 65535, 65536] {
        let mut table = ChannelTable::with_capacity(capacity)?;
        for i in 0..capacity {
            assert_eq!(table.subscribe(&format!("c{i:04x}"))?, ChannelId(i as u16));
        }
        assert_eq!(table.len(), capacity);
        assert!(table.subscribe("fresh_name").is_err());
        let id = ChannelId((capacity - 1) as u16);
        table.remove(id)?;
        assert_eq!(table.subscribe("replacement")?, id);
        assert!(table.lookup(&format!("c{:04x}", capacity - 1)).is_none());
        assert_eq!(table.lookup("replacement"), Some(id));
    }
    assert!(ChannelTable::with_capacity(0).is_err());
    assert!(ChannelTable::with_capacity(65537).is_err());
    Ok(())
}
