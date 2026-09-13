use ruma::room_id;

use super::InRoomCache;

#[test]
fn a_fill_that_overlapped_an_invalidation_is_never_kept() {
	let room = room_id!("!room:example.com");
	let mut cache = InRoomCache::default();

	// A fill reads membership, then a membership commit invalidates the room
	// before the fill inserts: the stale answer is refused.
	let seen = cache.generation;
	cache.invalidate(room);
	assert!(!cache.insert_if(seen, room, "bridge", true));
	assert_eq!(cache.get(room, "bridge"), None);

	// A fill that read after the invalidation is kept.
	let seen = cache.generation;
	assert!(cache.insert_if(seen, room, "bridge", false));
	assert_eq!(cache.get(room, "bridge"), Some(false));

	// Clearing the cache counts as an invalidation too.
	let seen = cache.generation;
	cache.clear();
	assert!(!cache.insert_if(seen, room, "bridge", true));
	assert_eq!(cache.get(room, "bridge"), None);
}
