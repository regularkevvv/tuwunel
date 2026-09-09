//! Stable numeric map identifiers (ADR-0002).
//!
//! Every catalog map owns one immutable `MapId` used by remote backends to
//! address it without shipping names. This table is APPEND-ONLY: an id is
//! never renumbered, reused, or removed, even for dropped tombstones, because
//! remote schemas and backups key rows by these values. New maps take the
//! next free id at the end. `SCHEMA_VERSION` tracks the catalog contract;
//! it is distinct from the D1 SQL migration version and protocol major in
//! the parent module. Moving this table does not change any of those versions.
//!
//! Generated once from the catalog order of `maps::MAPS` at fork pin
//! 5ff48622a03f6dcf110a59c8369611375b649037; frozen thereafter (the catalog
//! may reorder freely, this table may not). A unit test asserts the catalog
//! and this table stay in bijection.

/// Stable numeric identity of a catalog map, shared by the database and bridge.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MapId(
	/// The immutable numeric wire identifier.
	pub u16,
);

/// Catalog contract revision, independent of SQL migrations and protocol major.
pub const SCHEMA_VERSION: u32 = 2;

/// Immutable name -> id assignments for every described map, tombstones
/// included.
pub static MAP_IDS: &[(&str, MapId)] = &[
	("alias_roomid", MapId(0)),
	("alias_userid", MapId(1)),
	("aliasid_alias", MapId(2)),
	("authchainkey_authchain", MapId(3)),
	("backupid_algorithm", MapId(4)),
	("backupid_etag", MapId(5)),
	("backupkeyid_backup", MapId(6)),
	("bannedroomids", MapId(7)),
	("disabledroomids", MapId(8)),
	("email_userid", MapId(9)),
	("eventid_backoff", MapId(10)),
	("eventid_originalpdu", MapId(11)),
	("eventid_outlierpdu", MapId(12)),
	("eventid_pduid", MapId(13)),
	("eventid_policysigstate", MapId(14)),
	("eventid_resolvedstate", MapId(15)),
	("eventid_shorteventid", MapId(16)),
	("global", MapId(17)),
	("id_appserviceregistrations", MapId(18)),
	("keychangeid_userid", MapId(19)),
	("keyid_key", MapId(20)),
	("lazyloadedids", MapId(21)),
	("logintoken_expiresatuserid", MapId(22)),
	("mediaid_file", MapId(23)),
	("mediaid_lazy", MapId(24)),
	("mediaid_lazycontent", MapId(25)),
	("mediaid_pending", MapId(26)),
	("mediaid_user", MapId(27)),
	("oauthid_session", MapId(28)),
	("oauthuniqid_oauthid", MapId(29)),
	("oidc_signingkey", MapId(30)),
	("oidcclientid_registration", MapId(31)),
	("oidccode_authsession", MapId(32)),
	("oidcdevice_userdeviceid", MapId(33)),
	("oidcdevicecode_devicegrant", MapId(34)),
	("oidcusercode_devicecode", MapId(35)),
	("oidccskeybypass_userid", MapId(36)),
	("oidcreqid_authrequest", MapId(37)),
	("onetimekeyid_onetimekeys", MapId(38)),
	("onetimekeyid4225_otk", MapId(39)),
	("openidtoken_expiresatuserid", MapId(40)),
	("pduid_pdu", MapId(41)),
	("publicroomids", MapId(42)),
	("pushkey_deviceid", MapId(43)),
	("presenceid_presence", MapId(44)),
	("readreceiptid_readreceipt", MapId(45)),
	("referencedevents", MapId(46)),
	("relatesto_typed", MapId(47)),
	("registrationtoken_info", MapId(48)),
	("roomid_knockedcount", MapId(49)),
	("roomid_invitedcount", MapId(50)),
	("roomid_inviteviaservers", MapId(51)),
	("roomid_joinedcount", MapId(52)),
	("roomid_maxremotepowerlevel", MapId(53)),
	("roomid_pduleaves", MapId(54)),
	("roomid_shortroomid", MapId(55)),
	("roomid_shortstatehash", MapId(56)),
	("roomid_spacehierarchy", MapId(57)),
	("roomid_ts_pducount", MapId(58)),
	("roomid_tscount_pducount", MapId(59)),
	("roomserverids", MapId(60)),
	("roomsynctoken_shortstatehash", MapId(61)),
	("roomuserdataid_accountdata", MapId(62)),
	("roomuserid_invitecount", MapId(63)),
	("roomuserid_joined", MapId(64)),
	("roomuserid_lastprivatereadupdate", MapId(65)),
	("roomuserid_lastnotificationread", MapId(66)),
	("roomuserid_leftcount", MapId(67)),
	("roomuserid_knockedcount", MapId(68)),
	("roomuserid_privateread", MapId(69)),
	("roomuserid_privatereadsync", MapId(70)),
	("roomuseroncejoinedids", MapId(71)),
	("roomusertype_roomuserdataid", MapId(72)),
	("senderkey_pusher", MapId(73)),
	("server_signingkeys", MapId(74)),
	("servercurrentevent_data", MapId(75)),
	("servername_destination", MapId(76)),
	("servername_educount", MapId(77)),
	("servername_override", MapId(78)),
	("servername_status", MapId(79)),
	("servernameevent_data", MapId(80)),
	("serverroomids", MapId(81)),
	("shorteventid_authchain", MapId(82)),
	("shorteventid_eventid", MapId(83)),
	("shorteventid_shortstatehash", MapId(84)),
	("shortstatehash_statediff", MapId(85)),
	("shortstatekey_statekey", MapId(86)),
	("softfailedeventids", MapId(87)),
	("statehash_shortstatehash", MapId(88)),
	("statekey_shortstatekey", MapId(89)),
	("threadactivityid_rootid", MapId(90)),
	("threadid_userids", MapId(91)),
	("threadrootid_latestcount", MapId(92)),
	("threepidsid_pending", MapId(93)),
	("timeredacted_eventid", MapId(94)),
	("todeviceid_events", MapId(95)),
	("tofrom_relation", MapId(96)),
	("spentrefresh_userdeviceid", MapId(97)),
	("token_userdeviceid", MapId(98)),
	("tokenids", MapId(99)),
	("url_preview", MapId(100)),
	("url_previews", MapId(101)),
	("userdeviceid_metadata", MapId(102)),
	("userdeviceconnid_conn", MapId(103)),
	("userdeviceid_refresh", MapId(104)),
	("userdeviceid_spentrefresh", MapId(105)),
	("userdeviceid_token", MapId(106)),
	("userdeviceidtoken_index", MapId(107)),
	("userdeviceidalgorithm_fallback", MapId(108)),
	("userdevicesessionid_threepid", MapId(109)),
	("userdevicesessionid_uiaainfo", MapId(110)),
	("userdevicetxnid_response", MapId(111)),
	("userfilterid_filter", MapId(112)),
	("userid_avatarurl", MapId(113)),
	("userid_blurhash", MapId(114)),
	("userid_dehydrateddevice", MapId(115)),
	("userid_devicelistversion", MapId(116)),
	("userid_displayname", MapId(117)),
	("userid_email", MapId(118)),
	("userid_erased", MapId(119)),
	("userid_lastonetimekeyupdate", MapId(120)),
	("userid_locked", MapId(121)),
	("userid_masterkeyid", MapId(122)),
	("userid_oauthid", MapId(123)),
	("userid_origin", MapId(124)),
	("userid_password", MapId(125)),
	("userid_presenceid", MapId(126)),
	("userid_selfsigningkeyid", MapId(127)),
	("userid_suspended", MapId(128)),
	("userid_usersigningkeyid", MapId(129)),
	("useridcount_notification", MapId(130)),
	("useridprofilekey_value", MapId(131)),
	("userroomid_highlightcount", MapId(132)),
	("userroomid_invitestate", MapId(133)),
	("userroomid_joined", MapId(134)),
	("userroomid_leftstate", MapId(135)),
	("userroomid_knockedstate", MapId(136)),
	("userroomid_notificationcount", MapId(137)),
	("uiaasessionid_metadata", MapId(138)),
];

/// Looks up the immutable id for a catalog map name.
///
/// Foreign column families opened outside the catalog have no id and return
/// `None`; they are invisible to remote backends by design.
#[must_use]
pub fn map_id(name: &str) -> Option<MapId> {
	MAP_IDS
		.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, id)| *id)
}

/// Whether an identifier belongs to the frozen, append-only catalog.
/// Tombstones remain addressable for compatible older readers/writers.
#[must_use]
pub fn contains(id: u16) -> bool { MAP_IDS.iter().any(|(_, known)| known.0 == id) }
