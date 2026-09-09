//! Shared startup/runtime bounds for configured Matrix registration tokens.
//! These are application tokens, not bootstrap or Cloudflare credentials.
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{collections::HashSet, fs::OpenOptions, io::Read, path::Path};

use crate::{Err, Result, err};

/// Maximum UTF-8 bytes in a newly admitted registration token.
pub const MAX_TOKEN_BYTES: usize = 256;
/// Maximum distinct configured tokens, including the inline token.
pub const MAX_CONFIG_TOKENS: usize = 1024;
/// Maximum bytes read from the configured token file, plus one overflow probe.
pub const MAX_FILE_BYTES: usize = 64 * 1024;

/// Token files delimit on whitespace; controls and whitespace are never token
/// data.
#[must_use]
pub fn valid_token(token: &str) -> bool {
	!token.is_empty()
		&& token.len() <= MAX_TOKEN_BYTES
		&& !token
			.chars()
			.any(|ch| ch.is_whitespace() || ch.is_control())
}

/// Validate and combine configured tokens without truncating or logging them.
pub fn configured_tokens(inline: Option<&str>, path: Option<&Path>) -> Result<HashSet<String>> {
	let mut tokens = match path {
		| Some(path) => read_file(path)?,
		| None => HashSet::new(),
	};
	if let Some(token) = inline {
		insert_token(&mut tokens, token)?;
	}
	Ok(tokens)
}

fn insert_token(tokens: &mut HashSet<String>, token: &str) -> Result {
	if !valid_token(token) {
		return Err!("Registration token must be 1-256 bytes without whitespace or controls");
	}
	if tokens.len() >= MAX_CONFIG_TOKENS && !tokens.contains(token) {
		return Err!("Registration token configuration exceeds the token-count limit");
	}
	tokens.insert(token.to_owned());
	Ok(())
}

fn read_file(path: &Path) -> Result<HashSet<String>> {
	let mut options = OpenOptions::new();
	options.read(true);
	// Opening a FIFO must not block before we can reject its file type.
	#[cfg(unix)]
	options.custom_flags(libc::O_NONBLOCK);
	let file = options
		.open(path)
		.map_err(|_| err!("Failed to read registration token configuration"))?;
	if !file
		.metadata()
		.map_err(|_| err!("Failed to inspect registration token configuration"))?
		.is_file()
	{
		return Err!("Registration token configuration must be a regular file");
	}
	read_tokens(file)
}

fn read_tokens(reader: impl Read) -> Result<HashSet<String>> {
	let limit = u64::try_from(MAX_FILE_BYTES)
		.expect("constant bound fits u64")
		.saturating_add(1);
	let mut bytes = Vec::new();
	reader
		.take(limit)
		.read_to_end(&mut bytes)
		.map_err(|_| err!("Failed to read registration token configuration"))?;
	if bytes.len() > MAX_FILE_BYTES {
		return Err!("Registration token configuration exceeds the file-size limit");
	}
	let text = std::str::from_utf8(&bytes)
		.map_err(|_| err!("Registration token configuration is not UTF-8"))?;
	let mut tokens = HashSet::new();
	for token in text.split_ascii_whitespace() {
		insert_token(&mut tokens, token)?;
	}
	if tokens.is_empty() {
		return Err!("Registration token configuration is empty");
	}
	Ok(tokens)
}

#[cfg(test)]
mod tests {
	use std::{io::Cursor, path::Path};

	use super::{
		MAX_CONFIG_TOKENS, MAX_FILE_BYTES, MAX_TOKEN_BYTES, configured_tokens, insert_token,
		read_tokens, valid_token,
	};

	#[test]
	fn token_boundaries_and_blank_files_fail_closed() {
		assert!(valid_token(&"x".repeat(MAX_TOKEN_BYTES)));
		assert!(!valid_token(&"x".repeat(MAX_TOKEN_BYTES.saturating_add(1))));
		for token in ["", "has space", "has\0control", "unicode\u{a0}space"] {
			assert!(!valid_token(token));
		}
		read_tokens(b"".as_slice()).expect_err("empty file");
		read_tokens(b" \n\t".as_slice()).expect_err("blank file");
		let tokens = read_tokens(b" first\nsecond first ".as_slice()).expect("valid file");
		assert_eq!(tokens.len(), 2);
		assert!(tokens.contains("first"));
		assert!(tokens.contains("second"));
	}

	#[test]
	fn file_read_stops_after_one_overflow_byte() {
		let mut input = Cursor::new(vec![b'x'; MAX_FILE_BYTES.saturating_add(4096)]);
		read_tokens(&mut input).expect_err("oversize file");
		assert_eq!(
			input.position(),
			u64::try_from(MAX_FILE_BYTES)
				.expect("bound")
				.saturating_add(1)
		);
		let mut exact = vec![b' '; MAX_FILE_BYTES];
		exact[0] = b't';
		assert!(
			read_tokens(exact.as_slice())
				.expect("exact limit")
				.contains("t")
		);
	}

	#[test]
	fn configuration_count_includes_inline_token_and_deduplicates() {
		let text = (0..MAX_CONFIG_TOKENS)
			.map(|i| format!("token-{i}"))
			.collect::<Vec<_>>()
			.join(" ");
		let mut tokens = read_tokens(text.as_bytes()).expect("at capacity");
		insert_token(&mut tokens, "token-0").expect("duplicate consumes no slot");
		insert_token(&mut tokens, "extra-inline").expect_err("combined capacity");
		read_tokens(format!("{text} extra-file-token").as_bytes()).expect_err("file capacity");
	}

	#[test]
	fn invalid_file_errors_do_not_expose_path_or_content() {
		let error = configured_tokens(None, Some(Path::new("private-token-path\0")))
			.expect_err("unreadable file");
		assert!(!error.to_string().contains("private-token-path"));
		let error = read_tokens(b"private-token-body\xff".as_slice()).expect_err("invalid UTF-8");
		assert!(!error.to_string().contains("private-token-body"));
		assert!(
			configured_tokens(None, None)
				.expect("no token source")
				.is_empty()
		);
	}

	#[cfg(unix)]
	#[test]
	fn nonregular_file_is_rejected() {
		configured_tokens(None, Some(Path::new("/dev/null")))
			.expect_err("device is not a token file");
	}
}
