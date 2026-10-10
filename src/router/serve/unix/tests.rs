use std::{
	fs::{self, DirBuilder},
	os::unix::{
		fs::{DirBuilderExt, FileTypeExt, MetadataExt, symlink},
		net::UnixListener,
	},
	panic::{AssertUnwindSafe, catch_unwind},
	path::{Path, PathBuf},
	time::Duration,
};

use axum::Router;
use axum_server::Handle;
use futures::poll;
use tokio::time::timeout;
use tuwunel_core::{Result, utils::rand};

use super::serve;

struct Directory(PathBuf);

impl Directory {
	fn new() -> Self {
		let path = std::env::temp_dir().join(format!("tw-sock-{}", rand::string(8)));
		DirBuilder::new()
			.mode(0o700)
			.create(&path)
			.expect("owned socket fixture");
		Self(path)
	}

	fn socket(&self) -> PathBuf { self.0.join("listener.sock") }
}

impl Drop for Directory {
	fn drop(&mut self) { fs::remove_dir_all(&self.0).expect("owned socket fixture cleanup"); }
}

async fn listener(
	path: &Path,
	handle: &Handle<std::os::unix::net::SocketAddr>,
) -> Result<futures::future::BoxFuture<'static, std::io::Result<()>>> {
	let mut futures =
		serve(&Router::new(), handle, std::iter::empty(), Some(path), 0o600).await?;
	assert_eq!(futures.len(), 1);
	Ok(futures.pop().expect("one owned listener"))
}

#[tokio::test]
async fn dropped_unpolled_unix_listener_removes_owned_socket() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	let future = listener(&path, &Handle::new()).await?;
	assert!(
		fs::symlink_metadata(&path)?
			.file_type()
			.is_socket()
	);
	drop(future);
	assert!(!path.exists(), "unpolled listener left its socket path");
	Ok(())
}

#[tokio::test]
async fn cancelled_unix_listener_removes_owned_socket() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	let mut future = listener(&path, &Handle::new()).await?;
	assert!(poll!(future.as_mut()).is_pending());
	drop(future);
	assert!(!path.exists(), "cancelled listener left its socket path");
	Ok(())
}

#[test]
fn unix_listener_setup_panic_removes_owned_socket() {
	let directory = Directory::new();
	let path = directory.socket();
	let runtime = tokio::runtime::Builder::new_current_thread()
		.build()
		.expect("runtime without IO");
	let result =
		catch_unwind(AssertUnwindSafe(|| runtime.block_on(listener(&path, &Handle::new()))));
	assert!(result.is_err(), "conversion requires a Tokio IO driver");
	assert!(!path.exists(), "setup panic left a bound socket path");
}

async fn completed_listener_preserves_replacement(socket: bool) -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	let handle = Handle::new();
	let future = listener(&path, &handle).await?;
	fs::remove_file(&path)?;
	let replacement = if socket {
		Some(UnixListener::bind(&path)?)
	} else {
		fs::write(&path, b"another owner's data")?;
		None
	};
	handle.shutdown();
	timeout(Duration::from_secs(5), future)
		.await
		.expect("listener shutdown")?;
	assert!(path.exists(), "shutdown removed another owner's replacement");
	if socket {
		assert!(
			fs::symlink_metadata(&path)?
				.file_type()
				.is_socket()
		);
	} else {
		assert_eq!(fs::read(&path)?, b"another owner's data");
	}
	drop(replacement);
	Ok(())
}

#[tokio::test]
async fn unix_listener_completion_preserves_replacement_file() -> Result {
	completed_listener_preserves_replacement(false).await
}

#[tokio::test]
async fn unix_listener_completion_preserves_replacement_socket() -> Result {
	completed_listener_preserves_replacement(true).await
}

#[tokio::test]
async fn unix_listener_refuses_existing_regular_file() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	fs::write(&path, b"preserved data")?;
	let result = listener(&path, &Handle::new()).await;
	assert!(result.is_err(), "startup replaced an unrelated file");
	assert_eq!(fs::read(&path)?, b"preserved data");
	Ok(())
}

#[tokio::test]
async fn unix_listener_refuses_existing_symlink() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	let target = directory.0.join("data");
	fs::write(&target, b"preserved target")?;
	symlink(&target, &path)?;
	let result = listener(&path, &Handle::new()).await;
	assert!(result.is_err(), "startup replaced a symlink");
	assert!(
		fs::symlink_metadata(&path)?
			.file_type()
			.is_symlink()
	);
	assert_eq!(fs::read(&target)?, b"preserved target");
	Ok(())
}

#[tokio::test]
async fn unix_listener_refuses_existing_active_socket() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	let existing = UnixListener::bind(&path)?;
	let identity = fs::symlink_metadata(&path)?;
	let result = listener(&path, &Handle::new()).await;
	assert!(result.is_err(), "startup replaced an active listener");
	assert_eq!(fs::symlink_metadata(&path)?.ino(), identity.ino());
	drop(existing);
	Ok(())
}

#[tokio::test]
async fn unix_listener_recovers_stale_socket_and_cleans_normal_shutdown() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	drop(UnixListener::bind(&path)?);
	let handle = Handle::new();
	let future = listener(&path, &handle).await?;
	handle.shutdown();
	timeout(Duration::from_secs(5), future)
		.await
		.expect("stale socket recovery")?;
	assert!(!path.exists(), "normal shutdown removes the owned socket");
	Ok(())
}

#[tokio::test]
async fn dropped_passed_unix_listener_preserves_external_socket() -> Result {
	let directory = Directory::new();
	let path = directory.socket();
	let passed = UnixListener::bind(&path)?;
	passed.set_nonblocking(true)?;
	let futures =
		serve(&Router::new(), &Handle::new(), std::iter::once(passed), None, 0o600).await?;
	drop(futures);
	assert!(
		fs::symlink_metadata(&path)?
			.file_type()
			.is_socket(),
		"passed listener path is externally owned"
	);
	Ok(())
}
