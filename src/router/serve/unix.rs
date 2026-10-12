#![cfg(unix)]

#[cfg(test)]
mod tests;

use std::{
	fs,
	io::{self, ErrorKind},
	net::{IpAddr, Ipv4Addr, SocketAddr},
	os::unix::{
		self,
		fs::{FileTypeExt, MetadataExt, PermissionsExt},
		net::UnixListener,
	},
	path::{Path, PathBuf},
	time::Duration,
};

use axum::{Extension, Router, extract::ConnectInfo};
use axum_server::Handle;
use futures::{FutureExt, future::BoxFuture};
use tuwunel_core::{Result, err, warn};

#[tracing::instrument(skip_all, level = "debug")]
pub(super) async fn serve<'a>(
	router: &Router,
	handle: &Handle<unix::net::SocketAddr>,
	listeners: impl Iterator<Item = UnixListener>,
	path: Option<&Path>,
	socket_perms: u32,
) -> Result<Vec<BoxFuture<'a, Result<(), io::Error>>>> {
	// Loopback so a unix-socket peer bypasses a configured `ip_source`.
	let router = router
		.clone()
		.layer(Extension(ConnectInfo(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))))
		.into_make_service();

	let mut acceptors = listeners
		.map(|listener| {
			Ok(axum_server::from_unix(listener)?
				.handle(handle.clone())
				.serve(router.clone())
				.boxed())
		})
		.collect::<Result<Vec<_>>>()?;

	if let Some(path) = path {
		remove_stale(path).await?;

		let unix_listener = UnixListener::bind(path).map_err(|e| {
			err!(Config("unix_socket_path", "Failed to bind UNIX socket at {path:?}: {e}",))
		})?;
		let owned_path = OwnedSocket::new(path)?;

		unix_listener.set_nonblocking(true)?;

		let perms = fs::Permissions::from_mode(socket_perms);
		fs::set_permissions(path, perms).map_err(|e| {
			err!(Config(
				"unix_socket_path",
				"Failed to set permissions {socket_perms:o} on UNIX socket at {path:?}: {e}",
			))
		})?;

		let bound_acceptor = axum_server::from_unix(unix_listener)?
			.handle(handle.clone())
			.serve(router);
		// Capture ownership before polling: dropping an unpolled future, a
		// cancelled listener or failed later listener setup must all clean up.
		let bound_acceptor = async move {
			let _owned_path = owned_path;
			bound_acceptor.await
		}
		.boxed();

		acceptors.push(bound_acceptor);
	}

	Ok(acceptors)
}

/// The pathname belongs to this listener only while its socket identity
/// matches. Never unlink a replacement file, symlink or another listener's
/// socket.
struct OwnedSocket {
	path: PathBuf,
	device: u64,
	inode: u64,
}

impl OwnedSocket {
	fn new(path: &Path) -> io::Result<Self> {
		let metadata = fs::symlink_metadata(path)?;
		if !metadata.file_type().is_socket() {
			return Err(io::Error::other("Bound UNIX socket path changed during setup"));
		}
		Ok(Self {
			path: path.to_owned(),
			device: metadata.dev(),
			inode: metadata.ino(),
		})
	}

	fn remove(&self) -> io::Result<()> {
		match fs::symlink_metadata(&self.path) {
			| Ok(metadata)
				if metadata.file_type().is_socket()
					&& metadata.dev() == self.device
					&& metadata.ino() == self.inode =>
				fs::remove_file(&self.path),
			| Ok(_) => Ok(()),
			| Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
			| Err(error) => Err(error),
		}
	}
}

impl Drop for OwnedSocket {
	fn drop(&mut self) {
		if let Err(error) = self.remove() {
			warn!("Failed to remove owned UNIX socket {:?}: {error}", self.path);
		}
	}
}

async fn remove_stale(path: &Path) -> Result {
	let original = match fs::symlink_metadata(path) {
		| Ok(metadata) => metadata,
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
		| Err(error) => return Err(error.into()),
	};
	if !original.file_type().is_socket() {
		return Err(err!(Config(
			"unix_socket_path",
			"Refusing to replace a non-socket at {path:?}"
		)));
	}
	let probe =
		tokio::time::timeout(Duration::from_secs(1), tokio::net::UnixStream::connect(path))
			.await
			.map_err(|_| {
				err!(Config("unix_socket_path", "Timed out checking socket at {path:?}"))
			})?;
	match probe {
		| Ok(_) =>
			return Err(err!(Config(
				"unix_socket_path",
				"A listener is already active at {path:?}"
			))),
		| Err(error) if error.kind() == ErrorKind::ConnectionRefused => {},
		| Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
		| Err(error) =>
			return Err(err!(Config(
				"unix_socket_path",
				"Cannot verify stale socket at {path:?}: {error}"
			))),
	}
	let current = fs::symlink_metadata(path)?;
	if !current.file_type().is_socket()
		|| current.dev() != original.dev()
		|| current.ino() != original.ino()
	{
		return Err(err!(Config(
			"unix_socket_path",
			"Socket changed during stale check at {path:?}"
		)));
	}
	warn!("Removing stale UNIX socket {path:?}...");
	fs::remove_file(path)?;
	Ok(())
}
