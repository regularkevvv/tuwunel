use std::{fmt::Debug, mem};

use bytes::{Bytes, BytesMut};
use reqwest::Request;
use ruma::api::{
	IncomingResponse, OutgoingRequest, OutgoingRequestExt,
	appservice::Registration,
	auth_scheme::{AuthScheme, SendAccessToken},
	path_builder::PathBuilder,
};
use tuwunel_core::{
	Err, Result, debug_error, err, implement, trace, utils::string_from_bytes, warn,
};

use crate::client::read_response_capped;

/// Sends a request to an appservice
///
/// Only returns Ok(None) if there is no url specified in the appservice
/// registration file
#[implement(super::Service)]
pub async fn send_request<T>(
	&self,
	registration: Registration,
	request: T,
) -> Result<Option<T::IncomingResponse>>
where
	T: OutgoingRequest + Debug + Send,
	for<'a> T::Authentication: AuthScheme<Input<'a> = SendAccessToken<'a>>,
	for<'a> T::PathBuilder: PathBuilder<Input<'a> = ()>,
{
	let services_guard = self.services.get();
	let services_root = services_guard.as_ref();

	let client = &services_root.client.appservice;

	let Some(dest) = registration.url else {
		return Ok(None);
	};

	if dest == *"null" || dest.is_empty() {
		return Ok(None);
	}

	trace!("Appservice URL \"{dest}\", Appservice ID: {}", registration.id);

	let hs_token = registration.hs_token.as_str();
	let mut http_request = request
		.try_into_http_request::<BytesMut>(&dest, SendAccessToken::IfRequired(hs_token), ())
		.map_err(|e| {
			err!(BadServerResponse(
				warn!(appservice = %registration.id, "Failed to find destination {dest}: {e:?}")
			))
		})?
		.map(BytesMut::freeze);

	add_access_token_query(&mut http_request, hs_token);

	let reqwest_request = Request::try_from(http_request)?;

	let mut response = execute_request(client, reqwest_request)
		.await
		.map_err(|e| {
			warn!(
				"Could not send request to appservice \"{}\" at {dest}: {e:?}",
				registration.id
			);
			e
		})?;

	// reqwest::Response -> http::Response conversion
	let status = response.status();
	let mut http_response_builder = http::Response::builder()
		.status(status)
		.version(response.version());

	mem::swap(
		response.headers_mut(),
		http_response_builder
			.headers_mut()
			.expect("http::response::Builder is usable"),
	);

	let limit = services_root.config.max_response_size;
	let body = read_response_capped(response, limit)
		.await
		.map_err(|e| {
			err!(BadServerResponse(warn!(
				"Failed reading response from appservice \"{}\" at {dest}: {e}",
				registration.id
			)))
		})?;

	if !status.is_success() {
		debug_error!("Appservice response bytes: {:?}", string_from_bytes(&body));
		return Err!(BadServerResponse(warn!(
			"Appservice \"{}\" returned unsuccessful HTTP response {status} at {dest}",
			registration.id
		)));
	}

	let response = T::IncomingResponse::try_from_http_response(
		http_response_builder
			.body(body)
			.expect("reqwest body is valid http body"),
	);

	response.map(Some).map_err(|e| {
		err!(BadServerResponse(warn!(
			"Appservice \"{}\" returned invalid/malformed response bytes {dest}: {e}",
			registration.id
		)))
	})
}

/// Appends the `hs_token` as an `access_token` query parameter, the legacy
/// authentication scheme some appservices still require.
pub(super) fn add_access_token_query(request: &mut http::Request<Bytes>, hs_token: &str) {
	let mut parts = request.uri().clone().into_parts();
	let old_path_and_query = parts
		.path_and_query
		.expect("valid request uri path and query")
		.as_str()
		.to_owned();

	let symbol = if old_path_and_query.contains('?') { "&" } else { "?" };

	parts.path_and_query = Some(
		format!("{old_path_and_query}{symbol}access_token={hs_token}")
			.parse()
			.expect("valid path and query"),
	);

	*request.uri_mut() = parts
		.try_into()
		.expect("our manipulation is always valid");
}

/// Strip query-bearing URLs before appservice transport errors can be logged
/// or propagated. The request, authentication, response and error kind/source
/// are otherwise unchanged. Both ordinary delivery and ping use this boundary.
pub(super) async fn execute_request(
	client: &reqwest::Client,
	request: Request,
) -> reqwest::Result<reqwest::Response> {
	client
		.execute(request)
		.await
		.map_err(reqwest::Error::without_url)
}

#[cfg(test)]
mod tests {
	use std::{future::pending, time::Duration};

	use tokio::{
		io::{AsyncReadExt, AsyncWriteExt},
		net::TcpListener,
		sync::oneshot,
		task::JoinHandle,
	};

	use super::{Bytes, Request, add_access_token_query, execute_request};

	const TOKEN: &str = "disposable-appservice-legacy-query-credential";

	struct OwnedPeer(JoinHandle<()>);
	impl Drop for OwnedPeer {
		fn drop(&mut self) { self.0.abort(); }
	}

	fn request(base: &str, method: &str) -> Request {
		let mut request = http::Request::builder()
			.method(method)
			.uri(format!("{base}/_matrix/app/v1/test"))
			.header(http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
			.body(Bytes::new())
			.expect("owned HTTP request");
		add_access_token_query(&mut request, TOKEN);
		Request::try_from(request).expect("owned reqwest request")
	}

	async fn peer(reply: bool) -> (String, oneshot::Receiver<Vec<u8>>, OwnedPeer) {
		let listener = TcpListener::bind("127.0.0.1:0")
			.await
			.expect("owned listener");
		let base = format!("http://{}", listener.local_addr().expect("owned address"));
		let (send, receive) = oneshot::channel();
		let task = tokio::spawn(async move {
			let (mut socket, _) = listener.accept().await.expect("owned peer");
			let mut wire = Vec::new();
			let mut chunk = [0; 1024];
			while !wire.windows(4).any(|w| w == b"\r\n\r\n") {
				let n = socket
					.read(&mut chunk)
					.await
					.expect("owned request read");
				assert!(n > 0 && wire.len() + n <= 16 * 1024, "bounded request headers");
				wire.extend_from_slice(&chunk[..n]);
			}
			send.send(wire)
				.expect("request capture remains owned");
			if reply {
				socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.expect("owned response");
			} else {
				pending::<()>().await;
			}
		});
		(base, receive, OwnedPeer(task))
	}

	#[tokio::test]
	async fn appservice_execute_timeout_redacts_url_before_logging_and_propagation() {
		for method in ["PUT", "POST"] {
			let (base, mut captured, _peer) = peer(false).await;
			let client = reqwest::Client::builder()
				.no_proxy()
				.timeout(Duration::from_secs(1))
				.build()
				.expect("owned client");
			let error = tokio::time::timeout(
				Duration::from_secs(10),
				execute_request(&client, request(&base, method)),
			)
			.await
			.expect("bounded transport")
			.expect_err("held peer times out");
			let wire = captured
				.try_recv()
				.expect("actual request was captured");
			let wire = String::from_utf8(wire).expect("HTTP headers are UTF-8");
			assert!(
				wire.contains(&format!("access_token={TOKEN}")),
				"legacy credential remains on wire"
			);
			assert!(
				wire.to_ascii_lowercase()
					.contains(&format!("authorization: bearer {TOKEN}")),
				"header credential remains on wire"
			);
			assert!(
				error.is_timeout() && error.url().is_none(),
				"timeout classification retained without URL"
			);
			assert!(
				!format!("{error:?} {error}").contains(TOKEN),
				"transport formatting contains no credential"
			);
			let propagated = tuwunel_core::Error::from(error);
			assert!(
				matches!(&propagated, tuwunel_core::Error::Reqwest(e) if e.is_timeout() && e.url().is_none())
			);
			assert!(
				!format!(
					"{propagated:?} {propagated} {}",
					tuwunel_core::error::error_chain(&propagated)
				)
				.contains(TOKEN),
				"propagated formatting contains no credential"
			);
		}
	}

	#[tokio::test]
	async fn appservice_execute_connect_error_redacts_url_and_retains_class() {
		let listener = TcpListener::bind("127.0.0.1:0")
			.await
			.expect("owned listener");
		let base = format!("http://{}", listener.local_addr().expect("owned address"));
		drop(listener);
		let client = reqwest::Client::builder()
			.no_proxy()
			.timeout(Duration::from_secs(2))
			.build()
			.expect("owned client");
		let error = tokio::time::timeout(
			Duration::from_secs(10),
			execute_request(&client, request(&base, "PUT")),
		)
		.await
		.expect("bounded transport")
		.expect_err("closed peer refuses connection");
		assert!(
			error.is_connect() && error.url().is_none(),
			"connect classification retained without URL"
		);
		assert!(
			!format!("{error:?} {error}").contains(TOKEN),
			"connect formatting contains no credential"
		);
	}

	#[tokio::test]
	async fn appservice_execute_preserves_http_failure_response_for_existing_retry_policy() {
		let (base, mut captured, _peer) = peer(true).await;
		let client = reqwest::Client::builder()
			.no_proxy()
			.timeout(Duration::from_secs(2))
			.build()
			.expect("owned client");
		let response = tokio::time::timeout(
			Duration::from_secs(10),
			execute_request(&client, request(&base, "PUT")),
		)
		.await
		.expect("bounded transport")
		.expect("HTTP failure remains a response");
		assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
		let _wire = captured
			.try_recv()
			.expect("actual request was captured");
		assert_eq!(
			response
				.bytes()
				.await
				.expect("owned response body")
				.as_ref(),
			b"{}"
		);
	}
}
