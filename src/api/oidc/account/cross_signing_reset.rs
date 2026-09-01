use const_str::format as const_format;
use ruma::UserId;
use tuwunel_core::{Result, info, utils::html::escape as html_escape};
use tuwunel_service::Services;

use super::{ACCOUNT_HEAD, url_encode};

/// Shows a POST confirmation form for a cross-signing reset. The `login_token`
/// is peeked by the GET handler and embedded here; submitting the form consumes
/// it and opens the replacement window.
pub(super) async fn cross_signing_reset_confirm_html(
	user_id: &UserId,
	login_token: &str,
) -> Result<String> {
	let uid = html_escape(user_id.as_str());
	let tok = html_escape(login_token);
	let tok_enc = url_encode(login_token);

	Ok(CONFIRM_HTML
		.replace("{uid}", &uid)
		.replace("{tok}", &tok)
		.replace("{tok_enc}", &tok_enc))
}

/// Opens the ten-minute window during which the user's client may upload a new
/// cross-signing identity without further interactive authentication (MSC4312).
pub(super) async fn cross_signing_reset_execute_html(
	services: &Services,
	user_id: &UserId,
) -> Result<String> {
	services
		.users
		.allow_cross_signing_replacement(user_id)
		.await;

	info!(?user_id, "Cross-signing reset approved via account management page");

	Ok(EXECUTE_HTML.replace("{uid}", &html_escape(user_id.as_str())))
}

static CONFIRM_HTML: &str = const_format!(
	r#"
<!DOCTYPE html>
<html lang="en">
	<head>
		{ACCOUNT_HEAD}
		<title>Reset Cross-Signing</title>
	</head>
	<body>
		<h1>Reset Cross-Signing</h1>
		<p>
			Signed in as <strong>{{uid}}</strong>.
		</p>
		<p class="warn">
			Reset your cross-signing identity? After you approve, your client can
			upload a new identity for the next ten minutes. Other users and your
			other sessions will need to verify you again.
		</p>
		<form method="POST" action="/_tuwunel/oidc/account_callback">
			<input type="hidden" name="action" value="org.matrix.cross_signing_reset">
			<input type="hidden" name="loginToken" value="{{tok}}">
			<button type="submit" class="danger">Reset cross-signing</button>
			<a
				class="cancel"
				href="/_tuwunel/oidc/account_callback?action=org.matrix.sessions_list&loginToken={{tok_enc}}"
			>
				Cancel
			</a>
		</form>
	</body>
</html>"#
);

static EXECUTE_HTML: &str = const_format!(
	r#"
<!DOCTYPE html>
<html lang="en">
	<head>
		{ACCOUNT_HEAD}
		<title>Cross-Signing Reset Approved</title>
	</head>
	<body>
		<h1 class="ok">Cross-Signing Reset Approved</h1>
		<p>
			You can now upload a new cross-signing identity for
			<strong>{{uid}}</strong> from your Matrix client. This approval expires
			in ten minutes.
		</p>
		<div class="nav">
			<a href="/_tuwunel/oidc/account?action=org.matrix.sessions_list">
				Back to sessions
			</a>
		</div>
	</body>
</html>"#
);
