use ruma::OwnedServerName;
use tuwunel_core::Result;
use tuwunel_service::federation::Classification;

use crate::admin_command;

#[admin_command]
pub(super) async fn peer_status_record_failure(
	&self,
	server_name: OwnedServerName,
	permanent: bool,
) -> Result {
	let classification = permanent
		.then_some(Classification::Permanent)
		.unwrap_or(Classification::Transient);

	self.services
		.federation
		.record_failure(&server_name, classification)
		.await?;

	write!(self, "Recorded {classification:?} failure for {server_name} in current bucket.").await
}
