use futures::TryStreamExt;
use tuwunel_core::{Result, utils::stream::IterStream};

use crate::admin_command;

#[admin_command]
pub(super) async fn database_files(&self, map: Option<String>, level: Option<i32>) -> Result {
	let mut files: Vec<_> = self
		.services
		.db
		.engine()?
		.file_list()
		.collect::<Result<_>>()?;

	files.sort_by_key(|f| f.name.clone());

	writeln!(self, "| lev  | sst  | keys | dels | size | column |").await?;
	writeln!(self, "| ---: | :--- | ---: | ---: | ---: | :---   |").await?;
	files
		.into_iter()
		.filter(|file| {
			map.as_deref()
				.is_none_or(|map| map == file.column_family_name)
		})
		.filter(|file| {
			level
				.as_ref()
				.is_none_or(|&level| level == file.level)
		})
		.try_stream()
		.try_for_each(|file| {
			writeln!(
				self,
				"| {} | {:<13} | {:7}+ | {:4}- | {:9} | {} |",
				file.level,
				file.name,
				file.num_entries,
				file.num_deletions,
				file.size,
				file.column_family_name,
			)
		})
		.await
}
