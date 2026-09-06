use color_eyre::eyre::{Result, eyre};

use crate::cli::args::BackendArg;
use crate::config::Config;

/// Show a configured version alias for a tool
///
/// Reads the merged `[tool_alias.TOOL.versions]` configuration. This prints the
/// stored request, which may itself be a prefix. Backend-provided aliases are listed
/// by `mise tool-alias ls`; they are not entries returned by this command.
#[derive(Debug, usage_rs::Args)]
#[usage(after_long_help = AFTER_LONG_HELP, verbatim_doc_comment)]
pub(super) struct ToolAliasGet {
    /// The tool to show the alias for
    #[usage(value_name = "TOOL")]
    pub tool: BackendArg,
    /// The alias to show
    pub alias: String,
}

impl ToolAliasGet {
    pub(super) async fn run(self) -> Result<()> {
        let config = Config::get().await?;
        match config.all_aliases.get(&self.tool.short) {
            Some(alias) => match alias.versions.get(&self.alias) {
                Some(alias) => {
                    miseprintln!("{alias}");
                    Ok(())
                }
                None => Err(eyre!("Unknown alias: {}", &self.alias)),
            },
            None => Err(eyre!("Unknown tool: {}", &self.tool)),
        }
    }
}

static AFTER_LONG_HELP: &str = color_print::cstr!(
    r#"<bold><underline>Examples:</underline></bold>

    $ <bold>mise tool-alias set node project 20</bold>
    $ <bold>mise tool-alias get node project</bold>
    20
"#
);
