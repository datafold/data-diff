# Connection Editor

A common complaint among new users was the difficulty in setting up the connections.

Connection URLs are admittedly confusing, and editing `.toml` files isn't always straight-forward either.

To ease this initial difficulty, we added a `textual`-based TUI tool to sqeleton, that allows users to edit configuration files and test the connections while editing them.

## Install

This tool needs `textual` to run. You can install it using:

```bash
pip install 'sqeleton[tui]'
```

Make sure you also have drivers for whatever database connection you're going to edit!

## Run

Once everything is installed, you can run the editor with the following command:

```bash
sqeleton conn-editor <conf_path.toml>
```

Example:

```bash
sqeleton conn-editor ~/dbs.toml
```

The available actions and hotkeys will be listed in the status bar.

Note: using the connection editor will delete comments and reformat the file!

We recommend backing up the configuration file before editing it.