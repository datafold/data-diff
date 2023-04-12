# Usage Analytics & Data Privacy

data-diff collects anonymous usage data to help our team improve the tool and to apply development efforts to where our users need them most.

We capture two events: one when the data-diff run starts, and one when it is finished. No user data or potentially sensitive information is or ever will be collected. The captured data is limited to:

- Operating System and Python version
- Types of databases used (postgresql, mysql, etc.)
- Sizes of tables diffed, run time, and diff row count (numbers only)
- Error message, if any, truncated to the first 20 characters.
- A persistent UUID to indentify the session, stored in `~/.datadiff.toml`

To disable, use one of the following methods:

* **CLI**: use the `--no-tracking` flag.
* **Config file**: set `no_tracking = true` (for example, under `[run.default]`)
* **Python API**:
    ```python
    import data_diff
    # Invoke the following before making any API calls
    data_diff.disable_tracking()
    ```