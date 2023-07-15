# Usage Analytics & Data Privacy

data-diff collects anonymous usage data to help our team improve the tool and to apply development efforts to where our users need them most.

We capture two events: one when the data-diff run starts, and one when it is finished. No user data or potentially sensitive information is or ever will be collected. The captured data is limited to:

- Operating System and Python version
- Types of databases used (postgresql, mysql, etc.)
- Sizes of tables diffed, run time, and diff row count (numbers only)
- Error message, if any, truncated to the first 20 characters.
- A persistent UUID to indentify the session, stored in `~/.datadiff.toml`
- IP address of the machine running diff

When using the `--dbt` feature, we also collect:

- dbt generated UUIDs (user_id and project_id)
- dbt-core version (e.g. 1.2.0)
- Users can also choose to provide an email address
     - When tracking is not disabled, we will prompt the user once to opt-in to release notifications
        - Users can decide not to opt-in by leaving the prompt blank

To disable, use one of the following methods:

* **CLI**: use the `--no-tracking` flag.
* **Config file**: set `no_tracking = true` (for example, under `[run.default]`)
* **Python API**:
    ```python
    import data_diff
    # Invoke the following before making any API calls
    data_diff.disable_tracking()
    ```
