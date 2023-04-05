import click
from .repl import repl as repl_main


@click.group(no_args_is_help=True)
def main():
    pass


@main.command(no_args_is_help=True)
@click.argument("database", required=True)
def repl(database):
    return repl_main(database)


CONN_EDITOR_HELP = """CONFIG_PATH - Path to a TOML config file of db connections, new or existing."""


@main.command(no_args_is_help=True, help=CONN_EDITOR_HELP)
@click.argument("config_path", required=True)
def conn_editor(config_path):
    from .conn_editor import main

    return main(config_path)


if __name__ == "__main__":
    main()
