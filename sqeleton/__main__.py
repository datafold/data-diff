import click
from .repl import repl as repl_main


@click.group(no_args_is_help=True)
def main():
    pass


@main.command(no_args_is_help=True)
@click.argument("database", required=True)
def repl(database):
    return repl_main(database)


if __name__ == "__main__":
    main()
