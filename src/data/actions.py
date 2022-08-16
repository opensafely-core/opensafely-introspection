import pathlib

import click
import pandas


@click.command()
@click.argument("actions_in", type=pathlib.Path)
@click.argument("actions_out", type=pathlib.Path)
def main(actions_in, actions_out):
    actions = pandas.read_csv(actions_in)
    actions.to_feather(actions_out)


if __name__ == "__main__":
    main()
