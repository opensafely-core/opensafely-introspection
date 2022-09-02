import pathlib

import click
import pandas


@click.command()
@click.argument("jobs_in", type=pathlib.Path)
@click.argument("jobs_out", type=pathlib.Path)
def main(jobs_in, jobs_out):
    jobs = pandas.read_csv(
        jobs_in,
        parse_dates=["created_at", "started_at", "updated_at", "completed_at"],
    )
    jobs.to_feather(jobs_out)


if __name__ == "__main__":
    main()
