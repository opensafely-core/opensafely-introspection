import pathlib

import click
import pandas


def transform_status(status):
    status = status.str.lower()
    status.loc[status == "0"] = pandas.NA
    status = status.astype("category")
    return status


@click.command()
@click.argument("jobs_in", type=pathlib.Path)
@click.argument("jobs_out", type=pathlib.Path)
def main(jobs_in, jobs_out):
    jobs = pandas.read_csv(
        jobs_in,
        parse_dates=["created_at", "started_at", "updated_at", "completed_at"],
    )
    jobs.status = transform_status(jobs.status)
    jobs.to_feather(jobs_out)


if __name__ == "__main__":
    main()
