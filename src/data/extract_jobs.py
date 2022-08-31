import csv
import dataclasses
import functools
import pathlib
import shlex

import click
import structlog
from pipeline import ProjectValidationError, load_pipeline
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine, select


log = structlog.get_logger()


@dataclasses.dataclass
class Job:
    id: int  # noqa: A003
    status: str
    status_code: str
    status_message: str
    created_at: str
    started_at: str
    updated_at: str
    completed_at: str
    identifier: str
    job_request_id: int
    workspace_id: int
    action_id: str
    action_type: str | None = None
    action_version: str | None = None


def job_factory(row):
    attrs = row._asdict()

    if (action := action_factory(row)) is not None:
        attrs["action_type"] = action.type
        attrs["action_version"] = action.version

    return Job(**attrs)


@dataclasses.dataclass
class Action:
    id: str  # noqa: A003
    type: str  # noqa: A003
    version: str


def action_factory(row):
    if (pipeline := extract_pipeline(row["job_request_id"])) is not None:
        action_id = row["action_id"]
        if (action := pipeline.actions.get(action_id)) is not None:
            type_, version = shlex.split(action.run.raw)[0].split(":")
            return Action(action_id, type_, version)


@functools.cache
def extract_pipeline(job_request_id):
    jobrequest = metadata.tables["jobserver_jobrequest"]
    query = select(
        jobrequest.c.project_definition.label("pipeline"),
        jobrequest.c.identifier,
    ).where(jobrequest.c.id == job_request_id)
    with engine.connect() as connection:
        result = connection.execute(query)
        row = result.one()

    pipeline = row["pipeline"]
    identifier = row["identifier"]
    if pipeline:
        try:
            return load_pipeline(pipeline)
        except (AttributeError, ProjectValidationError, ValidationError):
            log.msg(f"Cannot parse pipeline in job request {identifier}")
    else:
        log.msg(f"Job request {identifier} is missing a pipeline")


def extract_jobs():
    job = metadata.tables["jobserver_job"]
    jobrequest = metadata.tables["jobserver_jobrequest"]
    query = select(
        # Job
        job.c.id,
        job.c.status,
        job.c.status_code,
        job.c.status_message,
        job.c.created_at,
        job.c.started_at,
        job.c.updated_at,
        job.c.completed_at,
        job.c.identifier,
        job.c.job_request_id,
        jobrequest.c.workspace_id,
        # Action
        job.c.action.label("action_id"),
    ).join_from(job, jobrequest)
    with engine.connect() as connection:
        result = connection.execute(query)
        yield from (job_factory(row) for row in result)


def load_jobs(jobs, path):
    with open(path, mode="w", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(f.name for f in dataclasses.fields(Job))
        writer.writerows(dataclasses.astuple(j) for j in jobs)


@click.command()
@click.argument("dsn")
@click.argument("output", type=pathlib.Path)
def main(dsn, output):
    global engine
    global metadata
    engine = create_engine(dsn, future=True)
    metadata = MetaData()
    metadata.reflect(engine)

    jobs = extract_jobs()
    load_jobs(jobs, output)


if __name__ == "__main__":
    main()
