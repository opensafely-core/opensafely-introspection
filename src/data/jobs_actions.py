import csv
import dataclasses
import functools
import itertools
import pathlib
import shlex

import click
import structlog
from pipeline import ProjectValidationError, load_pipeline
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine, select


log = structlog.get_logger()


open_csv = functools.partial(open, mode="w", newline="")


def fields_to_attrs(prefix, row):
    return {
        k.removeprefix(prefix): v
        for k, v in row._asdict().items()
        if k.startswith(prefix)
    }


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


def job_factory(row):
    attrs = fields_to_attrs("job_", row)
    return Job(**attrs)


@dataclasses.dataclass
class Action:
    id: str  # noqa: A003
    pseudo_id: str
    job_id: int
    type: str  # noqa: A003
    version: str


def action_factory(row):
    pipeline = extract_pipeline(row["job_job_request_id"])
    if not pipeline:
        return None

    pseudo_action_id = row["pseudo_action_id"]

    if pseudo_action_id == "__error__":
        return None

    if pseudo_action_id == "run_all":
        actions = pipeline.actions.items()
    else:
        actions = [(pseudo_action_id, pipeline.actions[pseudo_action_id])]

    for action_id, action in actions:
        type_, version = shlex.split(action.run.raw)[0].split(":")
        yield Action(action_id, pseudo_action_id, row["job_id"], type_, version)


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


def extract_jobs_actions():
    job = metadata.tables["jobserver_job"]
    jobrequest = metadata.tables["jobserver_jobrequest"]
    query = select(
        # Job
        job.c.id.label("job_id"),
        job.c.status.label("job_status"),
        job.c.status_code.label("job_status_code"),
        job.c.status_message.label("job_status_message"),
        job.c.created_at.label("job_created_at"),
        job.c.started_at.label("job_started_at"),
        job.c.updated_at.label("job_updated_at"),
        job.c.completed_at.label("job_completed_at"),
        job.c.identifier.label("job_identifier"),
        job.c.job_request_id.label("job_job_request_id"),
        jobrequest.c.workspace_id.label("job_workspace_id"),
        # Action
        job.c.action.label("pseudo_action_id"),
    ).join_from(job, jobrequest)
    with engine.connect() as connection:
        result = connection.execute(query)
        for row in result:
            job = job_factory(row)
            actions = action_factory(row)
            yield from itertools.zip_longest([job], actions)


def load_jobs_actions(jobs_actions, jobs_path, actions_path):
    with open_csv(jobs_path) as jobs_fp, open_csv(actions_path) as actions_fp:
        jobs_writer = csv.writer(jobs_fp)
        actions_writer = csv.writer(actions_fp)

        jobs_writer.writerow(f.name for f in dataclasses.fields(Job))
        actions_writer.writerow(f.name for f in dataclasses.fields(Action))

        for job, action in jobs_actions:
            if job:
                jobs_writer.writerow(dataclasses.astuple(job))
            if action:
                actions_writer.writerow(dataclasses.astuple(action))


@click.command()
@click.argument("dsn")
@click.argument("jobs", type=pathlib.Path)
@click.argument("actions", type=pathlib.Path)
def main(dsn, jobs, actions):
    global engine
    global metadata
    engine = create_engine(dsn, future=True)
    metadata = MetaData()
    metadata.reflect(engine)

    jobs_actions = extract_jobs_actions()
    load_jobs_actions(jobs_actions, jobs, actions)


if __name__ == "__main__":
    main()
