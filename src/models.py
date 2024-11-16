import datetime
from typing import Annotated
from sqlalchemy import Enum, Table, Column, Integer, String, MetaData, ForeignKey, text, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column
from database import Base, str_256
import enum


intpk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[
    datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))
]
updated_at = Annotated[
    datetime.datetime,
    mapped_column(server_default=text("TIMEZONE('utc', now())"), onupdate=datetime.datetime.now),
]


class WorkersOrm(Base):
    __tablename__ = "workers"

    id: Mapped[intpk]
    username: Mapped[str]


class Workload(enum.Enum):
    parttime = "parttime"
    fulltime = "fulltime"


class ResumesOrm(Base):
    __tablename__ = "resumes"

    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[int | None]
    workload: Mapped[Workload]
    worker_id: Mapped[int] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"))
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]


metadata_obj = MetaData()

workers_table = Table(
    "workers", metadata_obj, Column("id", Integer, primary_key=True), Column("username", String)
)

resumes_table = Table(
    "resumes",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("title", String(256)),
    Column("compensation", Integer, nullable=True),
    Column("workload", Enum(Workload)),
    Column("worker_id", ForeignKey("worker.id", ondelete="CASCADE")),
    Column("create_at", TIMESTAMP, server_default=text("TIMEZONE('utc', now())")),
    Column(
        "update_at",
        TIMESTAMP,
        server_default=text("TIMEZONE('utc', now())"),
        onupdate=datetime.datetime.now,
    ),
)
