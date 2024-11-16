from sqlalchemy import Integer, and_, func, text, insert, select, update
from database import sync_engine, async_engine
from models import Workload, metadata_obj, workers_table, resumes_table


def get_123():
    with sync_engine.connect() as conn:
        res = conn.execute(text("SELECT VERSION()"))
        print(f"{res.first()=}")


async def get_123():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT VERSION()"))
        print(f"{res.first()=}")


class SyncCore:
    @staticmethod
    def create_tables():
        sync_engine.echo = False
        metadata_obj.drop_all(sync_engine)
        metadata_obj.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_workers():
        with sync_engine.connect() as conn:
            # stmt = """INSERT INTO workers (username) VALUES
            # 	('Bobr'),
            # 	('Volk');"""
            stmt = insert(workers_table).values(
                [
                    {"username": "Jack"},
                    {"username": "Michael"},
                ]
            )
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def select_workers():
        with sync_engine.connect() as conn:
            query = select(workers_table)  # SELECT * FROM workers
            result = conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        with sync_engine.connect() as conn:
            # stmt = text("UPDATE workers SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(workers_table).values(username=new_username)
                # .where(workers_table.c.id==worker_id)
                .filter_by(id=worker_id)
            )
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def insert_resumes():
        with sync_engine.connect() as conn:
            resumes = [
                {
                    "title": "Python Junior Developer",
                    "compensation": 50000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python разработчик",
                    "compensation": 150000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python Data Engineer",
                    "compensation": 250000,
                    "workload": Workload.fulltime,
                    "worker_id": 2,
                },
                {
                    "title": "Data Scientist",
                    "compensation": 300000,
                    "workload": Workload.fulltime,
                    "worker_id": 2,
                },
            ]
            stmt = insert(resumes_table).values(resumes)
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def select_resumes_ang_compensation(like_language: str = "Python"):
        """
        SELECT workload, avg(compensation)::int as avg_compensation
        FROM resumes
        WHERE title like '%Python%' and compensation > 40000
        GROUP BY workload
        """
        with sync_engine.connect() as conn:
            query = (
                select(
                    resumes_table.c.workload,
                    func.avg(resumes_table.c.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(resumes_table)
                .filter(
                    and_(
                        resumes_table.c.title.contains(like_language),
                        resumes_table.compensation > 40000,
                    )
                )
                .group_by(resumes_table.c.workload)
                .having(func.avg(resumes_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = conn.execute(query)
            result = res.all()
            print(result[0].avg_compensation)

    def insert_additional_resumes():
        with sync_engine.connect() as conn:
            workers = [
                {"username": "Artem"},  # 3
                {"username": "Roman"},  # 4
                {"username": "Petr"},  # 5
            ]
            resumes = [
                {
                    "title": "Python программист",
                    "compensation": 60000,
                    "workload": "fulltime",
                    "worker_id": 3,
                },
                {
                    "title": "Machine Learning Engineer",
                    "compensation": 70000,
                    "workload": "parttime",
                    "worker_id": 3,
                },
                {
                    "title": "Python Data Scientist",
                    "compensation": 80000,
                    "parttime": "fulltime",
                    "worker_id": 4,
                },
                {
                    "title": "Python Analyst",
                    "compensation": 90000,
                    "workload": "fulltime",
                    "worker_id": 4,
                },
                {
                    "title": "Python Junior Developer",
                    "compensation": 100000,
                    "workload": "fulltime",
                    "worker_id": 5,
                },
            ]
            insert_workers = insert(workers_table).values(workers)
            insert_resumes = insert(resumes_table).values(resumes)
            conn.execute(insert_workers)
            conn.execute(insert_resumes)
            conn.commit()


class AsyncCore:
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(metadata_obj.drop_all)
            await conn.run_sync(metadata_obj.create_all)

    @staticmethod
    async def insert_workers():
        async with async_engine.connect() as conn:
            stmt = insert(workers_table).values(
                [
                    {"username": "Jack"},
                    {"username": "Michael"},
                ]
            )
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_workers():
        async with async_engine.connect() as conn:
            query = select(workers_table)  # SELECT * FROM workers
            result = await conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        async with async_engine.connect() as conn:
            # stmt = text("UPDATE workers SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(workers_table).values(username=new_username)
                # .where(workers_table.c.id==worker_id)
                .filter_by(id=worker_id)
            )
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def insert_resumes():
        async with async_engine.connect() as conn:
            resumes = [
                {
                    "title": "Python Junior Developer",
                    "compensation": 50000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python разработчик",
                    "compensation": 150000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python Data Engineer",
                    "compensation": 250000,
                    "workload": Workload.fulltime,
                    "worker_id": 2,
                },
                {
                    "title": "Data Scientist",
                    "compensation": 300000,
                    "workload": Workload.fulltime,
                    "worker_id": 2,
                },
            ]
            stmt = insert(resumes_table).values(resumes)
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_resumes_ang_compensation(like_language: str = "Python"):
        """
        SELECT workload, avg(compensation)::int as avg_compensation
        FROM resumes
        WHERE title like '%Python%' and compensation > 40000
        GROUP BY workload
        """
        async with async_engine.connect() as conn:
            query = (
                select(
                    resumes_table.c.workload,
                    func.avg(resumes_table.c.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(resumes_table)
                .filter(
                    and_(
                        resumes_table.c.title.contains(like_language),
                        resumes_table.compensation > 40000,
                    )
                )
                .group_by(resumes_table.c.workload)
                .having(func.avg(resumes_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await conn.execute(query)
            result = res.all()
            print(result[0].avg_compensation)

    @staticmethod
    async def insert_additional_resumes():
        async with async_engine.connect() as conn:
            workers = [
                {"username": "Artem"},  # 3
                {"username": "Roman"},  # 4
                {"username": "Petr"},  # 5
            ]
            resumes = [
                {
                    "title": "Python программист",
                    "compensation": 60000,
                    "workload": "fulltime",
                    "worker_id": 3,
                },
                {
                    "title": "Machine Learning Engineer",
                    "compensation": 70000,
                    "workload": "parttime",
                    "worker_id": 3,
                },
                {
                    "title": "Python Data Scientist",
                    "compensation": 80000,
                    "parttime": "fulltime",
                    "worker_id": 4,
                },
                {
                    "title": "Python Analyst",
                    "compensation": 90000,
                    "workload": "fulltime",
                    "worker_id": 4,
                },
                {
                    "title": "Python Junior Developer",
                    "compensation": 100000,
                    "workload": "fulltime",
                    "worker_id": 5,
                },
            ]
            insert_workers = insert(workers_table).values(workers)
            insert_resumes = insert(resumes_table).values(resumes)
            await conn.execute(insert_workers)
            await conn.execute(insert_resumes)
            await conn.commit()
