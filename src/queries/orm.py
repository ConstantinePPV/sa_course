from sqlalchemy import Integer, and_, text, insert, select, inspect, func, cast
from database import sync_engine, async_engine,session_factory, async_session_factory, Base
from models import ResumesOrm, WorkerOrm, Workload


class SyncORM:
	@staticmethod
	def create_tables():
		sync_engine.echo = False
		Base.metadata.drop_all(sync_engine)
		Base.metadata.create_all(sync_engine)
		sync_engine.echo = True

	@staticmethod
	def insert_workers():	
		with session_factory() as session:
			worker_jack = WorkerOrm(username="Jack")
			worker_michael = WorkerOrm(username="Michael")
			session.add_all([worker_jack, worker_michael])
			session.flush()
			session.commit()

	@staticmethod
	def select_workers():
		with session_factory() as session:
			query = select(WorkerOrm)
			result = session.execute(query)
			workers = result.scalars().all()
			print(f"{workers=}")

	@staticmethod
	def update_worker(worker_id: int = 2, new_username: str = "Misha"):
		with session_factory() as session:
			worker_michael = session.get(WorkerOrm, worker_id)
			worker_michael.username = new_username
			session.refresh(worker_michael)
			session.commit()

	@staticmethod
	def insert_resumes():
		with session_factory() as session:
			resume_jack_1 = ResumesOrm(
				title="Python Junior Developer", compensation=50000, workload=Workload.fulltime, worker_id=1
			)
			resume_jack_2 = ResumesOrm(
				title="Python разработчик", compensation=150000, workload=Workload.fulltime, worker_id=1
			)
			resume_michael_1 = ResumesOrm(
				title="Python Data Engineer", compensation=250000, workload=Workload.parttime, worker_id=2
			)
			resume_michael_2 = ResumesOrm(
				title="Data Scientist", compensation=300000, workload=Workload.fulltime, worker_id=2
			)
			session.add_all([resume_jack_1, resume_jack_2, resume_michael_1, resume_michael_2])
			session.commit()

	@staticmethod
	def select_resumes_ang_compensation(like_language: str = "Python"):
		"""
		SELECT workload, avg(compensation)::int as avg_compensation
		FROM resumes
		WHERE title like '%Python%' and compensation > 40000
		GROUP BY workload
		"""
		with session_factory() as session:
			query = (
				select(
					ResumesOrm.workload,
					cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
				)
				.select_from(ResumesOrm)
				.filter(and_(
					ResumesOrm.title.contains(like_language),
					ResumesOrm.compensation > 40000,
				))
				.group_by(ResumesOrm.workload)
				.having(cast(func.avg(ResumesOrm.compensation), Integer) > 70000)
			)
			print(query.compile(compile_kwargs={"literal_binds": True}))
			res = session.execute(query)
			result = res.all()
			print(result)



class AsyncORM:
	@staticmethod
	async def create_tables():
		async with async_engine.begin() as conn:
			await conn.run_sync(Base.metadata.drop_all)
			await conn.run_sync(Base.metadata.create_all)

	@staticmethod
	async def insert_workers():
		async with async_session_factory() as session:
			worker_jack = WorkerOrm(username="Jack")
			worker_michael = WorkerOrm(username="Michael")
			session.add_all([worker_jack, worker_michael])
			await session.flush()
			await session.commit()

	@staticmethod
	async def select_workers():
		async with session_factory() as session:
			query = select(WorkerOrm)
			result = await session.execute(query)
			workers = result.scalars().all()
			print(f"{workers=}")

	@staticmethod
	async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
		async with session_factory() as session:
			worker_michael = await session.get(WorkerOrm, worker_id)
			worker_michael.username = new_username
			await session.refresh(worker_michael)
			await session.commit()

	@staticmethod
	async def insert_resumes():
		async with session_factory() as session:
			resume_jack_1 = ResumesOrm(
				title="Python Junior Developer", compensation=50000, workload=Workload.fulltime, worker_id=1
			)
			resume_jack_2 = ResumesOrm(
				title="Python разработчик", compensation=150000, workload=Workload.fulltime, worker_id=1
			)
			resume_michael_1 = ResumesOrm(
				title="Python Data Engineer", compensation=250000, workload=Workload.parttime, worker_id=2
			)
			resume_michael_2 = ResumesOrm(
				title="Data Scientist", compensation=300000, workload=Workload.fulltime, worker_id=2
			)
			session.add_all([resume_jack_1, resume_jack_2, resume_michael_1, resume_michael_2])
			await session.commit()