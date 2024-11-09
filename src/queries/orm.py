from sqlalchemy import text, insert, select
from database import sync_engine, async_engine,session_factory, async_session_factory, Base
from models import WorkerOrm


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
			session.commit()


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