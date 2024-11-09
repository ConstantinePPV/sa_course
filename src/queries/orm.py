from sqlalchemy import text, insert
from database import sync_engine, async_engine,session_factory, async_session_factory, Base
from models import WorkerOrm



def create_tables():
	sync_engine.echo = False
	Base.metadata.drop_all(sync_engine)
	Base.metadata.create_all(sync_engine)
	sync_engine.echo = True


async def insert_data():	
	async with async_session_factory() as session:
		worker_bobr = WorkerOrm(username="Bobr")
		worker_volk = WorkerOrm(username="Volk")
		session.add_all([worker_bobr, worker_volk])
		await session.commit()