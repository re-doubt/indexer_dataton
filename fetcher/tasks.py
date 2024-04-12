import asyncio
import json

from celery.signals import worker_process_init
from urllib.parse import urlparse
from loguru import logger

from config import settings
from indexer.database import *
from indexer.crud import *
from parser.parsers_collection import RemoteDataFetcher, opt_apply
from fetcher.celery import app


loop = None
worker = None


@worker_process_init.connect()
def configure_worker(signal=None, sender=None, **kwargs):
    global loop
    global worker
    loop = asyncio.get_event_loop()


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    logger.info("Setting up periodic fetcher invocation")
    sender.add_periodic_task(settings.fetcher.poll_interval, fetch_outbox_task.s("test"), name="Fetcher task")


async def get_entity(session: SessionMaker, task: FetchOutbox):
    if task.entity_type == FetchOutbox.JETTON_MASTER:
        entity = await get_jetton_master(session, task.address)

    elif task.entity_type == FetchOutbox.NFT_ITEM:
        entity = await get_nft(session, task.address)

    elif task.entity_type == FetchOutbox.NFT_COLLECTION:
        entity = await get_nft_collection(session, task.address)

    else:
        raise Exception(f"entity_type not supported: {task.entity_type}")

    if not entity:
        raise Exception(f"Entity not parsed yet: address {task.address}, entity_type {task.entity_type}. Waiting")

    return entity


async def process_item(session: SessionMaker, task: FetchOutbox):
    try:
        fetcher = RemoteDataFetcher()
        metadata_json = await fetcher.fetch(task.metadata_url)

        try:
            metadata = json.loads(metadata_json)

        except Exception as e:
            logger.error(f"Failed to parse metadata for {task.metadata_url}: {e}")
            metadata = {}

        entity = await get_entity(session, task)

        if task.entity_type == FetchOutbox.JETTON_MASTER:
            entity.symbol = metadata.get("symbol")
            entity.decimals = opt_apply(metadata.get("decimals"), int)

        elif task.entity_type == FetchOutbox.NFT_ITEM:
            entity.attributes = json.dumps(metadata.get("attributes")) if "attributes" in metadata else None

        entity.name = metadata.get("name")
        entity.image = metadata.get("image").replace("\x00", "") if "image" in metadata else None
        entity.image_data = metadata.get("image_data").replace("\x00", "") if "image_data" in metadata else None
        entity.description = metadata.get("description")

        logger.info(f"Updating metadata for entity: address {task.address}, entity_type {task.entity_type}")
        await remove_fetch_outbox_item(session, task.outbox_id)

    except Exception as e:
        logger.error(f"Failed to perform fetching for outbox item {task.outbox_id}: {e}")
        await postpone_fetch_outbox_item(session, task, settings.fetcher.retry.timeout)


async def fetch_outbox():
    logger.info("Starting fetch outbox loop")

    while True:
        async with SessionMaker() as session:
            tasks = await get_fetch_outbox_items(session, settings.fetcher.batch_size)
            if not tasks:
                logger.info("Fetch outbox is empty, exiting")
                break
            tasks = [process_item(session, task[0]) for task in tasks]
            await asyncio.gather(*tasks)

            await session.commit()


@app.task
def fetch_outbox_task(arg):
    logger.info("Running fetch outbox iteration")
    loop.run_until_complete(fetch_outbox())

    return
