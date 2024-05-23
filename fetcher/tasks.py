import asyncio
import json
import time

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


async def fetch_metadata(metadata_url: str):
    fetcher = RemoteDataFetcher()
    metadata_json = await fetcher.fetch(metadata_url)

    try:
        metadata = json.loads(metadata_json)

    except Exception as e:
        logger.error(f"Failed to parse metadata for {metadata_url}: {e}")
        metadata = {}

    return metadata


async def process_nft_collection(entity: NFTCollection):
    try:
        metadata = await fetch_metadata(entity.metadata_url)

        if metadata:
            entity.name = metadata.get("name")
            entity.image = metadata.get("image").replace("\x00", "") if "image" in metadata else None
            entity.image_data = metadata.get("image_data").replace("\x00", "") if "image_data" in metadata else None
            entity.description = metadata.get("description")
            entity.metadata_update_time = None
            entity.metadata_updated = False
            logger.info(f"Fetching metadata for NFT collection: {entity.address}")

    except Exception as e:
        entity.metadata_update_time = int(time.time()) + settings.fetcher.retry_interval
        logger.error(f"Failed to perform fetching for NFT collection {entity.address}: {e}")


async def process_nft_item(entity: NFTItem):
    try:
        metadata = await fetch_metadata(entity.metadata_url)

        if metadata:
            entity.name = metadata.get("name")
            entity.image = metadata.get("image").replace("\x00", "") if "image" in metadata else None
            entity.image_data = metadata.get("image_data").replace("\x00", "") if "image_data" in metadata else None
            entity.description = metadata.get("description")
            entity.attributes = json.dumps(metadata.get("attributes")) if "attributes" in metadata else None
            entity.metadata_update_time = None
            entity.metadata_updated = False
            logger.info(f"Fetching metadata for NFT item: {entity.address}")

    except Exception as e:
        entity.metadata_update_time = int(time.time()) + settings.fetcher.retry_interval
        logger.error(f"Failed to perform fetching for NFT item {entity.address}: {e}")


async def process_jetton_master(entity: JettonMaster):
    try:
        metadata = await fetch_metadata(entity.metadata_url)

        if metadata:
            entity.name = metadata.get("name")
            entity.image = metadata.get("image").replace("\x00", "") if "image" in metadata else None
            entity.image_data = metadata.get("image_data").replace("\x00", "") if "image_data" in metadata else None
            entity.description = metadata.get("description")
            entity.symbol = metadata.get("symbol")
            entity.decimals = opt_apply(metadata.get("decimals"), int)
            entity.metadata_update_time = None
            entity.metadata_updated = False
            logger.info(f"Fetching metadata for jetton master: {entity.address}")

    except Exception as e:
        entity.metadata_update_time = int(time.time()) + settings.fetcher.retry_interval
        logger.error(f"Failed to perform fetching for jetton master {entity.address}: {e}")


async def fetch_all():
    logger.info("Starting fetch loop")

    while True:
        async with SessionMaker() as session:
            nft_collection_tasks = await get_nft_collection_fetch_tasks(session, settings.fetcher.batch_size)
            if nft_collection_tasks:
                tasks = [process_nft_collection(task) for task in nft_collection_tasks]
                await asyncio.gather(*tasks)

            nft_item_tasks = await get_nft_item_fetch_tasks(session, settings.fetcher.batch_size)
            if nft_item_tasks:
                tasks = [process_nft_item(task) for task in nft_item_tasks]
                await asyncio.gather(*tasks)

            jetton_master_tasks = await get_jetton_master_fetch_tasks(session, settings.fetcher.batch_size)
            if jetton_master_tasks:
                tasks = [process_jetton_master(task) for task in jetton_master_tasks]
                await asyncio.gather(*tasks)

            if not nft_collection_tasks and not nft_item_tasks and not jetton_master_tasks:
                logger.info("Fetch queue is empty, exiting")
                break

            await session.commit()
            asyncio.sleep(1)


@app.task
def fetch_outbox_task(arg):
    logger.info("Running remote data fetching iteration")
    loop.run_until_complete(fetch_all())

    return
