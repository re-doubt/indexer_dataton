import asyncio
import json
import time

import aiohttp
from celery.signals import worker_process_init
from urllib.parse import urlparse
from loguru import logger

from config import settings
from indexer.database import *
from indexer.crud import *
from parser.parsers_collection import opt_apply
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


class RemoteDataFetcher:
    def __init__(self, ipfs_gateway='https://w3s.link/ipfs/', timeout=5, max_attempts=3):
        self.ipfs_gateway = ipfs_gateway
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.headers = {'User-Agent': 'redoubt'}

    async def fetch(self, url):
        logger.debug(f"Fetching {url}")
        parsed_url = urlparse(url)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            if parsed_url.scheme == 'ipfs':
                # assert len(parsed_url.path) == 0, parsed_url
                async with session.get(self.ipfs_gateway + parsed_url.netloc + parsed_url.path, timeout=self.timeout) as resp:
                    return await resp.text()
            elif parsed_url.scheme is None or len(parsed_url.scheme) == 0:
                logger.error(f"No schema for URL: {url}")
                return None
            else:
                if parsed_url.netloc == 'localhost' or parsed_url.netloc in ["ton-metadata.fanz.ee", "startupmarket.io", "mashamimosa.ru", "server.tonguys.org"]:
                    return None
                retry = 0
                while retry < self.max_attempts:
                    try:
                        async with session.get(url, timeout=self.timeout) as resp:
                            return await resp.text()
                    except Exception as e:
                        logger.error(f"Unable to fetch data from {url}", e)
                        await asyncio.sleep(1)
                    retry += 1
                return None


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
            if "name" in metadata:
                entity.name = str(metadata.get("name"))
            if "image" in metadata:
                entity.image = str(metadata.get("image")).replace("\x00", "")
            if "image_data" in metadata:
                entity.image_data = str(metadata.get("image_data")).replace("\x00", "")
            if "description":
                entity.description = str(metadata.get("description"))
            entity.metadata_update_time = None
            entity.metadata_updated = False
            logger.info(f"Fetching metadata for NFT collection: {entity.address}")

        else:
            raise Exception()

    except Exception as e:
        entity.metadata_update_time = int(time.time()) + settings.fetcher.retry_interval
        logger.error(f"Failed to perform fetching for NFT collection {entity.address}: {e}")


async def process_nft_item(entity: NFTItem):
    try:
        metadata = await fetch_metadata(entity.metadata_url)

        if metadata:
            if "name" in metadata:
                entity.name = str(metadata.get("name"))
            if "image" in metadata:
                entity.image = str(metadata.get("image")).replace("\x00", "")
            if "image_data" in metadata:
                entity.image_data = str(metadata.get("image_data")).replace("\x00", "")
            if "description" in metadata:
                entity.description = str(metadata.get("description"))
            if "attributes" in metadata:
                entity.attributes = json.dumps(metadata.get("attributes"))
            entity.metadata_update_time = None
            entity.metadata_updated = False
            logger.info(f"Fetching metadata for NFT item: {entity.address}")

        else:
            raise Exception()

    except Exception as e:
        entity.metadata_update_time = int(time.time()) + settings.fetcher.retry_interval * 168  # retry coefficient for nft_item
        logger.error(f"Failed to perform fetching for NFT item {entity.address}: {e}")


async def process_jetton_master(entity: JettonMaster):
    try:
        metadata = await fetch_metadata(entity.metadata_url)

        if metadata:
            if "name" in metadata:
                entity.name = str(metadata.get("name"))
            if "image" in metadata:
                entity.image = str(metadata.get("image")).replace("\x00", "")
            if "image_data" in metadata:
                entity.image_data = str(metadata.get("image_data")).replace("\x00", "")
            if "description" in metadata:
                entity.description = str(metadata.get("description"))
            if "symbol" in metadata:
                entity.symbol = str(metadata.get("symbol"))
            if "decimals" in metadata:
                entity.decimals = opt_apply(metadata.get("decimals"), int)
            entity.metadata_update_time = None
            entity.metadata_updated = False
            logger.info(f"Fetching metadata for jetton master: {entity.address}")

        else:
            raise Exception()

    except Exception as e:
        entity.metadata_update_time = int(time.time()) + settings.fetcher.retry_interval
        logger.error(f"Failed to perform fetching for jetton master {entity.address}: {e}")


async def fetch_all():
    logger.info("Starting fetch loop")

    while True:
        async with SessionMaker() as session:
            tasks = []

            nft_collection_tasks = await get_nft_collection_fetch_tasks(session, settings.fetcher.batch_size)
            if nft_collection_tasks:
                tasks += [process_nft_collection(task) for task in nft_collection_tasks]

            nft_item_tasks = await get_nft_item_fetch_tasks(session, settings.fetcher.batch_size)
            if nft_item_tasks:
                tasks += [process_nft_item(task) for task in nft_item_tasks]

            jetton_master_tasks = await get_jetton_master_fetch_tasks(session, settings.fetcher.batch_size)
            if jetton_master_tasks:
                tasks += [process_jetton_master(task) for task in jetton_master_tasks]

            if tasks:
                await asyncio.gather(*tasks)
            else:
                logger.info("Fetch queue is empty, exiting")
                break

            await session.commit()
            await asyncio.sleep(1)


@app.task
def fetch_outbox_task(arg):
    logger.info("Running remote data fetching iteration")
    loop.run_until_complete(fetch_all())

    return
