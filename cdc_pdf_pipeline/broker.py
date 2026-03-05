from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend

from cdc_pdf_pipeline.config import settings

broker = ListQueueBroker(url=settings.redis_url).with_result_backend(
    RedisAsyncResultBackend(redis_url=settings.redis_url)
)
