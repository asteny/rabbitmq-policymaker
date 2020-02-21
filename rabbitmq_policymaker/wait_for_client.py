#!/usr/bin/env python

import logging
import time
from pyrabbit2.http import NetworkError

log = logging.getLogger()


def wait_for_client(client) -> bool:
    """
    :param client:
    :return: bool
    """
    log.debug("RabbitMQ http not ready, waiting...")
    try:
        return client.is_alive()
    except NetworkError:
        time.sleep(5)
        wait_for_client(client)
