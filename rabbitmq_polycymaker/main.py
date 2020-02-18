#!/usr/bin/env python

import logging
import json
from time import sleep
from configargparse import ArgumentParser
from prettylog import basic_config, LogFormat
from yarl import URL
from pyrabbit2.api import Client
from rabbitmq_polycymaker.rabbitmq_policy import RabbitData
from rabbitmq_polycymaker.wait_for_client import wait_for_client

parser = ArgumentParser(auto_env_var_prefix="AMQP_")

parser.add_argument("--api-url", type=URL, default="localhost:15672")

parser.add_argument("--user", type=str, default="admin")

parser.add_argument("--password", type=str, default="admin")

parser.add_argument(
    "--policy-groups",
    type=json.loads,
    required=True,
    help='JSON DC hosts groups for policies. Example: {"dc_name": ["host1"]}'
)

parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Dry run mode. Only show which policies will be create")

parser.add_argument(
    "--sleep",
    type=int,
    default=20,
    help="Sleep seconds between run")

parser.add_argument(
    "-L",
    "--log-level",
    help="Log level",
    default="info",
    choices=(
        'critical', 'error', 'warning', 'info', 'debug'
    ),
)

parser.add_argument(
    "--log-format", choices=LogFormat.choices(), default=LogFormat.stream
)

arguments = parser.parse_args()

log = logging.getLogger()

if __name__ == "__main__":
    basic_config(
        level=arguments.log_level.upper(),
        buffered=False,
        log_format=arguments.log_format,
        date_format=True,
    )

    client = Client(arguments.api_url, arguments.user, arguments.password)
    wait_for_client(client)
    log.debug("RabbitMQ alive")

    rabbit_info = RabbitData(client)

    while True:
        queues_dict = rabbit_info.queues()

        policies_dict = rabbit_info.policies()

        queues_without_policy = rabbit_info.queues_without_policy()

        if rabbit_info.need_a_policy():
            for vhost, queues in queues_without_policy.items():
                for queue in queues:
                    rabbit_info.create_policy(
                        vhost, queue, arguments.policy_groups, arguments.dry_run
                    )

        nodes_dict = rabbit_info.nodes_dict()
        master_nodes_queues = rabbit_info.master_nodes_queues(nodes_dict)
        queues_on_nodes = rabbit_info.calculate_queues(master_nodes_queues)

        log.info('Sleeping for %r seconds', arguments.sleep)
        sleep(arguments.sleep)
        rabbit_info.reload()
