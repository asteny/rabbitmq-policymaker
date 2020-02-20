#!/usr/bin/env python

import logging
from dataclasses import dataclass
from hashlib import sha1
from re import escape
from time import sleep
from typing import Dict, List

from pyrabbit2 import Client
from pyrabbit2.http import HTTPError

log = logging.getLogger()

RUNNING = "running"


def bucket(string, size):
    hs = int(sha1(string.encode("utf-8")).hexdigest(), 16)
    return hs % size


@dataclass
class QueueWithoutPolicy:
    vhost: str
    name: str


class RabbitData:
    def __init__(
        self,
        client: Client,
        policy_groups: Dict,
        dry_run: bool,
        wait_sleep: int,
    ):
        self.client = client
        self.policy_groups = policy_groups
        self.dry_run = dry_run
        self.wait_sleep = wait_sleep

    def reload(self):
        self.client.get_vhost_names()
        self.client.get_queues()
        self.client.get_nodes()

    @property
    def queues_without_policy(self) -> List[QueueWithoutPolicy]:

        queues_list = []

        for queue in self.client.get_queues():
            exclusive = queue.get("exclusive")
            auto_delete = queue.get("auto_delete")

            if exclusive or auto_delete:
                continue

            queue_name = queue.get("name")
            queue_vhost = queue.get("vhost")
            policy_on_queue = queue.get("policy")

            if not policy_on_queue or queue_name != policy_on_queue:
                try:
                    self.client.get_policy(queue_vhost, queue_name).get("name")
                except HTTPError as er:
                    log.debug(er)
                    if er.status == 404:
                        queues_list.append(
                            QueueWithoutPolicy(
                                vhost=queue_vhost, name=queue_name
                            )
                        )

        return queues_list

    def is_queue_running(self, vhost: str, queue: str) -> bool:
        state = None
        while state != RUNNING:
            try:
                state = self.client.get_queue(vhost, queue)["state"]
                log.info("Queue %r has state %r", queue, state)
                if state != RUNNING:
                    sleep(self.wait_sleep)
            except KeyError:
                log.exception("RabbitMQ API not ready to answer")
                sleep(self.wait_sleep)
        return True

    def create_policy(self, vhost: str, queue: str):

        bucket_number = bucket(
            "{}{}".format(vhost, queue), len(self.policy_groups)
        )
        bucket_nodes = self.policy_groups.get(str(bucket_number))

        rabbit_nodes = []

        for node in bucket_nodes:
            rabbit_nodes.append("rabbit@{}".format(node))

        definition_dict = {"ha-mode": "nodes", "ha-params": rabbit_nodes}
        dict_params = {
            "pattern": "{}{}{}".format("^", escape(queue), "$"),
            "definition": definition_dict,
            "priority": 30,
            "apply-to": "queues",
        }

        if not self.dry_run:
            log.info("Policy body dict is %r", dict_params)
            self.client.create_policy(
                vhost=vhost, policy_name=queue, **dict_params
            )
            sleep(self.wait_sleep)

            if self.is_queue_running(vhost, queue):
                log.info("Policy created and queue %r in running state", queue)
        else:
            log.info(
                "It's a dry run mode: Policy body dict will be %r", dict_params
            )

    @property
    def calculate_queues_on_hosts(self) -> Dict[str, int]:
        """
        :return: dict {node1: number_queues, node2: number_queues,}
        """

        calculated_dict = {}

        nodes = self.client.get_nodes()
        queues = self.client.get_queues()

        for node in nodes:
            node_name = node.get("name")
            for queue in queues:
                exclusive = queue.get("exclusive")
                auto_delete = queue.get("auto_delete")

                if exclusive or auto_delete:
                    continue

                queue_node = queue.get("node")
                if node_name == queue_node:
                    calculated_dict[node_name] = (
                        calculated_dict.setdefault(node_name, 0) + 1
                    )

        return calculated_dict
