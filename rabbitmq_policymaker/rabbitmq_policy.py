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
QUEUE_BALANCER_POLICY_NAME = "queue_master_balancer"


def bucket(string, size):
    hs = int(sha1(string.encode("utf-8")).hexdigest(), 16)
    return hs % size


@dataclass
class Queue:
    vhost: str
    name: str


@dataclass
class Node:
    node: str
    queues: list


class RabbitData:
    def __init__(
        self,
        client: Client,
        policy_groups: Dict,
        dry_run: bool,
        wait_sleep: int,
        queues_delta: int,
    ):
        self.client = client
        self.policy_groups = policy_groups
        self.dry_run = dry_run
        self.wait_sleep = wait_sleep
        self.queues_delta = queues_delta

    def reload(self):
        self.client.get_vhost_names()
        self.client.get_queues()
        self.client.get_nodes()

    @property
    def queues_without_policy(self) -> List[Queue]:

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
                    policy = self.client.get_policy(
                        queue_vhost, queue_name
                    ).get("name")
                    log.debug("Policy: '{}' exist".format(policy))
                except HTTPError as er:
                    log.debug(er)
                    if er.status == 404:
                        queues_list.append(
                            Queue(vhost=queue_vhost, name=queue_name)
                        )

        return queues_list

    def is_queue_running(self, vhost: str, queue: str) -> bool:
        state = None
        while state != RUNNING:
            state = self.client.get_queue(vhost, queue).get("state")
            log.info("Queue %r has state %r", queue, state)
            if state != RUNNING:
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

        definition_dict = {
            "ha-mode": "nodes",
            "ha-params": rabbit_nodes,
            "queue-mode": "lazy",
        }
        dict_params = {
            "pattern": "{}{}{}".format("^", escape(queue), "$"),
            "definition": definition_dict,
            "priority": 30,
            "apply-to": "queues",
        }

        if not self.dry_run:
            log.info("Policy body dict is %r", dict_params)
            policy = self.client.create_policy(
                vhost=vhost, policy_name=queue, **dict_params
            )
            sleep(self.wait_sleep)

            if self.is_queue_running(vhost, queue):
                log.info("Policy created and queue %r in running state", queue)
        else:
            log.info(
                "It's a dry run mode: Policy body dict will be %r", dict_params
            )
        return policy

    def queues_on_hosts(self) -> List[Node]:
        nodes = self.client.get_nodes()
        queues = self.client.get_queues()

        queues_on_host_list = []

        for node in nodes:
            node_name = node.get("name")
            queues_on_host = []
            for queue in queues:
                exclusive = queue.get("exclusive")
                auto_delete = queue.get("auto_delete")

                if exclusive or auto_delete:
                    continue

                queue_name = queue.get("name")
                queue_vhost = queue.get("vhost")
                queue_node = queue.get("node")

                if node_name == queue_node:
                    queues_on_host.append(
                        Queue(vhost=queue_vhost, name=queue_name)
                    )
            log.info(
                "Node '{}' has {} queues".format(
                    node_name, len(queues_on_host)
                )
            )
            queues_on_host_list.append(
                Node(node=node_name, queues=queues_on_host)
            )
        return queues_on_host_list

    def queues_for_relocate(self):
        queues_on_hosts = self.queues_on_hosts()
        for group in self.policy_groups.values():
            calculated_queues = {}
            for rabbit in self.queues_on_hosts():
                if rabbit.node.split("@")[1] in group:
                    calculated_queues[rabbit.node] = len(rabbit.queues)
            min_queues_node = min(calculated_queues, key=calculated_queues.get)
            max_queues_node = max(calculated_queues, key=calculated_queues.get)
            log.info(
                "Max queues on '{}'. Min queues on '{}'".format(
                    max_queues_node, min_queues_node
                )
            )
            if (
                calculated_queues[max_queues_node]
                - calculated_queues[min_queues_node]
                > self.queues_delta
            ):
                for rabbit in queues_on_hosts:
                    if rabbit.node == max_queues_node:
                        queue = rabbit.queues[0].name
                        vhost = rabbit.queues[0].vhost

                        return queue, vhost, min_queues_node

    def relocate_queue(self):
        queue_data = self.queues_for_relocate()
        if queue_data:
            queue, vhost, min_queues_node = queue_data

            definition_dict = {
                "ha-mode": "nodes",
                "ha-params": min_queues_node.split(" "),
            }
            dict_params = {
                "pattern": "{}{}{}".format("^", escape(queue), "$"),
                "definition": definition_dict,
                "priority": 999,
                "apply-to": "queues",
            }
            log.info(
                "Relocate queue '{}'. Policy body dict is {}".format(
                    queue, dict_params
                )
            )

            if not self.dry_run:
                self.client.create_policy(
                    vhost=vhost,
                    policy_name=QUEUE_BALANCER_POLICY_NAME,
                    **dict_params
                )
                self.is_queue_running(vhost, queue)
                self.client.queue_action(vhost, queue, action="sync")
                self.is_queue_running(vhost, queue)
                self.client.queue_action(vhost, queue, action="sync")
                self.is_queue_running(vhost, queue)
                log.info("Deleting relocate policy")
                self.client.delete_policy(vhost, "queue_master_balancer")
                self.is_queue_running(vhost, queue)
                self.client.queue_action(vhost, queue, action="sync")
                self.is_queue_running(vhost, queue)
            else:
                log.info("It's dry run. Nothing changed")

        else:
            log.info("Nothing for balance")
