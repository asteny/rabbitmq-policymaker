#!/usr/bin/env python

import logging

from hashlib import sha1
from re import escape
from time import sleep
from typing import Dict, List

log = logging.getLogger()

RUNNING = "running"


def bucket(string, size):
    hs = int(sha1(string.encode("utf-8")).hexdigest(), 16)
    return hs % size


class RabbitData:
    def __init__(
        self,
        client,
        policy_groups: Dict,
        dry_run: bool,
        sleep_seconds : int
    ):
        self.client = client
        self.policy_groups = policy_groups
        self.dry_run = dry_run
        self.vhosts = self.client.get_vhost_names()
        self.all_queues = self.client.get_queues()
        self.all_policies = self.client.get_all_policies()
        self.nodes = self.client.get_nodes()
        self.sleep_seconds = sleep_seconds

    def reload(self):
        self.vhosts = self.client.get_vhost_names()
        self.all_queues = self.client.get_queues()
        self.all_policies = self.client.get_all_policies()
        self.nodes = self.client.get_nodes()

    def queues(self) -> Dict[str, List]:
        """
        :return: Dict: {vhost, [queues]}
        """

        queues_dict = {}

        for vhost in self.vhosts:
            list_queues = []

            for queue in self.all_queues:
                name = queue["name"]
                exclusive = queue["exclusive"]
                auto_delete = queue["auto_delete"]
                log.debug(
                    "Queue {}: Exclusive - {} and Auto_delete - {}".format(
                        name,
                        exclusive,
                        auto_delete,
                    )),
                if all((
                        queue["vhost"] == vhost,
                        not exclusive,
                        not auto_delete,
                )):
                    list_queues.append(queue["name"])

            log.debug("vhost: {}, list_queues: {}".format(vhost, list_queues))

            queues_dict[vhost] = list_queues

        log.debug("All queues in vhosts: %r", queues_dict)
        return queues_dict

    def policies(self) -> Dict[str, List]:
        """
        :return: {vhost: [policies]}
        """

        policies_dict = {}

        for vhost in self.vhosts:
            list_policies = []

            for policy in self.all_policies:
                policy_vhost = policy["vhost"]
                policy_name = policy["name"]
                if vhost == policy_vhost:
                    list_policies.append(policy_name)

            policies_dict[vhost] = list_policies

        log.debug("All policies in vhosts: %r", policies_dict)
        return policies_dict

    @property
    def queues_without_policy(self) -> Dict[str, List]:

        queues_without_policy_dict = {}

        for queue_vhost, queues in self.queues().items():
            list_queues = []
            for queue in queues:

                if queue not in self.policies()[queue_vhost]:
                    log.debug("Queue {} on vhost {} without policy".format(
                        queue, queue_vhost
                    ))
                    list_queues.append(queue)

            queues_without_policy_dict[queue_vhost] = list_queues

        return queues_without_policy_dict

    @property
    def need_a_policy(self):
        queues = []
        for q_list in self.queues_without_policy.values():
            if len(q_list) > 0:
                queues.append(q_list)

        log.info("Queues without policy: %r", queues)
        return len(queues) > 0

    def is_queue_running(self, vhost: str, queue: str) -> bool:
        state = None
        while state != RUNNING:
            try:
                state = self.client.get_queue(vhost, queue)["state"]
                log.info("Queue %r has state %r", queue, state)
                if state != RUNNING:
                    sleep(self.sleep_seconds)
                else:
                    return True
            except KeyError:
                log.exception("RabbitMQ API not ready to answer")
                sleep(self.sleep_seconds)

    def create_policy(self, vhost: str, queue: str):

        bucket_number = bucket(
            "{}{}".format(vhost, queue),
            len(self.policy_groups)
        )
        bucket_nodes = self.policy_groups.get(str(bucket_number))

        rabbit_nodes = []

        for node in bucket_nodes:
            rabbit_nodes.append("rabbit@{}".format(node))

        definition_dict = {
            "ha-mode": "nodes",
            "ha-params": rabbit_nodes,
        }
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
            sleep(self.sleep_seconds)

            if self.is_queue_running(vhost, queue):
                log.info(
                    "Policy created and queue %r in running state", queue
                )
        else:
            log.info(
                "It's a dry run mode: Policy body dict will be %r", dict_params
            )

    def nodes_dict(self) -> Dict[str, List]:
        """
        :param nodes_info_data
        :param vhost_names list of vhosts
        :return: dict: Key is a name of rabbit node, Value is empty list
        """
        temp_dict = dict.fromkeys((vhost for vhost in self.vhosts))

        nodes = self.nodes
        log.debug("Get nodes: %r", nodes)

        nodes_dict = dict.fromkeys(
            (node["name"] for node in nodes), temp_dict
        )
        log.debug("Nodes info: %r", nodes_dict)
        return nodes_dict

    def master_nodes_queues(self) -> Dict[str, Dict[str, List]]:
        """
        :return: dict {node_name: {vhost1: list_queues, vhost2: list_queues}
        """

        master_nodes_queues_dict = {}

        queues_data = self.all_queues
        log.debug("Queues info: %r", queues_data)

        for node in self.nodes_dict().keys():

            vhost_dict = {}

            for vhost in self.vhosts:
                list_queues = []

                for queue in queues_data:
                    name = queue["name"]
                    exclusive = queue["exclusive"]
                    auto_delete = queue["auto_delete"]
                    log.debug(
                        "Queue {}: Exclusive - {} and Auto_delete - {}".format(
                            name,
                            exclusive,
                            auto_delete,
                        )),
                    if all((
                            node == queue["node"],
                            queue["vhost"] == vhost,
                            not exclusive,
                            not auto_delete,
                    )):
                        list_queues.append(queue["name"])

                vhost_dict[vhost] = list_queues
                master_nodes_queues_dict[node] = vhost_dict

        log.debug("Master nodes queues dict %r", master_nodes_queues_dict)
        return master_nodes_queues_dict

    def calculate_queues_on_hosts(self) -> Dict[str, int]:
        """
        :return: dict {node1: number_queues, node2: number_queues,}
        """

        calculated_dict = {}

        for node, vhost in self.master_nodes_queues().items():
            counter = sum(map(len, vhost.values()))
            calculated_dict[node] = counter

        log.info("Queues on nodes: %r", calculated_dict)
        return calculated_dict
