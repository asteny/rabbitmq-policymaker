#!/usr/bin/env python

import logging
import time
import json

from re import escape
from typing import Dict, List
from hashlib import sha1

log = logging.getLogger()


def bucket(string, size):
    hs = int(sha1(string.encode("utf-8")).hexdigest(), 16)
    return hs % size


class RabbitData:
    def __init__(self, client):
        self.client = client
        self.vhosts = self.client.get_vhost_names()
        self.all_queues = self.client.get_queues()
        self.all_policies = self.client.get_all_policies()
        self.nodes = self.client.get_nodes()

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

        log.info("All queues in vhosts: %r", queues_dict)
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

        log.info("All policies in vhosts: %r", policies_dict)
        return policies_dict

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

    def need_a_policy(self):
        queues = []
        for q_list in self.queues_without_policy().values():
            if len(q_list) > 0:
                queues.append(q_list)

        log.info("Queue without policy: %r", len(queues))
        return len(queues) > 0

    def is_queue_running(self, vhost: str, queue: str) -> bool:
        state = None
        while state != "running":
            try:
                state = self.client.get_queue(vhost, queue)["state"]
                log.info("Queue %r has state %r", queue, state)
                if state != "running":
                    time.sleep(1)
                else:
                    return True
            except KeyError:
                log.exception("RabbitMQ API not ready to answer")
                time.sleep(2)

    def create_policy(
            self,
            vhost: str,
            queue: str,
            policy_groups: json,
            dry_run: bool
    ):

        bucket_number = bucket(
            "{}{}".format(vhost, queue),
            len(policy_groups)
        )
        bucket_nodes = policy_groups.get(str(bucket_number))

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

        if not dry_run:
            log.info("Policy body dict is %r", dict_params)
            self.client.create_policy(
                vhost=vhost, policy_name=queue, **dict_params
            )
            time.sleep(3)

            if self.is_queue_running(vhost, queue):
                log.info(
                    "Policy created and queue %r in running state", queue
                )
        else:
            log.info("Dry Run mode: Policy body dict is %r", dict_params)

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
        log.info("Nodes info: %r", nodes_dict)
        return nodes_dict

    def master_nodes_queues(self, nodes_dict: Dict) -> Dict[str, Dict[str, List]]:
        """
        :return: dict {node_name: {vhost1: list_queues, vhost2: list_queues}
        """

        master_nodes_queues_dict = {}

        queues_data = self.all_queues
        log.debug("Queues info: %r", queues_data)

        for node in nodes_dict.keys():

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

        log.info('Master nodes queues dict %r', master_nodes_queues_dict)
        return master_nodes_queues_dict

    def calculate_queues(
        self,
        master_nodes_queues_dict: dict
    ) -> Dict[str, int]:
        """
        :param master_nodes_queues_dict: dict
        :return: dict {node1: number_queues, node2: number_queues,}
        """

        calculated_dict = {}

        for node, vhost in master_nodes_queues_dict.items():
            counter = sum(map(len, vhost.values()))
            calculated_dict[node] = counter

        log.info("Queues on nodes: %r", calculated_dict)
        return calculated_dict
