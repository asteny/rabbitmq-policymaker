#!/usr/bin/env python

import logging
import time
import json

from re import escape
from typing import Dict, List
from hashlib import sha1
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


def bucket(string, size):
    hs = int(sha1(string.encode("utf-8")).hexdigest(), 16)
    return hs % size


class RabbitData:
    def __init__(self, client):
        self.client = client
        self.get_vhosts = self.client.get_vhost_names()
        self.get_queues = self.client.get_queues()
        self.get_all_policies = self.client.get_all_policies()

    def queues(self) -> Dict[str, List]:
        """
        :return: Dict: {vhost, [queues]}
        """

        queues_dict = {}

        for vhost in self.get_vhosts:
            list_queues = []

            for queue in self.get_queues:
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
        policies = self.get_all_policies

        policies_dict = {}

        for vhost in self.get_vhosts:
            list_policies = []

            for policy in policies:
                policy_vhost = policy["vhost"]
                policy_name = policy["name"]
                if vhost == policy_vhost:
                    list_policies.append(policy_name)

            policies_dict[vhost] = list_policies

        log.info("All policies in vhosts: %r", policies_dict)
        return policies_dict

    def queues_without_policy(
            self,
            queues_dict: dict,
            policies_dict: dict,
    ) -> Dict[str, List]:

        queues_without_policy_dict = {}

        for queue_vhost, queues in queues_dict.items():
            list_queues = []
            for queue in queues:

                if queue not in policies_dict[queue_vhost]:
                    log.debug("Queue {} on vhost {} without policy".format(
                        queue, queue_vhost
                    ))
                    list_queues.append(queue)

            queues_without_policy_dict[queue_vhost] = list_queues

        return queues_without_policy_dict

    def need_a_policy(self, queues_without_policy: dict):
        queues = []
        for q_list in queues_without_policy.values():
            if len(q_list) > 0:
                queues.append(q_list)

        if len(queues) > 0:
            log.info("Queues without_policy: %r", queues_without_policy)
            return True
        else:
            log.info("All queues has policy. Nothing to do")
            return False

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




# def nodes_dict(nodes_info_data, vhost_names: list) -> Dict[str, List]:
#     """
#     :param nodes_info_data
#     :param vhost_names list of vhosts
#     :return: dict: Key is a name of rabbit node, Value is empty list
#     """
#     temp_dict = dict.fromkeys((vhost for vhost in vhost_names))
#
#     nodes_dictionary = dict.fromkeys(
#         (node["name"] for node in nodes_info_data), temp_dict
#     )
#     log.debug("Nodes info: %r", nodes_dictionary)
#     return nodes_dictionary
#
#
# def master_nodes_queues(
#         nodes_dict: dict, vhost_names: list) -> Dict[str, Dict[str, List]]:
#     """
#     :param queues_data: list of dicts with all queues info
#     :param vhost_names list of vhosts
#     :return: dict {node_name: {vhost1: list_queues, vhost2: list_queues}
#     """
#
#     master_nodes_queues_dict = {}
#
#     queues_data = client.get_queues()
#     log.debug("Queues info: %r", queues_data)
#
#     for node in nodes_dict.keys():
#
#         vhost_dict = {}
#
#         for vhost in vhost_names:
#             list_queues = []
#
#             for queue in queues_data:
#                 name = queue["name"]
#                 exclusive = queue["exclusive"]
#                 auto_delete = queue["auto_delete"]
#                 log.debug(
#                     "Queue {}: Exclusive - {} and Auto_delete - {}".format(
#                         name,
#                         exclusive,
#                         auto_delete,
#                     )),
#                 if all((
#                         node == queue["node"],
#                         queue["vhost"] == vhost,
#                         not exclusive,
#                         not auto_delete,
#                 )):
#                     list_queues.append(queue["name"])
#
#             log.debug("list_queues: %r", list_queues)
#
#             vhost_dict[vhost] = list_queues
#             master_nodes_queues_dict[node] = vhost_dict
#
#     return master_nodes_queues_dict
#
#
# def calculate_queues(master_nodes_queues_dict: dict) -> Dict[str, int]:
#     """
#     :param master_nodes_queues_dict: dict
#     :return: dict {node1: number_queues, node2: number_queues,}
#     """
#
#     calculated_dict = {}
#
#     for node, vhost in master_nodes_queues_dict.items():
#         counter = sum(map(len, vhost.values()))
#         calculated_dict[node] = counter
#
#     return calculated_dict
#
#
# def calculate_vhost(max_master_dict: dict) -> Dict[str, int]:
#     """
#     :param max_master_dict:
#     :return: dict {vhost, number_queues}
#     """
#     calculated_dict = {}
#
#     for vhost, queues in max_master_dict.items():
#         if not queues:
#             continue
#         calculated_dict[vhost] = max(map(len, queues))
#     return calculated_dict
#
#
# def is_relocate(max_queues: int, min_queues: int, queue_delta: int) -> bool:
#     """
#     :param min_queues: int
#     :param max_queues: int
#     :return: bool
#     """
#     return max_queues - min_queues > queue_delta
#
#
# def relocate_policy(
#     queue_for_relocate: str,
#     max_queues_vhost: str,
#     min_queues_node: str,
#     policy_name: str,
# ):
#
#     definition_dict = {
#         "ha-mode": "nodes",
#         "ha-params": min_queues_node.split(" "),
#     }
#     dict_params = {
#         "pattern": "{}{}{}".format("^", escape(queue_for_relocate), "$"),
#         "definition": definition_dict,
#         "priority": 999,
#         "apply-to": "queues",
#     }
#     log.debug("Policy body dict is %r", dict_params)
#
#     client.create_policy(
#         vhost=max_queues_vhost, policy_name=policy_name, **dict_params
#     )
#
#
#
#
#
# def relocate_queue(
#     client,
#     max_master_dict: dict,
#     max_queues_vhost: str,
#     min_queues_node: str,
#     sleep_time: int,
# ):
#
#     queue_for_relocate = max_master_dict[max_queues_vhost][0]
#     log.debug("Queue for relocate is %r", queue_for_relocate)
#     log.debug("Creating relocate policy")
#     relocate_policy(
#         queue_for_relocate,
#         max_queues_vhost,
#         min_queues_node,
#         arguments.policy_name,
#     )
#     time.sleep(sleep_time)
#     client.queue_action(max_queues_vhost, queue_for_relocate, action="sync")
#     time.sleep(sleep_time)
#     is_queue_running(client, max_queues_vhost, queue_for_relocate)
#     client.queue_action(max_queues_vhost, queue_for_relocate, action="sync")
#     time.sleep(sleep_time)
#     client.delete_policy(max_queues_vhost, arguments.policy_name)
#     time.sleep(sleep_time)
#     client.queue_action(max_queues_vhost, queue_for_relocate, action="sync")
#     time.sleep(sleep_time)
#     is_queue_running(client, max_queues_vhost, queue_for_relocate)
