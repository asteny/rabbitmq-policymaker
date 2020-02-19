#!/usr/bin/env python

import json

import pytest

from rabbitmq_polycymaker.rabbitmq_policy import RabbitData, bucket


def get_json(file):
    with open(file, "r") as json_file:
        data = json.load(json_file)
    return data


class MockRabbit:
    def __init__(self, policies_file):
        self.policies_file = policies_file

    def get_vhost_names(self):
        return ["/", "first_vhost", "my_app", "second_vhost", "some_vhost"]

    def get_queues(self):
        return get_json("tests/data/get_queues.json")

    def get_all_policies(self):
        return get_json(self.policies_file)

    def get_nodes(self):
        return get_json("tests/data/get_nodes.json")


PARAMS = "policies_file,expected"
POLICY_GROUPS = "tests/data/policy_groups.json"
DRY_RUN = False
WAIT_SLEEP = 0


@pytest.mark.parametrize(
    PARAMS,
    [("tests/data/get_all_policies_empty.json", "tests/data/queues.json")],
)
def test_queue(policies_file, expected):
    client = MockRabbit(policies_file=policies_file)
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.queues() == get_json(expected)


@pytest.mark.parametrize(
    PARAMS,
    [
        (
            "tests/data/get_all_policies_empty.json",
            "tests/data/empty_policies.json",
        ),
        ("tests/data/get_all_policies.json", "tests/data/policies.json"),
    ],
)
def test_policies(policies_file, expected):
    client = MockRabbit(policies_file=policies_file)
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.policies() == get_json(expected)


@pytest.mark.parametrize(
    PARAMS,
    [
        (
            "tests/data/get_all_policies_empty.json",
            "tests/data/queues_without_policy.json",
        ),
        (
            "tests/data/get_all_policies.json",
            "tests/data/queues_with_policies.json",
        ),
    ],
)
def test_queues_policies(policies_file, expected):
    client = MockRabbit(policies_file=policies_file)
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.queues_without_policy == get_json(expected)


@pytest.mark.parametrize(
    PARAMS,
    [
        ("tests/data/get_all_policies_empty.json", True),
        ("tests/data/get_all_policies.json", False),
    ],
)
def test_need_a_policy(policies_file, expected):
    client = MockRabbit(policies_file=policies_file)
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.need_a_policy is expected


def test_hash_bucket():
    m = {}

    for i in range(1000):
        a = bucket(str(i), 3)
        m[a] = m.setdefault(a, 0) + 1

    assert m == {0: 340, 1: 328, 2: 332}


def test_reload_class():
    client = MockRabbit(policies_file="tests/data/get_all_policies.json")
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    need_a_policy = rabbit_info.need_a_policy
    assert need_a_policy is False
    client.policies_file = "tests/data/get_all_policies_empty.json"
    rabbit_info.reload()
    need_a_policy = rabbit_info.need_a_policy
    assert need_a_policy is True


def test_nodes_dict():
    client = MockRabbit(policies_file="tests/data/get_all_policies.json")
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    nodes = rabbit_info.nodes_dict()
    assert nodes == get_json("tests/data/nodes.json")


def test_master_nodes_queues():
    client = MockRabbit(policies_file="tests/data/get_all_policies.json")
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.master_nodes_queues() == get_json(
        "tests/data/master_nodes_queues.json"
    )


def test_calculate_queues():
    client = MockRabbit(policies_file="tests/data/get_all_policies.json")
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.calculate_queues_on_hosts == get_json(
        "tests/data/calculate_queues.json"
    )
