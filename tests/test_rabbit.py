#!/usr/bin/env python

import json
from typing import List

import pytest

from rabbitmq_polycymaker.rabbitmq_policy import (
    RabbitData,
    bucket,
    QueueWithoutPolicy,
)

PARAMS = "queues,expected"
POLICY_GROUPS = "tests/data/policy_groups.json"
DRY_RUN = False
WAIT_SLEEP = 0


def get_json(file):
    with open(file, "r") as json_file:
        data = json.load(json_file)
    return data


def get_test_queue_without_policy(file) -> List[QueueWithoutPolicy]:
    with open(file, "r") as json_file:
        data = json.load(json_file)
        return [
            QueueWithoutPolicy(name=tq.get("name"), vhost=tq.get("vhost"))
            for tq in data
        ]


class MockRabbit:
    def __init__(self, queue_data_file):
        self.queue_data_file = queue_data_file

    def get_vhost_names(self):
        return ["/", "first_vhost", "my_app", "second_vhost", "some_vhost"]

    def get_queues(self):
        return get_json(self.queue_data_file)

    def get_nodes(self):
        return get_json("tests/data/get_nodes.json")


@pytest.mark.parametrize(
    PARAMS,
    [
        (
            "tests/data/get_queues_without_policies.json",
            get_test_queue_without_policy(
                "tests/data/queues_without_policy.json"
            ),
        ),
        ("tests/data/get_queues_with_policies.json", []),
    ],
)
def test_queue_without_policy(queues, expected):
    client = MockRabbit(queues)
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.queues_without_policy() == expected


def test_hash_bucket():
    m = {}

    for i in range(1000):
        a = bucket(str(i), 3)
        m[a] = m.setdefault(a, 0) + 1

    assert m == {0: 340, 1: 328, 2: 332}


def test_calculate_queues_on_hosts():
    client = MockRabbit("tests/data/get_queues_without_policies.json")
    rabbit_info = RabbitData(
        client, get_json(POLICY_GROUPS), DRY_RUN, WAIT_SLEEP
    )
    assert rabbit_info.calculate_queues_on_hosts == get_json(
        "tests/data/calculate_queues.json"
    )


# for i in range(15):
#     name = 'app.delayed+{}'.format(i)
#     client.create_queue('my_app', name)
# for i in range(1,13):
#     name = 'app.delayed-{}'.format(i)
#     client.create_queue('my_app', name)
# for i in range(120):
#     name = 'worker_writer.queue.delay.part.{}'.format(i)
#     client.delete_policy('first_vhost', name)
# for i in range(120):
#     name = 'queue.part.{}'.format(i)
#     client.delete_policy('some_vhost', name)
