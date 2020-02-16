import pytest
import json
from rabbitmq_polycymaker.rabbitmq_policy import RabbitData, bucket


def get_json(file):
    with open(file, 'r') as json_file:
        data = json.load(json_file)
    return data


class MockRabbit:
    def get_vhost_names(self):
        return ['/', 'first_vhost', 'my_app', 'second_vhost', 'some_vhost']

    def get_queues(self):
        return get_json('tests/data/get_queues.json')

    def get_all_policies(self, file):
        return get_json(file)

    def create_policy(self, vhost, policy_name, **kwargs):

        body = json.dumps(kwargs)
        path = "'policy': 'policies/{}/{}'".format(vhost, policy_name)
        policy = self._call(path, 'PUT', body=body,
                            headers=Client.json_headers)
        return policy


def test_queue():
    client = MockRabbit()
    rabbit_info = RabbitData(client)
    assert rabbit_info.queues() == get_json('tests/data/queues.json')


def test_empty_policies():
    client = MockRabbit()
    rabbit_info = RabbitData(client)
    rabbit_info.get_all_policies(
        'tests/data/get_all_policies_empty.json'
    )
    assert rabbit_info.policies() == get_json('tests/data/empty_policies.json')


def test_queues_without_policy():
    client = MockRabbit()
    rabbit_info = RabbitData(client)
    queues_dict = rabbit_info.queues()
    policies_dict = rabbit_info.policies()
    queues_without_policy = rabbit_info.queues_without_policy(
        queues_dict, policies_dict
    )
    assert queues_without_policy == get_json(
        'tests/data/queues_without_policy.json'
    )


def test_need_a_policy():
    client = MockRabbit()
    rabbit_info = RabbitData(client)
    queues_dict = rabbit_info.queues()
    policies_dict = rabbit_info.policies()
    queues_without_policy = rabbit_info.queues_without_policy(
        queues_dict, policies_dict
    )
    need_a_policy = rabbit_info.need_a_policy(queues_without_policy)
    assert need_a_policy is True


def test_hash_bucket():
    m = {}

    for i in range(1000):
        a = bucket(str(i), 3)
        m[a] = m.setdefault(a, 0) + 1

    assert m == {0: 340, 1: 328, 2: 332}


# def test_create_one_policy():
#     client = MockRabbit()
#     rabbit_info = RabbitData(client)
#     queue_info = get_json('tests/data/one_queue.json')
#     vhost = queue_info.keys()
#     print('vhost', vhost)
#     queue = queue_info.values()
#     print('queue', queue)
#     policy_groups = get_json('tests/data/policy_groups.json')
#     rabbit_info.create_policy(vhost, queue, policy_groups)
#     print(rabbit_info)


