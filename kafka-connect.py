import argparse
import json
import sys
from enum import IntEnum

import requests


class State(IntEnum):
    UNASSIGNED = 1
    RUNNING = 2
    PAUSED = 3
    FAILED = 4


TIMEOUT = 5


def health_check(base_url):
    try:
        for connector_name in _get_connectors(base_url):
            connector_status = _get_connector_status(base_url, connector_name)
            connector_state = State[connector_status['connector']['state']]
            if connector_state != State.RUNNING:
                print(f'Connector {connector_name} in state {connector_state.name}')
                return 1
            # Need to read list of tasks and then check status of each task, even if we can get task id from connector.
            # For some reason API returns status as 'RUNNING' when Kakfa is down.
            for task_id in map(lambda t: t['id']['task'], _get_tasks(base_url, connector_name)):
                task = _get_task_status(base_url, connector_name, task_id)
                task_state = State[task['state']]
                if task_state != State.RUNNING:
                    print(f'Task {task_id} of connector {connector_name} in state {task["state"]}')
                    return 1
            print(f'Connector {connector_name} - OK')  # todo: remove
        return 0
    except Exception as err:
        print(err)
        return 1


def restart_task(base_url, connector_name, task_id):
    try:
        response = requests.post(f'{base_url}/connectors/{connector_name}/tasks/{task_id}/restart', timeout=TIMEOUT)
        if _is_2xx(response.status_code):
            raise RuntimeError(
                f'Unable to restart task, response status: {response.status_code}, message: {response.text}')
    except Exception as err:
        print(err)


def restart_connector(base_url, connector_name):
    try:
        response = requests.post(f'{base_url}/connectors/{connector_name}/restart', timeout=TIMEOUT)
        if _is_2xx(response.status_code):
            raise RuntimeError(
                f'Unable to restart task, response status: {response.status_code}, message: {response.text}')
    except Exception as err:
        print(err)


def connectors(base_url):
    connector_states = []
    for connector_name in _get_connectors(base_url):
        connector_status = _get_connector_status(base_url, connector_name)
        connector_state = State[connector_status['connector']['state']]
        failed_tasks = []
        try:
            for task in _get_tasks(base_url, connector_name):
                task_id = task['id']['task']
                task = _get_task_status(base_url, connector_name, task_id)
                task_state = State[task['state']]
                if task_state == State.FAILED:
                    failed_tasks.append(task_id)
                if task_state > connector_state:
                    connector_state = task_state
        except Exception:
            connector_state = State.FAILED
        connector_states.append(
            {'connector': connector_name, 'state': connector_state.name, 'failedTasks': failed_tasks})
    print(json.dumps(connector_states, indent=4))


def connector_tasks(base_url, connector_name):
    tasks_states = []
    # Check connector first, not status. For some reason API returns status as 'RUNNING' when Kakfa is down.
    _get_json(f'{base_url}/connectors/{connector_name}')
    for task in _get_connector_status(base_url, connector_name)['tasks']:
        tasks_states.append({'taskId': task['id'], 'state': task['state'], 'trace': task.get('trace', '')})
    print(json.dumps(tasks_states, indent=4))


def _get_connectors(base_url):
    return _get_json(f'{base_url}/connectors')


def _get_connector_status(base_url, connector_name):
    return _get_json(f'{base_url}/connectors/{connector_name}/status')


def _get_tasks(base_url, connector_name):
    return _get_json(f'{base_url}/connectors/{connector_name}/tasks')


def _get_task_status(base_url, connector_name, task_id):
    return _get_json(f'{base_url}/connectors/{connector_name}/tasks/{task_id}/status')


def _get_json(url):
    response = requests.get(url, timeout=TIMEOUT)
    if response.status_code == 200:
        return response.json()
    raise RuntimeError(f'GET {url}: response status: {response.status_code}, message: {response.text}')


def _is_2xx(response_code):
    return int(response_code / 100) != 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help='Kafka connect server URL', required=False, default='http://localhost:8083')

    subparsers = parser.add_subparsers(help='Commands', dest='cmd')
    subparsers.add_parser('health-check', help='Check all connectors and their tasks')

    restart_task_args = subparsers.add_parser('restart-task', help='Restart connector task')
    restart_task_args.add_argument('--connector', help='Connector name', required=True)
    restart_task_args.add_argument('--task', help='Task ID', required=True)

    restart_connector_args = subparsers.add_parser('restart-connector', help='Restart connector')
    restart_connector_args.add_argument('--connector', help='Connector name', required=True)

    connectors_args = subparsers.add_parser('connectors', help='List connectors')

    connector_tasks_args = subparsers.add_parser('connector-tasks', help='List connector\'s tasks')
    connector_tasks_args.add_argument('--connector', help='Connector name', required=True)

    args = parser.parse_args()
    if args.cmd == 'health-check':
        sys.exit(health_check(args.url))
    elif args.cmd == 'restart-task':
        restart_task(args.url, args.connector, args.task)
    elif args.cmd == 'restart-connector':
        restart_connector(args.url, args.connector)
    elif args.cmd == 'connectors':
        connectors(args.url)
    elif args.cmd == 'connector-tasks':
        connector_tasks(args.url, args.connector)
    else:
        raise ValueError(args.cmd)
