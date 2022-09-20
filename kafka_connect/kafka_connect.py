import cgi
import time
from enum import IntEnum

import requests


class State(IntEnum):
    UNASSIGNED = 1
    RUNNING = 2
    PAUSED = 3
    FAILED = 4


class ApiError(Exception):
    def __init__(self, status, message=''):
        self.status = status
        self.message = message

    def __str__(self):
        return self.message


TIMEOUT = 5


def health_check(base_url, verbose=False):
    try:
        for connector_name in _get_connectors(base_url):
            connector_status = _get_connector_status(base_url, connector_name)
            connector_state = State[connector_status['connector']['state']]
            if connector_state != State.RUNNING:
                if verbose:
                    print(f'Connector {connector_name} in state {connector_state.name}')
                return 1
            # Need to read list of tasks and then check status of each task, even if we can get task id from connector.
            # For some reason API returns status as 'RUNNING' when Kakfa is down.
            for task_id in map(lambda t: t['id']['task'], _get_tasks(base_url, connector_name)):
                task = _get_task_status(base_url, connector_name, task_id)
                task_state = State[task['state']]
                if task_state != State.RUNNING:
                    if verbose:
                        print(f'Task {task_id} of connector {connector_name} in state {task["state"]}')
                    return 1
        return 0
    except requests.ConnectionError:
        if verbose:
            print(f'Connection to {base_url} refused')
        return 1
    except Exception as err:
        if verbose:
            print(err)
        return 1


def list_connectors(base_url):
    return _list_connectors(base_url)


def create_connector(base_url, connector_name, configuration, if_not_exists=False, backoff_limit=1, delay=0):
    return _retry(lambda: _create_connector(base_url, connector_name, configuration, if_not_exists),
                  lambda err: isinstance(err, requests.ConnectionError),
                  backoff_limit,
                  delay)


def update_connector(base_url, connector_name, configuration, backoff_limit=1, delay=0):
    return _retry(lambda: _update_connector(base_url, connector_name, configuration),
                  lambda err: isinstance(err, requests.ConnectionError),
                  backoff_limit,
                  delay)


def restart_connector(base_url, connector_name, backoff_limit=1, delay=0):
    _retry(lambda: _restart_connector(base_url, connector_name),
           lambda err: isinstance(err, requests.ConnectionError),
           backoff_limit,
           delay)


def delete_connector(base_url, connector_name, backoff_limit=1, delay=0):
    _retry(lambda: _delete_connector(base_url, connector_name),
           lambda err: isinstance(err, requests.ConnectionError),
           backoff_limit,
           delay)


def list_connector_tasks(base_url, connector_name):
    return _list_connector_tasks(base_url, connector_name)


def restart_connector_task(base_url, connector_name, task_id, backoff_limit=1, delay=0):
    _retry(lambda: _restart_connector_task(base_url, connector_name, task_id),
           lambda err: isinstance(err, requests.ConnectionError),
           backoff_limit,
           delay)


def _list_connectors(base_url):
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
    return connector_states


def _create_connector(base_url, name, configuration, if_not_exists):
    try:
        _get_connector(base_url, name)
        if not if_not_exists:
            raise RuntimeError(f'Connector {name} already exists')
    except ApiError as e:
        if e.status == 404:
            return _post_json(f'{base_url}/connectors', {'name': name, 'config': configuration})
        else:
            raise e


def _update_connector(base_url, name, configuration):
    return _put_json(f'{base_url}/connectors/{name}/config', configuration)


def _restart_connector(base_url, name):
    _post_json(f'{base_url}/connectors/{name}/restart', None)


def _delete_connector(base_url, name):
    _delete(f'{base_url}/connectors/{name}')


def _list_connector_tasks(base_url, connector_name):
    tasks_states = []
    # Check connector first, not status. For some reason API returns status as 'RUNNING' when Kafka is down.
    _get_json(f'{base_url}/connectors/{connector_name}')
    for task in _get_connector_status(base_url, connector_name)['tasks']:
        tasks_states.append({'taskId': task['id'], 'state': task['state'], 'trace': task.get('trace', '')})
    return tasks_states


def _restart_connector_task(base_url, connector_name, task_id):
    _post_json(f'{base_url}/connectors/{connector_name}/tasks/{task_id}/restart', None)


def _retry(func, is_retryable, backoff_limit, delay):
    err = None
    count = 0
    while count < backoff_limit:
        try:
            return func()
        except Exception as e:
            err = e
            if not is_retryable(e):
                break
            else:
                count += 1
                time.sleep(delay)
    raise err


def _get_connectors(base_url):
    return _get_json(f'{base_url}/connectors')


def _get_connector_status(base_url, connector_name):
    return _get_json(f'{base_url}/connectors/{connector_name}/status')


def _get_connector(base_url, connector_name):
    return _get_json(f'{base_url}/connectors/{connector_name}')


def _get_tasks(base_url, connector_name):
    return _get_json(f'{base_url}/connectors/{connector_name}/tasks')


def _get_task_status(base_url, connector_name, task_id):
    return _get_json(f'{base_url}/connectors/{connector_name}/tasks/{task_id}/status')


def _get_json(url):
    response = requests.get(url, timeout=TIMEOUT)
    if response.status_code == 200:
        return response.json()
    raise ApiError(response.status_code,
                   f'GET {url}: response status: {response.status_code}, message: {response.text}')


def _post_json(url, data):
    response = requests.post(url, timeout=TIMEOUT, json=data)
    if _is_not_2xx(response.status_code):
        raise ApiError(response.status_code,
                       f'POST {url}: response status: {response.status_code}, message: {response.text}')
    if _is_json_response(response):
        return response.json()
    else:
        return None


def _put_json(url, data):
    response = requests.put(url, timeout=TIMEOUT, json=data)
    if _is_not_2xx(response.status_code):
        raise ApiError(response.status_code,
                       f'PUT {url}: response status: {response.status_code}, message: {response.text}')
    if _is_json_response(response):
        return response.json()
    else:
        return None


def _delete(url):
    response = requests.delete(url, timeout=TIMEOUT)
    if _is_not_2xx(response.status_code):
        raise ApiError(response.status_code,
                       f'DELETE {url}: response status: {response.status_code}, message: {response.text}')


def _is_not_2xx(response_code):
    return int(response_code / 100) != 2


def _is_json_response(response):
    mimetype, options = cgi.parse_header(response.headers.get('Content-Type', ''))
    return mimetype == 'application/json'
