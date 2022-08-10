import argparse
import sys

from kafka_connect.kafka_connect import health_check, restart_task, restart_connector, connectors, connector_tasks


def main():
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
        print(parser.format_help())


if __name__ == '__main__':
    main()
