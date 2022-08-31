import argparse
import json
import sys
from argparse import Action

from kafka_connect.kafka_connect import health_check, list_connectors, create_connector, update_connector, \
    restart_connector, delete_connector, list_connector_tasks, restart_connector_task


class ParseConfigurationFileAction(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values == '-':
            setattr(namespace, 'configuration', sys.stdin.read())
        else:
            with open(values, 'r') as f:
                setattr(namespace, 'configuration', f.read())


def exception_handler(exception_type, exception, traceback):
    print(f'Error: {exception}')


def main():
    sys.excepthook = exception_handler
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('--url', help='Kafka connect server URL', required=False,
                               default='http://localhost:8083')
    backoff_parser = argparse.ArgumentParser(add_help=False)
    backoff_parser.add_argument('--backoff-limit', default=1, help='Number of retries before fail', type=int)
    backoff_parser.add_argument('--delay', default=0, help='How long to wait in seconds between retry attempts',
                                type=int)

    parser = argparse.ArgumentParser()

    # commands
    main_command_parser = parser.add_subparsers(dest='cmd', help='Commands', title='Commands')
    health_check_command_parser = main_command_parser.add_parser('health-check',
                                                                 help='Check all connectors and their tasks',
                                                                 parents=[common_parser])
    connector_command_parser = main_command_parser.add_parser('connector', help='Connector commands')
    connector_task_parser = main_command_parser.add_parser('task', help='Connector task commands')

    # health check
    health_check_command_parser.add_argument('--verbose', action='store_true', default=False)

    # connector
    connector_subcommand_parser = connector_command_parser.add_subparsers(dest='connector_command')

    # list
    connector_subcommand_parser.add_parser('list', help='List connectors', parents=[common_parser])

    # create
    connector_common_parser = argparse.ArgumentParser(add_help=False)
    connector_common_parser.add_argument('--name', help='Connector name', required=True)

    create_connector_command_parser = connector_subcommand_parser.add_parser('create', help='Create new connector',
                                                                             parents=[common_parser, backoff_parser,
                                                                                      connector_common_parser])
    create_connector_configuration = create_connector_command_parser.add_mutually_exclusive_group(required=True)
    create_connector_configuration.add_argument('--configuration', help='Connector configuration as JSON string')
    create_connector_configuration.add_argument('--configuration-file',
                                                help='Path to file with connector configuration in JSON format',
                                                action=ParseConfigurationFileAction)
    create_connector_command_parser.add_argument('--if-not-exists', default=False,
                                                 help='Attempt to create connector only if connector does not already exist',
                                                 action='store_true')

    # update
    update_connector_command_parser = connector_subcommand_parser.add_parser('update', help='Update connector',
                                                                             parents=[common_parser, backoff_parser,
                                                                                      connector_common_parser])
    update_connector_configuration = update_connector_command_parser.add_mutually_exclusive_group(required=True)
    update_connector_configuration.add_argument('--configuration', help='Connector configuration as JSON string')
    update_connector_configuration.add_argument('--configuration-file',
                                                help='Path to file with connector configuration in JSON format',
                                                action=ParseConfigurationFileAction)

    # restart
    connector_subcommand_parser.add_parser('restart', help='Restart connector',
                                           parents=[common_parser, backoff_parser, connector_common_parser])

    # delete
    connector_subcommand_parser.add_parser('delete', help='Delete connector',
                                           parents=[common_parser, backoff_parser, connector_common_parser])

    # connector task
    connector_task_common_parser = argparse.ArgumentParser(add_help=False)
    connector_task_common_parser.add_argument('--connector', help='Connector name', required=True)

    connector_task_subcommand_parser = connector_task_parser.add_subparsers(dest='task_command')
    # list
    connector_task_subcommand_parser.add_parser('list', help='List connector tasks',
                                                parents=[common_parser, connector_task_common_parser])

    # restart
    restart_connector_task_command_parser = connector_task_subcommand_parser.add_parser('restart',
                                                                                        help='Restart connector task',
                                                                                        parents=[common_parser,
                                                                                                 backoff_parser,
                                                                                                 connector_task_common_parser])
    restart_connector_task_command_parser.add_argument('--task', help='Task ID', required=True)

    args = parser.parse_args()

    if args.cmd == 'health-check':
        sys.exit(health_check(args.url, args.verbose))
    elif args.cmd == 'connector':
        if args.connector_command == 'list':
            print(json.dumps(list_connectors(args.url), indent=4))
        elif args.connector_command == 'create':
            print(json.dumps(
                create_connector(args.url, args.name, json.loads(args.configuration), args.if_not_exists,
                                 args.backoff_limit,
                                 args.delay),
                indent=4))
        elif args.connector_command == 'update':
            print(json.dumps(
                update_connector(args.url, args.name, json.loads(args.configuration), args.backoff_limit, args.delay),
                indent=4))
        elif args.connector_command == 'restart':
            restart_connector(args.url, args.name, args.backoff_limit, args.delay)
        elif args.connector_command == 'delete':
            delete_connector(args.url, args.name, args.backoff_limit, args.delay)
        else:
            print(parser.format_help())
    elif args.cmd == 'task':
        if args.task_command == 'list':
            print(json.dumps(list_connector_tasks(args.url, args.connector), indent=4))
        elif args.task_command == 'restart':
            restart_connector_task(args.url, args.connector, args.task, args.backoff_limit, args.delay)
        else:
            print(parser.format_help())
    else:
        print(parser.format_help())


if __name__ == '__main__':
    main()
