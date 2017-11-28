import logging
from argparse import ArgumentParser


log = logging.getLogger(__name__)


def _build_args():
    """
    Build argument parser and actually parse them at runtime.
    """
    parser = ArgumentParser(description='Nyuki implementation')
    parser.add_argument(
        '-l', '--logging',
        help='debug mode',
        required=False,
        choices=[
            'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
        ])
    parser.add_argument('-c', '--config',
                        help='config file name', required=False)
    parser.add_argument('-s', '--server',
                        help='bus host server: <host>[:<port>]', required=False)
    parser.add_argument('-a', '--api',
                        help='api binding: <host>[:<port>]', required=False)
    return parser.parse_args()


def get_command_kwargs():
    args = _build_args()

    command_args = {
        'api': {}
    }

    # Split host/port
    if args.server:
        server = args.server.split(':')
        if len(server) == 1:
            command_args['bus'] = {
                **command_args.get('bus', {}),
                'host': server[0]
            }
        else:
            command_args['bus'] = {
                **command_args.get('bus', {}),
                'host': server[0],
                'port': int(server[1])
            }

    # Split API host/port
    if args.api:
        api = args.api.split(':')
        try:
            command_args['api'].update(host=api[0], port=int(api[1]))
        except IndexError:
            command_args['api'].update(host=args.api)

    # Set logging root level
    if args.logging:
        command_args['log'] = {
            'root': {
                'level': args.logging
            }
        }

    if args.config:
        command_args['config'] = args.config

    return command_args
