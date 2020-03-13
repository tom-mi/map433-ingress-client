#!/usr/bin/env python3

import argparse
import configparser
import json
import logging
import os.path
import subprocess
import sys
import uuid
from datetime import datetime

import requests
from requests import RequestException

RTL_433_CMD = ['rtl_433', '-F', 'json', '-M', 'time:iso:usec:utc', '-M', 'level']


def main():
    args = parse_args()
    loglevel = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=loglevel)
    if args.write_config:
        write_sample_config(os.path.expanduser(args.config))
        return

    config = configparser.ConfigParser()
    config.read(os.path.expanduser(args.config))

    latitude = float(config['position']['latitude'])
    longitude = float(config['position']['longitude'])

    receiver_id = config['rtl_433']['receiver_id']

    ingress_endpoint = config['map433']['ingress_endpoint']
    api_key = config['map433']['api_key']

    logging.info(f'Position (fixed): ({latitude}, {longitude})')
    logging.info(f'Receiver id: {receiver_id}')
    logging.info(f'Ingress endpoint: {ingress_endpoint}')
    logging.info('Waiting for input.')
    print(args.command)
    with subprocess.Popen(args.command, stdout=subprocess.PIPE, encoding='utf8') as proc:
        while proc.returncode is not None:
            line = proc.stdout.readline().strip()
            logging.debug(f'Received line {line}')
            body = None
            try:
                event = json.loads(line)
                now = datetime.now()
                eid = f'{receiver_id}/{now.isoformat()}Z'
                body = {
                    'event_type': 'rtl_433',
                    'receiver_id': receiver_id,
                    'eid': eid,
                    'position': {'latitude': latitude, 'longitude': longitude},
                    'payload': event,
                }
                headers = {
                    'x-api-key': api_key,
                }
                requests.post(ingress_endpoint, json=body, headers=headers).raise_for_status()
                logging.debug(f'Event sent to ingress API with eid={eid}')
            except ValueError as e:
                logging.warning(f'Could not parse line {line}: {e}"')
            except RequestException as e:
                logging.error(f'Could not send request {body}: {e}')


def write_sample_config(path):
    if os.path.exists(path):
        print(f'Target path {path} already exists. Not overwriting.')
        sys.exit(1)
    with open(path, 'w') as f:
        f.writelines([
            '[map433]\n',
            'ingress_endpoint=https://api.map433.de/ingress\n',
            'api_key=  # FIXME\n'
            '\n',
            '[position]\n',
            'latitude=  # FIXME\n',
            'longitude= # FIXME\n',
            '\n',
            '[rtl_433]\n',
            f'receiver_id={uuid.uuid4()}\n',
        ])


def parse_args():
    parser = argparse.ArgumentParser('rtl_433_ingress_client')
    parser.add_argument('--config', metavar='CONFIG', default='~/.map433',
                        help='Path to config. Defaults to ~/.map433')
    parser.add_argument('--write-config', help='Write a sample config the CONFIG path.', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--command', nargs='+',
                        help='rtl_433 command line. Defaults to ' + ' '.join(RTL_433_CMD), default=RTL_433_CMD)
    return parser.parse_args()


if __name__ == '__main__':
    main()
