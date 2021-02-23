import yaml
from collections import namedtuple
import sys
import time
import os
import requests
import functools
import dns.resolver
import operator
import logging


DnsResponse = namedtuple('DnsResponse', ['ipv4', 'expiry', 'response_time'])
HostConfiguration = namedtuple('HostConfiguration', ['name', 'webhook_uri'])

def _load_configuration(path):
    with open(path, 'r') as f:
        data = yaml.load(f)

    return {k: HostConfiguration(v['name'], v['webhook_uri']) for k, v in data.items()}


def _load_response_cache(path):
    try:
        with open(path, 'r') as f:
            cache = yaml.load(f)

        if cache is None:
            return {}

        return {k: DnsResponse(v['ipv4'], v['expiry'], v['response_time']) for k, v in cache.items()}
    except FileNotFoundError:
        logging.error(f'Cache file "{path}" not found')

        return {}


def _save_response_cache(path, responses):
    dict_values = {k: v._asdict() for k, v in responses.items()}

    with open(path, 'w') as f:
        yaml.dump(dict_values, f)


def _is_response_stale(responses, host):
    response = responses.get(host)

    if response is None:
        return True

    return response.expiry < time.time()


def _notify_webhook(host, host_config, response):
    content = f'IP address for **{host_config.name}** ({host}) is now **{response.ipv4}**'

    requests.post(
        host_config.webhook_uri,
        headers={'Content-Type': 'application/json'},
        json={'content': content}
    )


def _check_dns(host):
    logging.debug(f'Retrieving A record for {host}')

    answers = dns.resolver.query(host, 'A')

    answer = answers[0]

    response = DnsResponse(answer.address, answers.expiration, time.time())
    logging.debug(f'Received response: {response}')

    return response


def main():
    logging.basicConfig(level=logging.DEBUG)

    config = _load_configuration(os.environ.get('CONFIG_FILE', 'config.yml'))
    logging.info(f'Loaded configuration: {config}')

    response_cache_path = os.environ.get('CACHE_FILE', 'response_cache.yml')
    responses = _load_response_cache(response_cache_path)
    logging.info(f'Cached responses: {responses}')

    stale_check_predicate = functools.partial(_is_response_stale, responses)

    while True:
        stale_hosts = filter(stale_check_predicate, config.keys())

        for stale_host in stale_hosts:
            response = _check_dns(stale_host)

            old_response = responses.get(stale_host)

            if old_response is None or old_response.ipv4 != response.ipv4:
                old_ipv4 = None if old_response is None else old_response.ipv4
                logging.info(f'{stale_host} IP address changed to {response.ipv4} from {old_ipv4}')
                _notify_webhook(stale_host, config[stale_host], response)

            responses[stale_host] = response
            _save_response_cache(response_cache_path, responses)

        next_check = min(map(operator.attrgetter('expiry'), responses.values()))

        sleep_time = max(30, next_check - time.time())
        logging.debug(f'Sleeping {sleep_time} seconds')

        time.sleep(sleep_time)


if __name__ == '__main__':
    main()