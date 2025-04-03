#!/usr/bin/env python3
'''Server proces for the simple Scheduler.  It works in a loop:
    1. Check the state of the cluster.
    2. Check for new requests.
    3. Process the requests.
    4. Check on running tasks.
    5. Write the state if there have been any changes.'''

import argparse
import json
import fcntl
import logging
import os
import time
import pandas as pd
import subprocess as sp

MY_DIR = '/home/dan/git/simple_scheduler'
STATE_DIR = f'{MY_DIR}/state'
LOG_FILE = f'{MY_DIR}/simple.log'
MUTEX_FILE = f'{MY_DIR}/mutex'
REQUEST_DIR = '/dev/shm/simple_scheduler'

if not os.path.exists(REQUEST_DIR):
    os.makedirs(REQUEST_DIR)
    os.chmod(REQUEST_DIR, 0o777)


def get_logger():
    '''Get a logger that writes to a file and stdout.'''
    logger = logging.getLogger('simple_scheduler')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def parse_args():
    '''Desn't do much aside from providin help output and some explanatoin of the server
    behavior.  Talks aobut machines.json format.'''
    description = '''Server proces for the simple Scheduler.   It receives requests to run tasks on
            a cluster of machines and schedules them to run.'''
    epilog = '''...'''
    parser = argparse.ArgumentParser(description=description,
                                        epilog=epilog)
    parser.add_argument('-m', '--machines_file', type=str, required=True,
                        help='Path to the machines.json file.')
    return parser.parse_args()


def get_request():
    '''Get a lock on the oldest request file not owned by root, lock it, read the request,
    handle the request, write the response, chown it to root, and unlock the file.'''
    request_files = sorted(os.listdir(REQUEST_DIR))
    unprocessed_request_files = [f for f in request_files
                                 if os.stat(os.path.join(REQUEST_DIR, f)).st_uid != 0]
    if len(unprocessed_request_files) == 0:
        return False
    request_file = unprocessed_request_files[0]
    request_file = os.path.join(REQUEST_DIR, request_file)
    with open(request_file, 'r', encoding='utf-8') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        request = json.loads(f.read())
        fcntl.flock(f, fcntl.LOCK_UN)
    return request, request_file


def read_state():
    '''Working from newest to oldest, check state files in the state dir
    and try to read them into a pandas dataframe.  If one of them is invalid
    and an exception is caught, try the next newest.  Once a good state file is
    read, return the dataframe.'''
    if not os.path.exists(STATE_DIR):
        os.makedirs(STATE_DIR)
        os.chmod(STATE_DIR, 0o755)
    for state_file in sorted(os.listdir(STATE_DIR), reverse=True):
        try:
            state_file = os.path.join(STATE_DIR, state_file)
            return pd.read_json(state_file)
        except pd.errors.ParserError:
            continue
    return pd.DataFrame()


def write_state(df):
    '''Write the dataframe to a new state file in the state dir.  Keep ten state fils
    and purge the rest.  Validate that the written states file is valid before doing cleanup.'''
    state_file = os.path.join(STATE_DIR, str(int(time.time())))
    df.to_json(state_file)

    if read_state() is not None:
        all_state_files = sorted(os.listdir(STATE_DIR))
        if len(all_state_files) > 10:
            for state_file in all_state_files[:-10]:
                os.remove(os.path.join(STATE_DIR, state_file))


def process_request(request, state):
    if request['command'] == 'run':
        user_result, state_item = start_task(request)
    elif request['command'] == 'list':
        user_result = state.to_json()
        state_item = None
    elif request['command'] == 'kill':
        user_result, state_item = kill_task(request)
    else:
        user_result = 'Invalid command'
        state_item = None
    with open(request['response_file'], 'w', encoding='utf-8') as f:
        f.write(json.dumps(user_result))
    return state_item


def start_task(request):
    pass

def start_agent(hostname):
    '''Agent is a bash process that we can use to spawn jobs for users,
    check running processes, check other machines states.  The agent 
    processes stays running and takes commands on stdin.'''
    agent_cmd = ['ssh',
                 '-o', 'StrictHostKeyChecking=no',
                 '-o', 'batchmode=yes', 
                 '-o', 'connectiontimeout=5', 
                 hostname, '/bin/bash']
    p = sp.Popen(agent_cmd, shell=False, encoding='utf-8',
                 stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
    return p


def main_loop():
    '''Main loop for the server.'''
    while True:
        # Check the agents
        for hostname, machine in MACHINES.items():
            if 'connection' not in machine:
                machine['connection'] = start_agent(hostname)
            elif machine['connection'].poll() is not None:
                machine['connection'] = start_agent(hostname)
            
        # Check for new requests
        request = get_request()
        if request is None:
            break
        # Process the request
        result = process_request(request)
        if result:
            pd.concat([STATE, result])
        # Check on running tasks
        STATE = check_running_tasks(STATE)
        # Write state if there have been any changes
        pass


if __name__ == '__main__':
    ARGS = parse_args()

    # Verify running as root
    if os.geteuid() != 0:
        print('This tool must be run as root.')
        # exit(1)

    # Get lock on the mutex or quit.

    # Get a logger
    LOG = get_logger()

    STATE = read_state()
    MACHINES = pd.read_json(parse_args().machines_file)
    MACHINES = dict(zip([m['hostname'] for m in MACHINES], MACHINES))

    while True:
        try:
            main_loop()
        except Exception as e:
            LOG.exception(e)
            break

    # while True:
    #     # Check the agent
    #     print(MACHINES.head())
    #     # Check for new requests
    #     for count in range(10):
    #         request = get_request()
    #         if request is None:
    #             break
    #         # Process the request
    #         result = process_request(request)
    #         if result:
    #             pd.concat([STATE, result])
    #     # Check on running tasks
    #     STATE = check_running_tasks(STATE)
    #     # Write state if there have been any changes
    #     pass
 