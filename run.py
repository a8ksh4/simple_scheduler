#!/usr/bin/env python3
'''Client interface to schedule tasks on a simple_scheduler cluster.'''

import subprocess as sp
import argparse
import json
import fcntl
import os
import tempfile
import hashlib
import time
import sys

TMP_DIR = '/dev/shm/simple_scheduler'
MY_DIR = '/home/dan/git/simple_scheduler'
with open(f'{MY_DIR}/settings.json', 'r') as f:
    settings = json.load(f)
TIMEOUT = settings['timeout']
SERVER = settings['server']
os.umask(0o044)

def parse_args():
    '''Args stuff'''
    description = '''Client interface to schedule tasks on a simple_scheduler
            cluster.'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-s', '--server', type=str,
                        help=f'Server to connect to. Default: {SERVER}',
                        default=SERVER)
    parser.add_argument('-r', '--run', action='store_true',
                        help='Run the command on the cluster.')
    parser.add_argument('-l', '--list', action='store_true',
                        help='List all running jobs on the cluster')
    parser.add_argument('-k', '--kill', type=int,
                        help='Kill a job on the cluster.')
    parser.add_argument('-n', '--not_a_user_option', action='store_true',
                        help='Do not use this.')
    parser.add_argument('-c', '--command', type=str,
                        help='Command to run', action='store')
    args = parser.parse_args()

    exclusive = [a for a in (args.run, args.list, args.kill) if a]
    if len(exclusive) > 1:
        parser.error('Only one of --run, --list, or --kill can be used.')
    return args


def server_side_operations():
    '''This is executed on the server with each request. It will generate a tmp file, 
    lock it, write the request, unlock it, wait for the server to lock the file, process
    the request, and unlock the file again.  Then this function will return the exit code
    and response message.'''
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
        os.chmod(TMP_DIR, 0o777)

    response = {'exit_code': 0, 'message': 'Success'}

    # capture the request on stdin
    request = json.loads(input())
    tmp_file = tempfile.NamedTemporaryFile(dir=TMP_DIR, delete=False)
    os.chmod(tmp_file.name, 0o755)
    with open(tmp_file.name, 'w', encoding='utf-8') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(json.dumps(request))
    with open(tmp_file.name, 'r', encoding='utf-8') as f:
        file_md5 = hashlib.md5(f.read().encode()).hexdigest()
        fcntl.flock(f, fcntl.LOCK_UN)

    # wait for the file md5 to change
    epoch_seconds = time.time()
    while True:
        with open(tmp_file.name, 'r', encoding='utf-8') as f:
            contents = f.read()
        new_md5 = hashlib.md5(contents.encode()).hexdigest()
        if file_md5 != new_md5:
            # Server Responded
            # print(json.dumps(response))
            break
        time.sleep(1)
        if time.time() - epoch_seconds > TIMEOUT:
            # Request Timed Out!
            response = {'exit_code': 1, 'message': 'Timeout'}
            print(json.dumps(response))
            sys.exit()

    # get a lock, read the response
    with open(tmp_file.name, 'r', encoding='utf-8') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        response = f.read()
        fcntl.flock(f, fcntl.LOCK_UN)
    # client is waiting for stdout...
    print(response)


def communicate_to_server(args):
    '''This will ssh to the server and call this script again using the -n option.
    The arguments used here will be passed vi stdin to the commmand on the server, and
    a response will be collected on stdout. This function will then print the response
    and exit with the returned exit code.'''
    ssh_cmd = ['ssh', args.server, __file__, '-n']
    payload = json.dumps(vars(args)).encode()
    print(payload)
    # run the ssh command with subprocess, pass the payload onstdin, and
    # blocking capture stdout until the process exits.
    p = sp.Popen(ssh_cmd, stdin=sp.PIPE, stdout=sp.PIPE)
    stdout, stderr = p.communicate(payload)
    print(stderr)
    print(stdout)
    response = json.loads(stdout)
    sys.exit(response['exit_code'])


if __name__ == '__main__':
    ARGS = parse_args()

    if ARGS.not_a_user_option:
        server_side_operations()
    else:
        communicate_to_server(ARGS)
