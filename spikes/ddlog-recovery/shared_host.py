#!/usr/bin/env python3
"""Independent pipe clients against an actual shared DDlog host.

Requires the same operator-supplied DDlog build environment as test-ddlog-mcp.py.
No PTY, provider calls, or simulated graph evaluation is used.
"""
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time

BINARY = os.path.abspath(os.environ.get('LEMMALOG_DDLOG_MCP', 'target/debug/lemmalog-ddlog-mcp'))
BINARY_SHA256 = hashlib.sha256(Path(BINARY).read_bytes()).hexdigest()
TIMEOUT = float(os.environ.get('SHARED_TEST_TIMEOUT', '180'))
RECEIPT = Path(os.environ.get('SHARED_INSTANCE_RECEIPT', '/tmp/shared-instance-receipt.json'))
checks = []
builds = []
hosts = []
clients = []


def checked(name, **evidence):
    checks.append({'check': name, **evidence})


class Client:
    def __init__(self, descriptor, env):
        self.errors = tempfile.TemporaryFile()
        self.process = subprocess.Popen(
            [BINARY, 'connect', '--descriptor', str(descriptor)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors,
            env=env, bufsize=0)
        self.lines = queue.Queue()
        self.sequence = 0
        def read():
            try:
                while True:
                    line = self.process.stdout.readline()
                    self.lines.put(line)
                    if not line:
                        break
            except Exception as exc:
                self.lines.put(exc)
        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
        clients.append(self)

    def rpc(self, method, params, fragmented=False):
        self.sequence += 1
        request = {'jsonrpc': '2.0', 'id': self.sequence, 'method': method, 'params': params}
        payload = (json.dumps(request, separators=(',', ':')) + '\n').encode()
        # Fragment large requests deliberately while preserving one JSON-RPC line.
        size = 997 if fragmented else len(payload)
        for start in range(0, len(payload), size):
            remaining = memoryview(payload)[start:start + size]
            while remaining:
                count = self.process.stdin.write(remaining)
                if not count:
                    raise RuntimeError('Bridge stdin stopped accepting bytes')
                remaining = remaining[count:]
        line = self.lines.get(timeout=TIMEOUT)
        if not isinstance(line, bytes) or not line:
            self.errors.seek(0)
            detail = self.errors.read().decode(errors='replace')
            raise AssertionError(f'Bridge exited or failed: {line!r}: {detail}')
        response = json.loads(line)
        assert response['id'] == request['id'], response
        assert 'error' not in response, response
        return response['result'], len(payload), hashlib.sha256(payload).hexdigest()

    def call(self, name, args=None, error=False, fragmented=False):
        result, size, digest = self.rpc('tools/call', {'name': name, 'arguments': args or {}}, fragmented)
        assert result.get('isError', False) == error, result
        if error:
            return result
        return json.loads(result['content'][0]['text'])

    def initialize(self):
        result, _, _ = self.rpc('initialize', {})
        assert result['serverInfo']['name']

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            raise AssertionError('Bridge did not exit after input EOF')
        self.process.stdout.close()
        self.errors.close()


class Host:
    def __init__(self, root, index, env):
        self.directory = root / f'h{index}'
        self.directory.mkdir(mode=0o700)
        self.socket = self.directory / 'socket'
        self.descriptor = self.directory / 'descriptor.json'
        self.env = dict(env, LEMMALOG_DDLOG_WORKDIR=str(self.directory / 'build'))
        self.log = open(self.directory / 'host.log', 'wb')
        self.process = subprocess.Popen(
            [BINARY, 'host', '--socket', str(self.socket), '--descriptor', str(self.descriptor)],
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log,
            env=self.env, start_new_session=True)
        hosts.append(self)
        deadline = time.monotonic() + 10
        while not self.descriptor.exists():
            assert self.process.poll() is None, 'Host exited before readiness'
            assert time.monotonic() < deadline, 'Host readiness deadline exceeded'
            time.sleep(0.025)
        self.identity = json.loads(self.descriptor.read_text())['instance_id']
        assert self.socket.stat().st_mode & 0o777 == 0o600
        assert self.directory.stat().st_mode & 0o777 == 0o700

    def client(self):
        client = Client(self.descriptor, self.env)
        client.initialize()
        info = client.call('instance_info')
        assert info['instance_id'] == self.identity
        return client

    def stop(self):
        if self.process.poll() is None:
            stop = subprocess.run([BINARY, 'stop', '--descriptor', str(self.descriptor)],
                                  env=self.env, capture_output=True, timeout=20)
            assert stop.returncode == 0, stop.stderr.decode()
            self.process.wait(timeout=10)
        assert self.process.returncode == 0, 'Host shutdown was not clean'
        assert not self.socket.exists(), 'Host socket survived stop'
        self.log.close()

