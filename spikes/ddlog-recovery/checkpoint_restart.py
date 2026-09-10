#!/usr/bin/env python3
"""Bounded controlled checkpoint/restart of one pure real DDlog graph program."""
from pathlib import Path, PurePosixPath
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
if sys.flags.optimize:
    raise SystemExit("Run without -O: this acceptance oracle requires assertions")
if len(sys.argv) != 3:
    raise SystemExit("usage: checkpoint_restart.py ARTIFACT.json FRESH_RUN_DIRECTORY")
config_path = Path(sys.argv[1]).resolve(strict=True)
CONFIG = json.loads(config_path.read_text())
PERSIST = Path(os.environ.get('DDLOG_RECOVERY_PERSIST', str(HERE / 'target/debug/ddlog-recovery-persistence-spike'))).resolve(strict=True)
os.environ['DDLOG_RECOVERY_ARTIFACT'] = str(config_path)
os.environ['LEMMALOG_DDLOG_MCP'] = CONFIG['host_binary']
os.environ['SHARED_TEST_TIMEOUT'] = '30'
import shared_host as shared
from connected_components_oracle import apply, expected, rows


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def json_write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def facts_json(facts):
    return {name: [list(row) for row in sorted(values)] for name, values in sorted(facts.items())}


class Ledger:
    """Only mutation admission path for this fixture; updated after native ack."""
    def __init__(self, client, initial=None, completed=0):
        self.client = client
        self.facts = ({name: {tuple(row) for row in values} for name, values in initial.items()}
                      if initial else {'vertices': set(), 'edges': set()})
        self.completed = completed
        self.receipts = {}

    def transact(self, sequence, changes):
        digest = sha(canonical(changes))
        if sequence in self.receipts:
            assert self.receipts[sequence]['digest'] == digest, 'logical transaction identity conflict'
            return dict(self.receipts[sequence], fixture_deduplicated=True)
        assert sequence == self.completed + 1, 'noncontiguous fixture transaction'
        staged = apply(self.facts, changes)  # validates all relation names/types before admission
        result = self.client.call('apply_changes', {'changes': changes})
        self.facts, self.completed = staged, sequence
        receipt = {'sequence': sequence, 'digest': digest, 'native_ack': result,
                   'durability': 'memory-only until explicit checkpoint acknowledgment'}
        self.receipts[sequence] = receipt
        return receipt


def register(client):
    definition = {'rules': 'labels(V,C) :- components(V,C).', 'schemas': {
        'vertices': {'input': True, 'fields': ['int']},
        'edges': {'input': True, 'fields': ['int', 'int']},
        'components': {'input': False, 'fields': ['int', 'int']},
        'labels': {'input': False, 'fields': ['int', 'int']}},
        'operators': [{'type': 'large_small_star', 'vertices': 'vertices',
                       'edges': 'edges', 'output': 'components'}],
        'interface': {'inputs': ['vertices', 'edges'], 'outputs': ['labels']}}
    declared = {name: item['fields'] for name, item in definition['schemas'].items() if item['input']}
    assert declared == {'vertices':['int'], 'edges':['int','int']}
    assert set(definition['interface']['inputs']) == set(declared)
    leaf = client.call('processor_create', {'definition': definition})
    leaf_pin = {key: leaf[key] for key in ('processor_id', 'version')}
    wrapper = {'composition': {
        'nodes': {'graph': leaf_pin}, 'bindings': [],
        'inputs': {name: {'fields': fields, 'targets': [{'node': 'graph', 'relation': name}]}
                   for name, fields in declared.items()},
        'outputs': {'labels': {'node':'graph', 'relation':'labels'}}}}
    record = client.call('processor_create', {'definition': wrapper})
    pin = {key: record[key] for key in ('processor_id','version')}
    return pin, leaf, record


def archive_registry(registry):
    entries = []
    for path in sorted(registry.rglob('*.json')):
        data = path.read_bytes()
        entries.append({'path':str(path.relative_to(registry)), 'sha256':sha(data),
                        'contents':data.decode()})
    assert len(entries) == 4, 'expected exactly leaf and wrapper version/current records'
    return entries


def restore_registry(registry, entries):
    # Fixture file adapter; public runtime installation still validates version hashes.
    registry.mkdir(mode=0o700)
    for entry in entries:
        relative = PurePosixPath(entry['path'])
        assert not relative.is_absolute() and '..' not in relative.parts
        data = entry['contents'].encode()
        assert sha(data) == entry['sha256']
        destination = registry.joinpath(*relative.parts)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with destination.open('xb') as output:
            output.write(data)
    assert archive_registry(registry) == entries


def persistence(*args):
    result = subprocess.run([str(PERSIST), *map(str,args)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def observe(client, label, facts, observations):
    actual = rows(client.call('lemmalog_query', {'predicate':'labels'})['rows'])
    oracle = expected(facts)
    assert actual == oracle, {'phase':label,'actual':sorted(actual),'oracle':sorted(oracle)}
    observations.append({'phase':label,'actual':sorted(actual),'independent_bfs':sorted(oracle)})
    return actual


def main():
    started = time.monotonic()
    run = Path(sys.argv[2]).resolve()
    assert not run.exists(), 'fresh run required'
    assert shutil.disk_usage(HERE).free > 900 * 1024**2, 'disk below guard floor'
    run.mkdir(parents=True)
    assert sha(Path(CONFIG['host_binary']).read_bytes()) == CONFIG['host_sha256']
    host_root = Path(tempfile.mkdtemp(prefix='ddr-', dir='/tmp'))
    source_registry = host_root / 'registry'
    env = dict(os.environ, LEMMALOG_DDLOG_BUILD=str(HERE/'reuse_native.py'),
               LEMMALOG_PROCESSOR_REGISTRY=str(source_registry))
    env.pop('LEMMALOG_AGENT_OPERATIONS', None)
    evidence = {'passed':False, 'scope':'controlled checkpoint restart', 'artifact':CONFIG,
                'host_root':str(host_root), 'native_builds':0, 'provider_calls':0,
                'observations':[], 'cleanup_errors':[]}
    try:
        original = shared.Host(host_root, 1, env)
        source = original.client()
        pin, leaf, program = register(source)
        installed = source.call('processor_install', pin)
        assert installed['replayed_facts'] == 0
        assert installed['composition']['generated_source_sha256'] == CONFIG['generated_files']['program.dl']
        assert installed['composition']['inputs'] == {'edges':'Input_edges','vertices':'Input_vertices'}
        generated_sources = list((original.directory/'build').rglob('program.dl'))
        assert len(generated_sources) == 1
        native_inputs = re.findall(r'^input relation (\w+)\(',generated_sources[0].read_text(),re.MULTILINE)
        assert sorted(native_inputs) == ['R_Input_edges','R_Input_vertices'], 'uncheckpointed native input relation'
        exports = {name:source.call('lemmalog_query', {'predicate':name}, error=True)
                   for name in ['edges','vertices']}
        assert all(item['isError'] for item in exports.values())
        reference_host = shared.Host(host_root, 2, env)
        reference = reference_host.client()
        reference.call('processor_install', pin)
        live, uninterrupted = Ledger(source), Ledger(reference)
        changes1 = [{'op':'insert','predicate':'vertices','values':[v]} for v in [-9,-4,0,3,7,42]]
        changes1 += [{'op':'insert','predicate':'edges','values':edge} for edge in [[-9,-4],[-4,0],[3,7]]]
        changes2 = [{'op':'insert','predicate':'vertices','values':[99]},
                    {'op':'insert','predicate':'edges','values':[-9,-4]}]  # set duplicate
        for sequence, changes in [(1,changes1),(2,changes2)]:
            live.transact(sequence,changes)
            uninterrupted.transact(sequence,changes)
        duplicate = live.transact(2,changes2)
        assert duplicate['fixture_deduplicated'] and live.completed == 2
        initial = observe(source,'source_at_completed_2',live.facts,evidence['observations'])
        assert observe(reference,'uninterrupted_at_2',uninterrupted.facts,evidence['observations']) == initial
        manifest = {'kind':'controlled-checkpoint-v1','checkpoint_id':'cc-checkpoint-2',
            'logical_run_id':'cc-recovery-logical-run-1','completed_logical_boundary':2,
            'input_capture':'fixture-owned ledger; sole mutation admission; updated only after native transaction acknowledgment',
            'representation':'complete input sets; no tail; no outputs are restoration inputs',
            'program_pin':pin,'program_record':program,'leaf_record':leaf,
            'declared_input_schemas':{'edges':['int','int'],'vertices':['int']},
            'compiled_input_relations':native_inputs,
            'registry_files':archive_registry(source_registry),
            'artifact':CONFIG,'source_transaction_receipts':list(live.receipts.values())}
        request = run/'checkpoint-request.json'
        json_write(request, {'manifest':manifest,'facts':facts_json(live.facts)})
        committed = persistence('publish',run/'checkpoint',request)
        assert committed['facts'] == facts_json(live.facts)
        assert committed['manifest']['completed_logical_boundary'] == 2
        evidence['durable_acknowledgment'] = {'catalog_snapshot':committed['catalog_snapshot'],
            'manifest_sha256':committed['manifest_sha256'], 'completed_logical_boundary':2,
            'input_counts':{name:len(value) for name,value in committed['facts'].items()}}
        json_write(run/'checkpoint-acknowledgment.json',evidence['durable_acknowledgment'])
        # This completed native transaction is deliberately beyond the durable checkpoint.
        live.transact(3,[{'op':'insert','predicate':'vertices','values':[999]},
                         {'op':'insert','predicate':'edges','values':[0,3]}])
        volatile = observe(source,'source_memory_only_3',live.facts,evidence['observations'])
        assert volatile != initial and (999,999) in volatile
        evidence['unpersisted_boundary'] = 3
        original_id, original_pid = original.identity, original.process.pid
        source.close(); original.stop()
        assert original.process.returncode == 0 and not original.descriptor.exists()
        # Restoration reads committed catalog data anew after the original host has stopped.
        recovered = persistence('read',run/'checkpoint')
        restore_manifest = recovered['manifest']
        assert restore_manifest['artifact'] == CONFIG
        assert restore_manifest['completed_logical_boundary'] == 2
        assert restore_manifest['declared_input_schemas'] == {'edges':['int','int'],'vertices':['int']}
        assert set(recovered['facts']) == set(restore_manifest['declared_input_schemas'])
        assert sha(canonical(recovered['facts'])) == restore_manifest['input_facts_sha256']
        fresh_registry = host_root/'restored-registry'
        restore_registry(fresh_registry,restore_manifest['registry_files'])
        fresh_env = dict(env,LEMMALOG_PROCESSOR_REGISTRY=str(fresh_registry))
        fresh_host = shared.Host(host_root,3,fresh_env)
        restored = fresh_host.client()
        fresh_install = restored.call('processor_install',restore_manifest['program_pin'])
        assert fresh_install['processor'] == pin and fresh_install['replayed_facts'] == 0
        assert restored.call('processor_get',restore_manifest['program_pin']) == restore_manifest['program_record']
        assert fresh_install['composition']['generated_source_sha256'] == CONFIG['generated_files']['program.dl']
        assert fresh_host.identity != original_id and fresh_host.process.pid != original_pid
        assert rows(restored.call('lemmalog_query',{'predicate':'labels'})['rows']) == set()
        all_inputs = [{'op':'insert','predicate':name,'values':values}
                      for name, valueset in recovered['facts'].items() for values in valueset]
        restored.call('apply_changes',{'changes':all_inputs})
        recovered_live = Ledger(restored,recovered['facts'],restore_manifest['completed_logical_boundary'])
        restored_rows = observe(restored,'fresh_restored_at_2',recovered_live.facts,evidence['observations'])
        assert restored_rows == initial and (999,999) not in restored_rows
        for sequence, operation in [(3,'insert'),(4,'delete')]:
            changes = [{'op':operation,'predicate':'edges','values':[0,3]}]
            recovered_live.transact(sequence,changes)
            uninterrupted.transact(sequence,changes)
            actual = observe(restored,f'restored_bridge_{operation}',recovered_live.facts,evidence['observations'])
            reference_rows = observe(reference,f'uninterrupted_bridge_{operation}',uninterrupted.facts,evidence['observations'])
            assert actual == reference_rows
        assert actual == initial
        evidence.update({'passed':True,'program_pin':pin,'restored_program_pin':fresh_install['processor'],
            'logical_run_id':restore_manifest['logical_run_id'],'restored_completed_boundary':2,
            'original_host':{'instance_id':original_id,'pid':original_pid,'clean_exit':original.process.returncode},
            'fresh_host':{'instance_id':fresh_host.identity,'pid':fresh_host.process.pid,'initial_replayed_facts':0},
            'checkpoint':recovered,'input_export_rejections':exports,
            'fixture_duplicate_transaction':duplicate,'native_activations':3,
            'checks':['all_declared_inputs_checkpointed','exact_program_pin_restored_from_checkpointed_registry',
                      'generated_native_input_relations_exactly_match_checkpoint_inventory',
                      'fresh_instance_initially_empty','restored_outputs_equal_uninterrupted_and_independent_bfs',
                      'memory_only_boundary_3_not_restored','bridge_insert_delete_equal_reference_and_bfs'],
            'limitations':['controlled clean shutdown only','fixture input ledger, no runtime export API',
                'full checkpoint only, no WAL or checkpoint tail','local process durability only',
                'verified artifact reuse, no fresh native compilation','file/JSON adapter, not Arrow-native FFI',
                'no inference or external effects','no general exactly-once guarantee',
                'pinned Parquet58/Thrift vulnerability inherited from isolated Iceberg fixture']})
    finally:
        for client in shared.clients:
            if not client.errors.closed:
                try: client.close()
                except Exception as error: evidence['cleanup_errors'].append(str(error))
        for host in shared.hosts:
            if not host.log.closed:
                try: host.stop()
                except Exception as error: evidence['cleanup_errors'].append(str(error))
        evidence['passed'] = evidence['passed'] and not evidence['cleanup_errors']
        evidence['seconds'] = round(time.monotonic()-started,3)
        evidence['free_mib_after'] = shutil.disk_usage(HERE).free//1024**2
        json_write(run/'evidence.json',evidence)
    assert evidence['passed'], evidence
    print(json.dumps({'passed':True,'seconds':evidence['seconds'],'checks':evidence['checks'],
                      'evidence':str(run/'evidence.json'),'native_builds':0,'provider_calls':0}))


if __name__ == '__main__':
    main()
