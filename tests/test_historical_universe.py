"""No current constituent snapshot may silently become a historical universe."""
import json
from types import SimpleNamespace

import pytest

from stopat30m.data.historical_universe import HistoricalUniverse, UniverseError, download_index_snapshot
from stopat30m.data.fetcher.qlib_dumper import QlibDumper, write_current_index_snapshots


def index_payload():
    def interval(code, start, end, known):
        payload = {'instrument': code, 'effective_from': start, 'effective_to': end,
                'known_at': known, 'source': 'synthetic_official_changes',
                'source_url': 'https://example.test/dated-index-notice'}
        if end:
            payload.update(exit_known_at='2020-06-01', exit_source_url='https://example.test/dated-exit-notice')
        return payload
    return {'name': 'test_index', 'kind': 'index_pit', 'coverage_start': '2020-01-01',
            'coverage_end': '2020-12-31', 'expected_size': 1,
            'coverage_evidence': [{'start': '2020-01-01', 'end': '2020-12-31',
                                   'method': 'official_change_log', 'source_url': 'https://example.test/complete-history'}],
            'intervals': [interval('SH600000', '2020-01-01', '2020-07-01', '2019-12-01'),
                          interval('SZ000001', '2020-07-01', None, '2020-06-01')]}


def test_effective_intervals_and_known_information_are_separate():
    universe = HistoricalUniverse.from_dict(index_payload())
    assert universe.members_on('2020-06-30') == ['SH600000']
    assert universe.members_on('2020-07-01', known_on='2020-06-30') == ['SZ000001']
    with pytest.raises(UniverseError, match='not known'):
        universe.members_on('2020-07-01', known_on='2020-05-30')
    assert universe.audit('2020-01-01', '2020-12-31', require_pit=True)['point_in_time']
    with pytest.raises(UniverseError, match='coverage'):
        universe.members_on('2019-01-01')


def test_day_only_announcement_not_assumed_known_before_same_day_close():
    payload = index_payload()
    payload['intervals'][1]['known_at'] = '2020-06-30'
    universe = HistoricalUniverse.from_dict(payload)
    assert universe.members_on('2020-07-01', known_on='2020-06-30') == []
    assert universe.members_on('2020-07-01', known_on='2020-07-01') == ['SZ000001']


def test_fixed_pool_keeps_real_selection_date_and_discloses_retrospection(tmp_path):
    universe = HistoricalUniverse.fixed_pool('fixed_etfs', ['510300', '510500', '159915'],
                    '2014-01-01', '2025-12-31', selection_date='2026-10-19')
    assert universe.members_on('2015-01-05', known_on='2015-01-02') == ['SH510300', 'SH510500', 'SZ159915']
    audit = universe.audit('2015-01-01', '2025-12-31')
    assert audit['retrospective_selection'] and not audit['point_in_time']
    assert audit['limitations']
    with pytest.raises(UniverseError, match='Point-in-time'):
        universe.audit('2015-01-01', '2025-12-31', require_pit=True)
    path = universe.save(tmp_path / 'pool.json')
    assert HistoricalUniverse.load(path).content_hash == universe.content_hash
    raw = json.loads(path.read_text())
    raw['selection_date'] = '2014-01-01'
    path.write_text(json.dumps(raw))
    with pytest.raises(UniverseError, match='checksum'): HistoricalUniverse.load(path)


def test_exit_fields_do_not_change_existing_fixed_protocol_hash():
    universe = HistoricalUniverse.fixed_pool('compatibility_fixture', ['SH510300', 'SZ159915'],
                    '2014-01-01', '2025-12-31', selection_date='2026-10-19')
    assert universe.content_hash == '8dbc33326f7889d65cde764af770bab710f60caf0c9f3e9cb186fc438292fb89'
    assert all('exit_known_at' not in row and 'exit_source_url' not in row for row in universe.as_dict()['intervals'])


def test_pit_history_requires_separate_exit_notice_evidence():
    for missing in ('exit_known_at', 'exit_source_url'):
        payload = index_payload()
        payload['intervals'][0].pop(missing)
        universe = HistoricalUniverse.from_dict(payload)
        with pytest.raises(UniverseError, match='Point-in-time'):
            universe.audit('2020-01-01', '2020-12-31', require_pit=True)


def test_evening_entry_and_exit_cannot_change_prior_close_known_pool():
    payload = index_payload()
    payload['intervals'][0].update(exit_known_at='2020-06-30T20:00:00+08:00')
    payload['intervals'][1].update(known_at='2020-06-30T20:00:00+08:00')
    payload['intervals'].append({
        'instrument': 'SH600002', 'effective_from': '2020-01-01', 'effective_to': None,
        'known_at': '2019-12-01', 'source': 'synthetic_official_changes',
        'source_url': 'https://example.test/stable-member',
    })
    payload['expected_size'] = 2
    universe = HistoricalUniverse.from_dict(payload)
    # The source can describe exact historical intervals while the strategy
    # deliberately limits knowledge to the previous session's 15:00 close.
    assert universe.audit('2020-01-01', '2020-12-31', require_pit=True)['point_in_time']
    with pytest.raises(UniverseError, match='future exit information'):
        universe.members_on('2020-07-01', known_on='2020-06-30T15:00:00+08:00')
    assert universe.members_on('2020-07-01', known_on='2020-06-30T21:00:00+08:00') == ['SH600002', 'SZ000001']


@pytest.mark.parametrize('problem', ['no_coverage', 'gap', 'overlap', 'late_known', 'snapshot_relabel'])
def test_incomplete_or_relabelled_history_cannot_pass_pit(problem):
    payload = index_payload()
    if problem == 'no_coverage': payload['coverage_evidence'] = []
    if problem == 'gap': payload['intervals'][0]['effective_to'] = '2020-06-01'
    if problem == 'overlap': payload['intervals'][1]['instrument'] = 'SH600000'; payload['intervals'][1]['effective_from'] = '2020-06-15'
    if problem == 'late_known': payload['intervals'][1]['known_at'] = '2020-07-15'
    if problem == 'snapshot_relabel': payload['intervals'][0]['source'] = 'baostock.query_hs300_stocks'
    with pytest.raises(UniverseError):
        universe = HistoricalUniverse.from_dict(payload)
        universe.audit('2020-01-01', '2020-12-31', require_pit=True)


def test_only_audited_intervals_export_to_qlib(tmp_path):
    universe = HistoricalUniverse.from_dict(index_payload())
    path = universe.write_qlib(tmp_path / 'test_index.txt', '2020-01-01', '2020-12-31')
    assert path.read_text().splitlines() == ['SH600000\t2020-01-01\t2020-06-30',
                                           'SZ000001\t2020-07-01\t2020-12-31']


class Result:
    error_code = '0'
    fields = ['updateDate', 'code', 'code_name']
    def __init__(self, day): self.rows = iter([[day, 'sh.600000', 'A']])
    def next(self):
        self.row = next(self.rows, None)
        return self.row is not None
    def get_row_data(self): return self.row


def test_free_snapshot_date_leaks_rejected_and_weekly_dates_not_extended(tmp_path):
    client = SimpleNamespace(login=lambda: SimpleNamespace(error_code='0'), logout=lambda: None,
                             query_hs300_stocks=lambda **kwargs: Result('2026-10-19'))
    with pytest.raises(UniverseError, match='does not establish'):
        download_index_snapshot('csi300', '2015-01-01', tmp_path / 'bad.json', client=client)
    client.query_hs300_stocks = lambda **kwargs: Result('2020-06-01')
    universe = download_index_snapshot('csi300', '2020-06-03', tmp_path / 'snapshot.json', client=client)
    assert universe.kind == 'snapshot'
    assert universe.metadata['provider_update_date'] == '2020-06-01'
    assert universe.audit('2020-06-03', '2020-06-03')['status'] == 'blocked'
    with pytest.raises(UniverseError): universe.members_on('2020-06-04')
    with pytest.raises(UniverseError): universe.audit('2020-06-03', '2020-06-03', require_pit=True)


def test_old_qlib_writers_preserve_history_and_only_write_reference_snapshot(tmp_path):
    instruments = tmp_path / 'instruments'
    instruments.mkdir()
    old = instruments / 'csi300.txt'
    original = 'SH600001\t2005-01-01\t2006-01-01\n'
    old.write_text(original)
    dumper = QlibDumper(tmp_path)
    dumper._instruments = {'SH600000': ('2005-01-01', '2026-01-01')}
    dumper.write_instruments(['600000'], ['000001'])
    assert old.read_text() == original
    assert not (instruments / 'csi500.txt').exists()
    snapshot = HistoricalUniverse.load(instruments / 'csi300.current_snapshot.json')
    assert snapshot.kind == 'snapshot'
    assert snapshot.metadata['legacy_historical_file_present']
    assert snapshot.metadata['historical_membership_verified'] is False
    write_current_index_snapshots(tmp_path, ['600000'], source='incremental_current_lookup')
    assert old.read_text() == original
