import http.client
import copy
import json
import os
import shutil
import sys
import threading
import time
import unittest
import uuid
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'webapp'))
import server
from analysis_jobs import AnalysisJobs, JobLimitError


def wait_for_job(store, job_id):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        job = store.get(job_id)
        if job['status'] != 'running':
            return job
        threading.Event().wait(.01)
    raise AssertionError('job did not finish')


class AnalysisJobTests(unittest.TestCase):
    def test_capacity_deduplication_progress_and_results(self):
        store = AnalysisJobs(max_active=1)
        release = threading.Event()

        def work(respond, progress):
            progress(1, 3)
            if not release.wait(5):
                raise ValueError('test was not released')
            return respond({'results': [1, 2, 3]})

        job = store.submit(work, 'same', session='s1')
        try:
            duplicate = store.submit(work, 'same', session='s1')
            self.assertEqual(job['job_id'], duplicate['job_id'])
            self.assertTrue(store.active_for_session('s1'))
            with self.assertRaises(JobLimitError):
                store.submit(work, 'different')
        finally:
            release.set()
        done = wait_for_job(store, job['job_id'])
        self.assertEqual(done['result'], {'results': [1, 2, 3]})
        self.assertEqual((done['completed'], done['total']), (1, 3))
        self.assertFalse(store.active_for_session('s1'))
        self.assertNotIn('key', done)

    def test_validation_error_is_reported_as_failed_job(self):
        store = AnalysisJobs()
        job = store.submit(lambda respond, progress: respond({'error': 'missing log'}, 400), 'bad')
        done = wait_for_job(store, job['job_id'])
        self.assertEqual(done['status'], 'failed')
        self.assertEqual(done['error'], 'missing log')

    def test_results_expire_and_retention_is_bounded(self):
        store = AnalysisJobs(max_retained=1)
        first = store.submit(lambda respond, progress: respond({'ok': True}), 'one')
        wait_for_job(store, first['job_id'])
        second = store.submit(lambda respond, progress: respond({'ok': True}), 'two')
        wait_for_job(store, second['job_id'])
        self.assertIsNone(store.get(first['job_id']))
        with store.lock:
            store.jobs[second['job_id']]['updated'] = time.time() - 3601
        self.assertIsNone(store.get(second['job_id']))


class AnalysisHttpTests(unittest.TestCase):
    def setUp(self):
        class QuietHandler(server.Handler):
            def log_message(self, *args):
                pass
        self.httpd = server.ThreadingHTTPServer(('127.0.0.1', 0), QuietHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.store = AnalysisJobs()
        self.patcher = patch.object(server, 'ANALYSIS_JOBS', self.store)
        self.patcher.start()
        self.sessions = []

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(5)
        self.patcher.stop()
        for sid in self.sessions:
            session = server.SESSIONS.pop(sid, None)
            if session:
                shutil.rmtree(session['root'], ignore_errors=True)

    def request(self, method, path, body=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.httpd.server_port, timeout=2)
        try:
            conn.request(method, path, json.dumps(body) if body is not None else None,
                         {'Content-Type': 'application/json'})
            response = conn.getresponse()
            return response.status, json.loads(response.read())
        finally:
            conn.close()

    def test_slow_analysis_survives_initiating_connection_and_reset_is_blocked(self):
        release = threading.Event()

        def slow(handler, request=None, respond=None, progress=None):
            progress(2, 10)
            if not release.wait(5):
                raise ValueError('test was not released')
            return respond({'session': request['session'], 'results': ['finished']})

        with patch.object(server.Handler, 'handle_analyze', slow):
            try:
                status, job = self.request('POST', '/api/analysis/jobs', {
                    'endpoint': '/api/analyze', 'request': {'session': 'test-session'}})
                self.assertEqual(status, 202)  # Work is still blocked after this connection closes.
                self.assertFalse(release.is_set())
                self.assertEqual(self.request('GET', '/api/health')[0], 200)
                status, pending = self.request('GET', job['poll'])
                self.assertEqual(status, 200)
                self.assertEqual(pending['status'], 'running')
                self.assertEqual(self.request('POST', '/api/reset', {'session': 'test-session'})[0], 409)
            finally:
                release.set()
            wait_for_job(self.store, job['job_id'])
            status, done = self.request('GET', job['poll'])
            self.assertEqual(status, 200)
            self.assertEqual(done['result']['results'], ['finished'])

    def test_all_analysis_routes_preserve_validation_errors(self):
        for endpoint in ['/api/analyze', '/api/errors', '/api/ingest/analyze', '/api/ingest/errors']:
            with self.subTest(endpoint=endpoint):
                status, job = self.request('POST', '/api/analysis/jobs', {'endpoint': endpoint, 'request': {}})
                self.assertEqual(status, 202)
                done = wait_for_job(self.store, job['job_id'])
                self.assertEqual(done['status'], 'failed')
                self.assertIn('error', done)

    def test_unknown_job_and_invalid_operation(self):
        self.assertEqual(self.request('GET', '/api/analysis/jobs/missing')[0], 404)
        self.assertEqual(self.request('POST', '/api/analysis/jobs', {'endpoint': '/api/reset', 'request': {}})[0], 400)

    def test_legacy_latency_validation_remains_json(self):
        status, body = self.request('POST', '/api/analyze', {})
        self.assertEqual(status, 400)
        self.assertIn('upload a log', body['error'])

    def test_real_uploaded_latency_and_errors_return_complete_results(self):
        sid = uuid.uuid4().hex
        self.sessions.append(sid)
        session = server.get_session(sid, create=True)
        for node in range(2):
            path = os.path.join(session['root'], '10.0.0.%d-aerospike.log' % (node + 1))
            with open(path, 'w') as out:
                for i in range(3):
                    prefix = 'Nov 23 2026 00:00:%02d GMT: INFO (info): (hist.c:1)' % (i * 10)
                    for operation in ['read', 'write']:
                        out.write('%s histogram dump: {test}-%s (%d total) msec\n' % (prefix, operation, (i + 1) * 100))
                        out.write('%s (00: %010d)\n' % (prefix, (i + 1) * 100))
                out.write('Nov 23 2026 00:00:21 GMT: WARNING (test): (test.c:1) example warning\n')
            server.ingest_upload_target(session, path)
        server.upload_result_payload(session, sid, time.time(), [], [])
        for endpoint in ['/api/analyze', '/api/errors']:
            status, job = self.request('POST', '/api/analysis/jobs', {
                'endpoint': endpoint, 'request': {'session': sid, 'alert_threshold': 6}})
            self.assertEqual(status, 202)
            done = wait_for_job(self.store, job['job_id'])
            self.assertEqual(done['status'], 'completed', done.get('error'))
            result = done['result']
            self.assertEqual(result['session'], sid)
            if endpoint == '/api/analyze':
                self.assertEqual(result['totals']['runs'], 4)
                self.assertEqual(result['totals']['with_data'], 4)
                self.assertEqual((done['completed'], done['total']), (4, 4))
                self.assertEqual(len(session['raw']), 4)
                self.assertEqual(result['options']['alert_threshold'], 6)
                self.assertTrue(all(r['summary']['primary_threshold'] == 4 for r in result['results']))
                self.assertIn('over 4 ms', result['cumulative']['headline'][0])
                self.assertNotIn('over 6 ms', result['cumulative']['headline'][0])
                # A small slow node must remain visible after weighting against
                # a busy node; rounding to three decimals used to erase it.
                entries = copy.deepcopy(result['results'])
                reads = [r for r in entries if r['op'] == 'read']
                reads[0]['series']['ops'][0] = 1
                reads[1]['series']['ops'][0] = 1000000
                primary = reads[0]['summary']['primary_index']
                reads[0]['series']['overs'][primary][0] = .01
                opts = dict(result['options'], from_dt=None, to_dt=None)
                aggregate, _ = server.build_rollups(entries, opts)
                self.assertAlmostEqual(aggregate['pct']['read'][0], .01 / 1000001, places=15)
            else:
                self.assertEqual(result['severity_totals']['WARNING'], 2)
            if os.environ.get('AERO_TEST_EXPORT'):
                os.makedirs(os.environ['AERO_TEST_EXPORT'], exist_ok=True)
                with open(os.path.join(os.environ['AERO_TEST_EXPORT'], endpoint.rsplit('/', 1)[1] + '.json'), 'w') as out:
                    json.dump(result, out)


if __name__ == '__main__':
    unittest.main()
