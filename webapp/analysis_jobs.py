"""Bounded in-memory analysis jobs, independent of browser connections."""

import threading
import time
import uuid


class JobLimitError(Exception):
    pass


class AnalysisJobs:
    def __init__(self, max_active=2, max_retained=16, retention_seconds=3600):
        self.lock = threading.Lock()
        self.jobs = {}
        self.max_active = max_active
        self.max_retained = max_retained
        self.retention_seconds = retention_seconds

    def submit(self, work, key, session=None):
        with self.lock:
            now = time.time()
            finished = sorted((j for j in self.jobs.values() if j['status'] in ('completed', 'failed')),
                              key=lambda j: j['updated'])
            for job in finished:
                if now - job['updated'] > self.retention_seconds or len(self.jobs) >= self.max_retained:
                    self.jobs.pop(job['job_id'], None)
            active = [j for j in self.jobs.values() if j['status'] == 'running']
            for job in active:
                if job['key'] == key:
                    return self._public(job)
            if len(active) >= self.max_active:
                raise JobLimitError('Analysis capacity is busy. Wait for a running analysis to finish.')
            job = {'job_id': uuid.uuid4().hex, 'status': 'running', 'created': now,
                   'updated': now, 'key': key, 'session': session, 'completed': 0, 'total': 0}
            self.jobs[job['job_id']] = job
            snapshot = self._public(job)
        threading.Thread(target=self._run, args=(job['job_id'], work), daemon=True).start()
        return snapshot

    def _public(self, job):
        return {k: v for k, v in job.items() if k not in ('key', 'session')}

    def get(self, job_id):
        with self.lock:
            job = self.jobs.get(job_id)
            if job and job['status'] != 'running' and time.time() - job['updated'] > self.retention_seconds:
                self.jobs.pop(job_id, None)
                return None
            return self._public(job) if job else None

    def active_for_session(self, session):
        with self.lock:
            return any(j['session'] == session and j['status'] == 'running'
                       for j in self.jobs.values())

    def _run(self, job_id, work):
        def progress(completed, total):
            with self.lock:
                self.jobs[job_id].update(completed=completed, total=total, updated=time.time())

        def respond(payload, status=200):
            if status >= 400:
                raise ValueError(payload.get('error') or 'Analysis failed')
            return payload

        try:
            result = work(respond, progress)
            if result is None:
                raise ValueError('Analysis returned no result')
            fields = {'status': 'completed', 'result': result}
        except Exception as exc:
            fields = {'status': 'failed', 'error': str(exc)}
        with self.lock:
            self.jobs[job_id].update(updated=time.time(), **fields)
