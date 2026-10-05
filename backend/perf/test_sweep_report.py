"""sweep_report.py 프로브 표 + tps-sweep.sh 시뮬레이터 모드 헬퍼 검증 (Docker/서버 불필요).

    python3 backend/perf/test_sweep_report.py
"""
import os
import re
import subprocess
import tempfile
import textwrap
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SUMMARY = ('rate,effective_send,consume_realtime,consume_storage,peak_lag_rt,end_lag_rt,drain_s,verdict\n'
           '2000,1990,1985,1980,100,0,5,KEPT_UP\n'
           '4000,3900,NA,NA,9000,8000,timeout,SATURATED\n')


def report(out_dir):
    return subprocess.run(['python3', os.path.join(HERE, 'sweep_report.py'), out_dir],
                          capture_output=True, text=True, check=True).stdout


def make_out(d, probe=None):
    with open(os.path.join(d, 'summary.csv'), 'w') as f:
        f.write(SUMMARY)
    if probe is not None:
        with open(os.path.join(d, 'analysis_probe.csv'), 'w') as f:
            f.write(probe)


def sh(script, **env):
    e = dict(os.environ, **env)
    return subprocess.run(['bash', '-c', script], capture_output=True, text=True, env=e)


def sim_funcs():
    with open(os.path.join(HERE, 'tps-sweep.sh'), encoding='utf-8') as f:
        src = f.read()
    return re.search(r'^sim_scale\(\).*?^run_loadgen\(\) \{.*?^\}\n', src, re.S | re.M).group(0)


class ProbeTable(unittest.TestCase):
    def test_no_probe_file_output_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            make_out(d)
            self.assertNotIn('분석 API', report(d))

    def test_probe_table(self):
        probe = textwrap.dedent('''\
            timestamp,rate,symbol,http,seconds
            1,2000,AAPL,200,0.100
            2,2000,NVDA,200,0.300
            3,2000,MSFT,200,0.200
            4,2000,TSLA,200,0.400
            5,2000,SPY,500,0.050
            6,4000,AAPL,000,0
            7,4000,NVDA,,
            8,4000,MSFT,200,
            9,4000,TSLA,200,1.5
            10,6000,AAPL,200,0.7
            ''')
        with tempfile.TemporaryDirectory() as d:
            make_out(d, probe)
            out = report(d)
        self.assertIn('| 2000 | 5 | 4 | 1 | 0.100 | 0.250 | 0.400 |', out)   # 짝수 개 중앙값, 500 은 실패·시간 제외
        self.assertIn('| 4000 | 4 | 2 | 2 | 1.500 | 1.500 | 1.500 |', out)   # 000/빈 값 실패, 200+빈 시간은 통계 제외
        self.assertIn('| 6000 | 1 | 1 | 0 | 0.700 | 0.700 | 0.700 |', out)   # summary 에 없는 rate 도 표시
        self.assertLess(out.index('분석 API'), out.index('## 결론'))

    def test_all_failed_rate_is_na(self):
        with tempfile.TemporaryDirectory() as d:
            make_out(d, 'timestamp,rate,symbol,http,seconds\n1,2000,AAPL,000,0\n')
            self.assertIn('| 2000 | 1 | 0 | 1 | NA | NA | NA |', report(d))

    def test_header_only_probe_file(self):
        with tempfile.TemporaryDirectory() as d:
            make_out(d, 'timestamp,rate,symbol,http,seconds\n')
            self.assertIn('분석 API', report(d))


class SimHelpers(unittest.TestCase):
    def test_scale(self):
        for rate, want in (('2000', '2.402463'), ('10000', '12.012315'), ('832.479', '1.000000')):
            r = sh(sim_funcs() + f'\nsim_scale {rate}', SIM_BASE_TPS='832.479')
            self.assertEqual(r.stdout.strip(), want, r.stderr)
        r = sh(sim_funcs() + '\nsim_scale 1000', SIM_BASE_TPS='500')
        self.assertEqual(r.stdout.strip(), '2.000000')

    def test_effective_line(self):
        with tempfile.NamedTemporaryFile('w', suffix='.log', delete=False, encoding='utf-8') as f:
            f.write('📊 통계 | 전송: 1,000건 | 실패: 0\n...\n📊 통계 | 전송: 1,234,567건 | 실패: 0\n')
            path = f.name
        r = sh(sim_funcs() + f'\nsim_effective_line {path} 180')
        self.assertEqual(r.stdout.strip(), 'effective_rate=6858.7')
        self.assertTrue(re.fullmatch(r'effective_rate=[0-9.]+', r.stdout.strip()))
        with open(path, 'w', encoding='utf-8') as f:
            f.write('죽음\n')
        self.assertEqual(sh(sim_funcs() + f'\nsim_effective_line {path} 180').stdout, '')
        os.unlink(path)

    def test_run_loadgen_sim_with_fake_docker(self):
        with tempfile.TemporaryDirectory() as d:
            fake = os.path.join(d, 'docker')
            with open(fake, 'w', encoding='utf-8') as f:
                f.write('#!/bin/bash\necho "ARGS: $*"\necho "통계 | 전송: 90,000건 | 실패: 0"\nexit ${FAKE_RC:-124}\n')
            os.chmod(fake, 0o755)
            log = os.path.join(d, 'x.log')
            env = dict(PATH=d + ':' + os.environ['PATH'], NETWORK='net', TOPIC='t', LOADGEN_IMAGE='img',
                       SIM_CODE_DIR='/code', SIM_CPUS='2', SIM_SOURCE_LABEL='SIMLOAD', SIM_BASE_TPS='832.479')
            r = sh(sim_funcs() + f'\nrun_loadgen_sim 2000 180 {log}; echo rc=$?', **env)   # 124 = 정상 만료
            self.assertIn('rc=0', r.stdout)
            with open(log, encoding='utf-8') as f:
                text = f.read()
            self.assertIn('-e SIM_RATE_SCALE=2.402463', text)
            self.assertIn('-e SIM_SOURCE_LABEL=SIMLOAD', text)
            self.assertIn('-e KAFKA_TOPIC_NAME=t', text)
            self.assertIn('-v /code:/app:ro', text)
            self.assertIn('timeout -k 15 180 python stock_simulator.py', text)
            self.assertIn('effective_rate=500.0', text)
            r = sh(sim_funcs() + f'\nrun_loadgen_sim 2000 180 {log}; echo rc=$?', FAKE_RC='1', **env)  # 비정상 → 실패 전파
            self.assertIn('rc=1', r.stdout)

    def test_dispatch_default_is_synthetic(self):
        r = sh(sim_funcs() + '\nrun_loadgen_synthetic(){ echo SYN; }; run_loadgen_sim(){ echo SIM; }\n'
               'LOADGEN_MODE=loadgen run_loadgen 1 2 3; LOADGEN_MODE=sim run_loadgen 1 2 3')
        self.assertEqual(r.stdout.split(), ['SYN', 'SIM'])


class ProbeScript(unittest.TestCase):
    def test_probe_writes_csv_and_dies_without_orphans(self):
        with tempfile.TemporaryDirectory() as d:
            fake = os.path.join(d, 'curl')
            with open(fake, 'w') as f:
                f.write('#!/bin/bash\nsleep 0.2\nprintf "200 0.123"\n')
            os.chmod(fake, 0o755)
            csvf = os.path.join(d, 'p.csv')
            env = dict(os.environ, PATH=d + ':' + os.environ['PATH'], ANALYSIS_PROBE_INTERVAL='1',
                       ANALYSIS_PROBE_SYMBOLS='AAPL NVDA', APP_URL='http://x')
            p = subprocess.Popen([os.path.join(HERE, 'analysis_probe.sh'), '2000', csvf], env=env)
            time.sleep(2.5)
            p.terminate()
            p.wait(timeout=5)
            with open(csvf) as f:
                rows = [l.split(',') for l in f.read().splitlines()]
            self.assertGreaterEqual(len(rows), 2)
            self.assertEqual([r[2] for r in rows[:3]], ['AAPL', 'NVDA', 'AAPL'][:len(rows[:3])])
            self.assertEqual(rows[0][1:2] + rows[0][3:], ['2000', '200', '0.123'])
            time.sleep(0.3)
            left = subprocess.run(['pgrep', '-f', d], capture_output=True, text=True).stdout.split()
            self.assertEqual(left, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
