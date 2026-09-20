"""
pytest 설정: collectors/ 디렉터리를 sys.path에 추가한다.

collectors/ 에는 패키지 구조(__init__.py)가 없으므로, pytest가 tests/ 만
sys.path에 넣는 기본 동작(rootdir 자동 삽입)으로는 `import normalizer`가
실패할 수 있다. 이 conftest.py가 있는 디렉터리(collectors/)를 명시적으로
sys.path에 추가해, 어떤 위치에서 pytest를 실행하든 동작하게 한다.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
