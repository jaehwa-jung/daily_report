import os
from datetime import datetime, timedelta

# Trino 기본 연결 설정
TRINO_BASE_CONFIG = {
    'host': os.getenv('TRINO_HOST', 'aidp-trino-analysis.sksiltron.co.kr'),
    'port': int(os.getenv('TRINO_PORT', 31085)),
    'user': os.getenv('TRINO_USER', '253699'),
    'password': os.getenv('TRINO_PASSWORD', '$iltron3501'),
    'http_scheme': 'https',
    'verify': False
}

# Catalog 설정
CATALOGS = {
    'oracle': {
        'catalog': 'oracle',
        'schema': 'PMDW_MGR'
    },
    'iceberg': {
        'catalog': 'iceberg',
        'schema': 'ibg_lake'
    }
}

# 쿼리 설정
QUERY_CONFIG = {
    'waf_size': '300',
    'fac_ids': ['WF7', 'WF8', 'WFA', 'FPC7', 'FPC8'],
    'grade_filter': 'PN',  # 'PN' 또는 특정 등급
    'oper_div_l': 'WF',
    'max_data_size_gb': 1.0  # EXPLAIN 체크 시 허용 최대 용량 
    } 

def get_last_3months_date_range(self, target_date_str=None):
    """
    현재 월 기준 직전 3개월 전체 기간 계산
    예: target_date=20260201 → ('20251101', '20260131')
    """
    if target_date_str is None:
        target_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    target_dt = datetime.strptime(target_date_str, '%Y%m%d')
    current_year = target_dt.year
    current_month = target_dt.month

    # 현재 월 기준으로 직전 3개월 시작월 계산
    start_month = current_month - 3
    start_year = current_year
    if start_month <= 0:
        start_month += 12
        start_year -= 1
    # 시작일: start_year-start_month-01
    start_date = datetime(start_year, start_month, 1)
    # 종료일: 전월 말일 (현재 월의 전월)
    end_date = datetime(current_year, current_month, 1) - timedelta(days=1)
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    return start_date_str, end_date_str
