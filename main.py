import logging
from datetime import datetime, timedelta 
import sys
import io
import os
from functools import partial

from config.database import TRINO_BASE_CONFIG, CATALOGS, QUERY_CONFIG, get_last_3months_date_range
# from config.email import EMAIL_CONFIG 
from queries.daily_queries import QUERIES_BY_CATALOG, set_runtime_config
from modules.data_loader import TrinoDataLoader
from modules.report_generator import DailyReportGenerator 
# from modules.email_sender import EmailSender


# Windows cp949 인코딩 문제 해결
if sys.platform == "win32":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 로깅 핸들러에 encoding 추가
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'daily_report/logs/daily_report_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'daily_report/logs/daily_report_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """메인 실행 함수"""
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    logger.info(f"===== Daily Report 시작: {target_date} =====")

    data_loader = None
    
    try:
        # 1. 3개월 기간 계산
        start_3m, end_3m = get_last_3months_date_range(target_date)
        logger.info(f"3개월 기간: {start_3m} \~ {end_3m}")

        # 2. QUERY_CONFIG에 기간 추가 (복사해서 사용)
        query_config = QUERY_CONFIG.copy()
        query_config['start_3m'] = start_3m
        query_config['end_3m'] = end_3m

        set_runtime_config(query_config)

        # 3. 데이터 로드
        logger.info("Step 1: 데이터 조회")
        data_loader = TrinoDataLoader(TRINO_BASE_CONFIG, CATALOGS, QUERY_CONFIG)
        data = data_loader.fetch_data_by_catalog(QUERIES_BY_CATALOG, target_date)
        
        logger.info(f"총 {len(data)} 개 쿼리 실행 완료")
        
        # 2. 리포트 생성
        logger.info("Step 2: 리포트 생성")
        report_gen = DailyReportGenerator(data)
        report = report_gen.generate()
        
        # 3. 이메일 발송 (나중에 구현)
        # logger.info("Step 3: 이메일 발송")
        # email_sender = EmailSender(EMAIL_CONFIG)
        # email_sender.send_daily_report(report, target_date)
        
        logger.info(f"===== Daily Report 완료: {target_date} =====")
        
        return 0
        
    except Exception as e:
        logger.error(f"Daily Report 실행 중 오류 발생: {e}", exc_info=True)
        return 1
        
    finally:
        if data_loader:
            data_loader.close_all()

if __name__ == "__main__":
    sys.exit(main())
