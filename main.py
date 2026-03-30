import argparse
import io
import logging
import os
import sys
from datetime import datetime, timedelta

from config.database import CATALOGS, QUERY_CONFIG, TRINO_BASE_CONFIG, get_last_3months_date_range
from config.email import EMAIL_CONFIG
from config.process_registry import get_enabled_processes
from modules.data_loader import TrinoDataLoader
from modules.email_sender import EmailSender, should_send_now
from modules.report_generator import DailyReportGenerator
from queries.daily_queries import QUERIES_BY_CATALOG, set_runtime_config


# Windows cp949 인코딩 문제 해결
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f"daily_report/logs/daily_report_{datetime.now().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def run_daily_report(target_date: str):
    data_loader = None
    try:
        start_3m, end_3m = get_last_3months_date_range(target_date)
        logger.info(f"3개월 기간: {start_3m} ~ {end_3m}")

        query_config = QUERY_CONFIG.copy()
        query_config["start_3m"] = start_3m
        query_config["end_3m"] = end_3m
        set_runtime_config(query_config)

        enabled_processes = [p.code for p in get_enabled_processes(query_config)]
        logger.info(f"활성 공정: {enabled_processes}")

        logger.info("Step 1: 데이터 조회")
        data_loader = TrinoDataLoader(TRINO_BASE_CONFIG, CATALOGS, QUERY_CONFIG)
        data = data_loader.fetch_data_by_catalog(QUERIES_BY_CATALOG, target_date)
        logger.info(f"총 {len(data)} 개 쿼리 실행 완료")

        logger.info("Step 2: 리포트 생성")
        report_gen = DailyReportGenerator(data)
        report = report_gen.generate()
        return report
    finally:
        if data_loader:
            data_loader.close_all()


def maybe_send_email(report: dict, target_date: str, *, force_send: bool = False):
    if not force_send and not should_send_now(hhmm="08:30"):
        logger.info("현재 시간이 08:30이 아니어서 메일 발송을 건너뜁니다.")
        return

    excel_path = report.get("excel_report")
    sender = EmailSender(EMAIL_CONFIG)
    sender.send_daily_report(report, target_date, excel_path=excel_path)
    logger.info("이메일 발송 완료")


def main():
    parser = argparse.ArgumentParser(description="Daily wafering report")
    parser.add_argument("--send-email", action="store_true", help="실행 직후 메일 즉시 발송")
    args = parser.parse_args()

    target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"===== Daily Report 시작: {target_date} =====")

    try:
        report = run_daily_report(target_date)
        if args.send_email:
            maybe_send_email(report, target_date, force_send=True)
        logger.info(f"===== Daily Report 완료: {target_date} =====")
        return 0
    except Exception as e:
        logger.error(f"Daily Report 실행 중 오류 발생: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
