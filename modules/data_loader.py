import trino
import pandas as pd
import json
import sys
import warnings
import urllib3
from datetime import datetime, timedelta 
import logging
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logger = logging.getLogger(__name__)

class TrinoDataLoader:
    def __init__(self, base_config, catalogs_config, query_config):
        self.base_config = base_config
        self.catalogs_config = catalogs_config
        self.query_config = query_config
        self.connections = {}
    
    def connect(self, catalog_name):
        """특정 catalog에 연결"""
        if catalog_name in self.connections:
            return self.connections[catalog_name]
        
        try:
            catalog_config = self.catalogs_config[catalog_name]
            conn = trino.dbapi.connect(
                host=self.base_config['host'],
                port=self.base_config['port'],
                user=self.base_config['user'],
                http_scheme=self.base_config['http_scheme'],
                auth=trino.auth.BasicAuthentication(
                    self.base_config['user'],
                    self.base_config['password']
                ),
                verify=self.base_config['verify'],
                catalog=catalog_config['catalog'],
                schema=catalog_config['schema']
            )
            self.connections[catalog_name] = conn
            logger.info(f"Trino 연결 성공: {catalog_name}")
            return conn
        except Exception as e:
            logger.error(f"Trino 연결 실패 ({catalog_name}): {e}")
            raise
    
    @staticmethod
    def safe_float(val, default=0.0):
        """안전한 float 변환"""
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            if val.strip().lower() == "nan":
                return float('nan')
            try:
                return float(val)
            except ValueError:
                return default
        return default
    
    def check_data_size_before_query(self, conn, query):
        """EXPLAIN으로 IO 통계 확인"""
        explain_query = f"EXPLAIN (TYPE IO, FORMAT JSON) {query}"
        cur = conn.cursor()

        try:
            logger.info("EXPLAIN 쿼리 실행 중... (예상 데이터 스캔 확인)")
            cur.execute(explain_query)
            explain_result = cur.fetchall()

            json_str = explain_result[0][0]
            io_stats = json.loads(json_str)

            input_tables = io_stats.get("inputTableColumnInfos", [])
            total_input_gb = 0.0
            
            logger.info("\n쿼리 예상 스캔 정보 (입력 기준):")
            for table_info in input_tables:
                table_name = table_info["table"]["schemaTable"]["table"]
                estimate = table_info.get("estimate", {})
                size_bytes = self.safe_float(estimate.get("outputSizeInBytes"), 0)
                size_gb = size_bytes / (1024 ** 3)
                total_input_gb += size_gb
                logger.info(f"  - 테이블: {table_name}")
                logger.info(f"    예상 스캔 크기: {size_bytes / (1024**2):.2f} MB ({size_gb:.3f} GB)")

            global_estimate = io_stats.get("estimate", {})
            output_size_bytes = self.safe_float(global_estimate.get("outputSizeInBytes"), float('nan'))
            output_size_gb = output_size_bytes / (1024 ** 3) if not pd.isna(output_size_bytes) else float('nan')

            logger.info(f"\n총 예상 출력 데이터 크기: "
                  f"{output_size_bytes / (1024**2):.2f} MB ({output_size_gb:.3f} GB)" 
                  if not pd.isna(output_size_gb) else "출력 크기 추정 불가")

            max_size = self.query_config.get('max_data_size_gb', 1.0)
            
            if pd.isna(output_size_gb) or output_size_gb == float('inf'):
                logger.warning(f"출력 추정 실패 → 입력 기준 예측 사용: {total_input_gb:.3f} GB")
                if total_input_gb > max_size:
                    confirm = input(f"데이터 크기가 {total_input_gb:.3f} GB입니다. 계속 진행하시겠습니까? (y/N): ").strip().lower()
                    if confirm not in ['y', 'yes']:
                        logger.info("사용자에 의해 쿼리 취소됨.")
                        sys.exit(0)
            else:
                if output_size_gb > max_size:
                    confirm = input(f"데이터 크기가 {output_size_gb:.3f} GB입니다. 계속 진행하시겠습니까? (y/N): ").strip().lower()
                    if confirm not in ['y', 'yes']:
                        logger.info("사용자에 의해 쿼리 취소됨.")
                        sys.exit(0)

            logger.info("용량 확인 완료. 실제 쿼리 실행을 시작합니다.")

        except Exception as e:
            logger.error(f"EXPLAIN 분석 중 오류 발생: {e}")
            confirm = input("EXPLAIN 실패. 그래도 쿼리 실행하시겠습니까? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                logger.info("사용자에 의해 쿼리 취소됨.")
                sys.exit(0)
        finally:
            cur.close()
    
    def fetch_data_by_catalog(self, queries_by_catalog, target_date=None):
        """Catalog별로 그룹화된 쿼리 실행"""
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        
        all_data = {}
        
        for catalog_name, queries in queries_by_catalog.items():
            logger.info(f"===== {catalog_name} catalog 데이터 조회 시작 =====")
            
            try:
                conn = self.connect(catalog_name)
                for query_name, query_func in queries.items():
                    try:
                        logger.info(f"{catalog_name}.{query_name} 조회 시작")
                        # 쿼리 생성
                        query = query_func(target_date, self.query_config)
                        # 데이터 크기 체크
                        self.check_data_size_before_query(conn, query)
                        # 실제 쿼리 실행
                        cur = conn.cursor()
                        cur.execute(query)
                        rows = cur.fetchall()
                        columns = [desc[0] for desc in cur.description]
                        df = pd.DataFrame(rows, columns=columns)
                        all_data[query_name] = df
                        logger.info(f"{catalog_name}.{query_name} 조회 완료: {len(df)} rows, {len(df.columns)} columns")
                        cur.close()
                    except Exception as e:
                        logger.error(f"{catalog_name}.{query_name} 조회 실패: {e}")
                        all_data[query_name] = pd.DataFrame()
                logger.info(f"===== {catalog_name} catalog 데이터 조회 완료 =====")
            except Exception as e:
                logger.error(f"{catalog_name} catalog 처리 중 오류: {e}")
                for query_name in queries.keys():
                    all_data[query_name] = pd.DataFrame()
        return all_data
    
    def close_all(self):
        """모든 연결 종료"""
        for catalog_name, conn in self.connections.items():
            try:
                conn.close()
                logger.info(f"연결 종료: {catalog_name}")
            except Exception as e:
                logger.error(f"연결 종료 실패 ({catalog_name}): {e}")
        self.connections = {}


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

def _modify_query_for_date_range(query, start_date, end_date):
    """
    쿼리 내 BASE_DT = '...' 조건을 BETWEEN 조건으로 변경
    """
    # 패턴 1: AND A.BASE_DT = '20260204'
    query = query.replace(
        f"AND A.BASE_DT = '{start_date}'",
        f"AND A.BASE_DT BETWEEN '{start_date}' AND '{end_date}'"
    )
    # 패턴 2: AND A.BASE_DT = '{target_date}'
    query = query.replace(
        "AND A.BASE_DT = '{target_date}'",
        f"AND A.BASE_DT BETWEEN '{start_date}' AND '{end_date}'"
    )
    return query

def load_data_lot_3210_3months_cached(self, target_date_str=None):
    """
    DATA_LOT_3210_wafering_300의 직전 7개월치 데이터를 캐싱하고,
    최근 3개월치만 반환합니다.
    """
    from pathlib import Path
    import pandas as pd
    from datetime import datetime, timedelta

    # 프로젝트 루트: data_loader.py 기준 상위 2단계
    PROJECT_ROOT = Path(__file__).parent.parent
    cache_dir = PROJECT_ROOT / "data_cache"
    cache_dir.mkdir(exist_ok=True)

    logger.info(f"캐시 디렉토리: {cache_dir.absolute()}")

    if target_date_str is None:
        target_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    target_dt = datetime.strptime(target_date_str, '%Y%m%d')

    def get_last_n_months_range(n):
        """현재 월 기준 직전 n개월 전체 기간 리스트 반환"""
        months = []
        current = target_dt
        for _ in range(n):
            # 해당 월의 시작일
            start_of_month = current.replace(day=1)
            # 종료일: 말일
            if current.month == 12:
                end_of_month = start_of_month.replace(year=current.year + 1, month=1) - timedelta(days=1)
            else:
                end_of_month = start_of_month.replace(month=current.month + 1) - timedelta(days=1)
            months.append((
                start_of_month.strftime('%Y%m%d'),
                end_of_month.strftime('%Y%m%d')
            ))
            # 이전 달로 이동
            if current.month == 1:
                current = current.replace(year=current.year - 1, month=12)
            else:
                current = current.replace(month=current.month - 1)
        return months[::-1]  # 과거 → 최근 순으로 정렬

    # 1. 전체 다운로드 대상: 직전 7개월
    download_months = get_last_n_months_range(9)
    # 2. 최종 사용 대상: 최근 3개월
    use_months = download_months[-3:]

    data_frames_all = []  # 전체 다운로드 (7개월)
    data_frames_used = []  # 실제 반환용 (3개월)

    for start_dt, end_dt in download_months:
        ym = start_dt[:6]
        file_path = cache_dir / f"DATA_LOT_3210_wafering_300_{ym}.parquet"

        # (1) 캐시 있으면 로드
        if file_path.exists():
            try:
                logger.info(f"캐시에서 로드: {file_path.name}")
                df = pd.read_parquet(file_path)
                logger.info(f"로드 완료: {len(df):,} 건")
                data_frames_all.append(df)
                if (start_dt, end_dt) in use_months:
                    data_frames_used.append(df)
                continue
            except Exception as e:
                logger.warning(f"캐시 파일 손상 또는 읽기 실패: {file_path.name} → 재조회: {e}")

        # (2) 캐시 없으면 Trino에서 조회
        logger.info(f"Trino에서 조회 중: {start_dt} \~ {end_dt}")
        try:
            from queries.daily_queries import DATA_LOT_3210_wafering_300
            dummy_query = DATA_LOT_3210_wafering_300(start_dt, self.query_config)
            query = _modify_query_for_date_range(dummy_query, start_dt, end_dt)

            conn = self.connect("oracle")
            self.check_data_size_before_query(conn, query)
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=columns)
            cur.close()

            # 조회 후 캐시 저장
            df.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"캐시 저장 완료: {file_path.name} ({len(df):,} 건)")

            data_frames_all.append(df)
            if (start_dt, end_dt) in use_months:
                data_frames_used.append(df)

        except Exception as e:
            logger.error(f"{start_dt}-{end_dt} 조회 실패: {e}")
            continue

    # 최종 반환: 최근 3개월 데이터만 결합
    if data_frames_used:
        combined_df = pd.concat(data_frames_used, ignore_index=True)
        logger.info(f"최근 3개월 데이터 병합 완료: {len(combined_df):,} 건 (총 7개월 중)")
        return combined_df
    else:
        logger.warning("사용할 3개월 데이터 모두 조회 실패 → 빈 데이터프레임 반환")
        return pd.DataFrame()

# 메서드 바인딩
TrinoDataLoader.load_data_lot_3210_3months_cached = load_data_lot_3210_3months_cached
