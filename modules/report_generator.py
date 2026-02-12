import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import logging
from logging import FileHandler
import matplotlib
from pathlib import Path
import base64
from analysis.defect_analyzer import analyze_flatness, analyze_warp, analyze_growing, analyze_broken, analyze_nano, analyze_pit, analyze_scratch, analyze_chip, analyze_edge, analyze_HUMAN_ERR, analyze_VISUAL, analyze_NOSALE, analyze_OTHER, analyze_GR, analyze_sample,analyze_particle
from config.mappings import REJ_GROUP_TO_MID_MAPPING

from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
import tempfile
from inspect import signature
import re



# 한글 폰트 설정
matplotlib.rcParams['font.family'] = 'Malgun Gothic'  # Windows
matplotlib.rcParams['font.size'] = 10
matplotlib.rcParams['axes.unicode_minus'] = False  # 마이너스 기호 깨짐 방지


# 결과 저장 폴더
REPORT_DIR = "./daily_reports_debug"
os.makedirs(REPORT_DIR, exist_ok=True)

# 기존 로거 설정 대체
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 기존 핸들러 제거
if logger.hasHandlers():
    logger.handlers.clear()

# UTF-8로 기록하는 FileHandler 추가
file_handler = FileHandler('daily_report.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class DailyReportGenerator:
    def __init__(self, data):
        self.data = data
    
    # ──────────────────────────────────────────────────
    # [신규] MS6.csv 기반 제품 정보 병합 함수
    # ──────────────────────────────────────────────────
    def _merge_product_type(self, df):
        """
        df에 PROD_ID 기준으로 MS6.csv의 '제품1' 컬럼을 병합하여 'PRODUCT_TYPE' 추가 + 디버그
        """
        if df.empty:
            df['PRODUCT_TYPE'] = 'Unknown'
            return df
        if 'PROD_ID' not in df.columns:
            df['PRODUCT_TYPE'] = 'Unknown'
            return df
        try:
            project_root = Path(__file__).parent.parent
            ms6_path = project_root / "queries" / "MS6.csv"
            if not ms6_path.exists():
                df['PRODUCT_TYPE'] = 'Unknown'
                return df
            else:
                print(f"[디버그] MS6.csv 파일 존재")
            # 인코딩 자동 감지
            try:
                df_ms6 = pd.read_csv(ms6_path, dtype=str, encoding='utf-8')
            except UnicodeDecodeError:
                df_ms6 = pd.read_csv(ms6_path, dtype=str, encoding='cp949')

            if 'MS6' not in df_ms6.columns or '제품1' not in df_ms6.columns:
                df['PRODUCT_TYPE'] = 'Unknown'
                return df
            # 매핑 딕셔너리 생성 전 확인
            df_ms6_clean = df_ms6.dropna(subset=['MS6', '제품1']).copy()
            if len(df_ms6_clean) == 0:
                df['PRODUCT_TYPE'] = 'Unknown'
                return df
            ms6_mapping = dict(zip(
                df_ms6_clean['MS6'].astype(str).str.strip(),
                df_ms6_clean['제품1'].astype(str).str.strip()
            ))
            # 복사본 생성 및 MS6 추출
            df = df.copy()
            df['MS6'] = df['PROD_ID'].astype(str).str[:6]
            # 매핑 적용
            df['PRODUCT_TYPE'] = df['MS6'].map(ms6_mapping)
            unknown_count = df['PRODUCT_TYPE'].isna().sum()
            df['PRODUCT_TYPE'] = df['PRODUCT_TYPE'].fillna('Unknown')
            # 최종 확인
            if 'PRODUCT_TYPE' in df.columns:
                sample = df[['PROD_ID', 'MS6', 'PRODUCT_TYPE']].dropna().head(3).to_dict('records')
            return df

        except Exception as e:
            import traceback
            traceback.print_exc()
            df['PRODUCT_TYPE'] = 'Unknown'
            return df

    def _get_top3_rej_groups(self):
        """
        안전하게 상위 3개 REJ_GROUP 목록 가져오기
        """
        return self.data.get('DATA_3210_wafering_300', {}).get('top3_rej_groups', [])


    def _create_product_influence_ref(self):
        """
        전 반기(6개월) 데이터 기반 제품 영향성 Ref 데이터 생성
        - 기간: 2025.06 \~ 2025.12
        - 대상 REJ_GROUP: PARTICLE, FLATNESS, NANO, WARP&BOW, GROWING, SCRATCH, VISUAL, SAMPLE
        - 분모: REJ_GROUP == '분모' 인 IN_QTY
        - 산출: PRODUCT_TYPE별 불량개수, Compile 수량, 불량률
        """

        PROJECT_ROOT = Path(__file__).parent.parent
        cache_dir = PROJECT_ROOT / "data_cache"
        pattern = "DATA_LOT_3210_wafering_300_*.parquet"
        parquet_files = list(cache_dir.glob(pattern))

        # 대상 월 설정: 202506 \~ 202512
        target_months = [f"2025{str(m).zfill(2)}" for m in range(6, 13)]
        print(f"대상 월: {target_months}")

        # 대상 REJ_GROUP
        target_rej_groups = ['PARTICLE', 'FLATNESS', 'NANO', 'WARP&BOW', 'GROWING', 'SCRATCH', 'VISUAL', 'SAMPLE']

        df_list = []
        for file_path in parquet_files:
            try:
                # 파일명에서 날짜 추출 (예: DATA_LOT_3210_wafering_300_202506.parquet)
                stem = file_path.stem
                date_part = stem.split('_')[-1]
                if len(date_part) == 6 and date_part.isdigit():
                    if date_part in target_months:
                        df_part = pd.read_parquet(file_path)
                        print(f"{file_path.name} 로드 완료: {len(df_part):,} 건")
                        df_part = self._merge_product_type(df_part)

                        for col in ['IN_QTY', 'LOSS_QTY']: # 타입 보정: IN_QTY, LOSS_QTY → 숫자형
                            if col in df_part.columns:
                                df_part[col] = pd.to_numeric(df_part[col], errors = 'coerce').fillna(0).astype('int64')
                            else:
                                df_part[col] = 0

                        if 'PRODUCT_TYPE' not in df_part.columns: # PRODUCT_TYPE 생성 확인
                            print(f"❌ {file_path.name}: PRODUCT_TYPE 추가 실패")
                            continue
                        df_list.append(df_part)
                    else:
                        print(f"{file_path.name} → 대상 외 월: {date_part}")
                else:
                    print(f"{file_path.name} → 날짜 형식 오류: {date_part}")
            except Exception as e:
                print(f"{file_path.name} 로드 실패: {e}")

        if not df_list:
            print("대상 데이터 없음 → 빈 결과 반환")
            return pd.DataFrame()

        # 병합
        df_full = pd.concat(df_list, ignore_index=True)
        print(f"총 {len(df_full):,} 건 데이터 병합 완료")

        # PRODUCT_TYPE 존재 여부 확인 (이미 병합된 상태 가정)
        if 'PRODUCT_TYPE' not in df_full.columns:
            print("PRODUCT_TYPE 컬럼 없음 → MS6 매핑 필요")
            return pd.DataFrame()
        # ===================================================================
        # 1. 불량개수: 대상 REJ_GROUP + PRODUCT_TYPE별 LOSS_QTY 합계
        # ===================================================================
        df_defect = df_full[
            df_full['REJ_GROUP'].isin(target_rej_groups) &
            (df_full['PRODUCT_TYPE'] != 'Unknown')
        ].copy()

        if df_defect.empty:
            print("불량 데이터 없음")
            return pd.DataFrame()

        defect_summary = df_defect.groupby(['REJ_GROUP', 'PRODUCT_TYPE'], dropna=False)['LOSS_QTY'].sum().reset_index()
        defect_summary.rename(columns={'LOSS_QTY': '불량개수'}, inplace=True)

        # ===================================================================
        # 2. Compile 수량: REJ_GROUP == '분모' 인 IN_QTY 합계
        # ===================================================================
        df_denom = df_full[(df_full['REJ_GROUP'] == '분모') & (df_full['PRODUCT_TYPE'] != 'Unknown')].copy()

        if df_denom.empty:
            print("분모 데이터 없음")
            return pd.DataFrame()

        compile_summary = df_denom.groupby('PRODUCT_TYPE', dropna=False)['IN_QTY'].sum().reset_index()
        compile_summary.rename(columns={'IN_QTY': 'Compile_수량'}, inplace=True)

        # ===================================================================
        # [수정] 물량비 계산: compile_summary 생성 직후 → 중복 방지
        # ===================================================================
        total = df_full[df_full['REJ_GROUP'] == '분모'].copy()
        total_volume = total['IN_QTY'].sum() # 전체 compile 수량
        if total_volume == 0:
            print("⚠️ 전체 분모 수량이 0입니다. 물량비 계산 불가")
            compile_summary['물량비(%)'] = 0.0
        else:
            compile_summary['물량비(%)'] = (compile_summary['Compile_수량'] / total_volume * 100).round(2)
            print(f"📊 전체 수량: {total_volume:,} 매 | 물량비 계산 완료")

        # ===================================================================
        # 3. 병합: 불량개수 + Compile 수량
        # ===================================================================
        result = pd.merge(defect_summary,compile_summary,on='PRODUCT_TYPE',how='left')
        # ===================================================================
        # 4. 불량률 계산
        # ===================================================================
        result['불량률(%)'] = ((result['불량개수'] / result['Compile_수량']) * 100).round(2) #계산용 컬럼 (float) → GAP 분석에 사용
        result['전체 불량률(%)'] = ((result['불량개수'] / total_volume) * 100).round(2) #계산용 컬럼 (float) → GAP 분석에 사용

        # ===================================================================
        # 5. 최종 정리
        # ===================================================================
        result = result[[
            'REJ_GROUP', 'PRODUCT_TYPE', '불량개수', 'Compile_수량', '불량률(%)', '전체 불량률(%)', '물량비(%)'
        ]].sort_values(['REJ_GROUP', '불량률(%)'], ascending=[True, False])

        print(f"제품 영향성 Ref 데이터 생성 완료: {len(result):,} 건")
        return result

    def _create_product_influence_daily(self):
        """
        금일 DATA_LOT_3210_wafering_300 데이터 기반 제품 영향성 분석
        - 출력: 불량개수, Compile_수량, 불량률(%), 물량비(%), 전체 불량률(%)
        - 사용 데이터: self.data['DATA_LOT_3210_wafering_300']
        """
        key = 'DATA_LOT_3210_wafering_300'
        if key not in self.data or self.data[key].empty:
            print(f"{key} 없거나 빈 데이터")
            return pd.DataFrame()

        df = self.data[key].copy()
        print(f"금일 데이터 건수: {len(df):,} 건")

        # --------------------------------------------------
        # 1. PRODUCT_TYPE 매핑
        # --------------------------------------------------
        if 'PRODUCT_TYPE' not in df.columns:
            df = self._merge_product_type(df)
            if 'PRODUCT_TYPE' not in df.columns:
                print("PRODUCT_TYPE 추가 실패")
                return pd.DataFrame()

        # --------------------------------------------------
        # 2. 숫자 컬럼 타입 보정
        # --------------------------------------------------
        for col in ['IN_QTY', 'LOSS_QTY']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            else:
                df[col] = 0

        # 대상 REJ_GROUP
        target_rej_groups = ['PARTICLE', 'FLATNESS', 'NANO', 'WARP&BOW', 'GROWING', 'SCRATCH', 'VISUAL', 'SAMPLE']

        # ===================================================================
        # 1. 불량개수: 대상 REJ_GROUP + PRODUCT_TYPE별 LOSS_QTY 합계
        # ===================================================================
        df_defect = df[
            df['REJ_GROUP'].isin(target_rej_groups) &
            (df['PRODUCT_TYPE'] != 'Unknown')
        ].copy()

        if df_defect.empty:
            print("불량 데이터 없음")
            return pd.DataFrame()

        defect_summary = df_defect.groupby(['REJ_GROUP', 'PRODUCT_TYPE'], dropna=False)['LOSS_QTY'].sum().reset_index()
        defect_summary.rename(columns={'LOSS_QTY': '불량개수'}, inplace=True)

        # ===================================================================
        # 2. Compile 수량: REJ_GROUP == '분모' 인 IN_QTY 합계
        # ===================================================================
        df_denom = df[(df['REJ_GROUP'] == '분모') & (df['PRODUCT_TYPE'] != 'Unknown')].copy()

        if df_denom.empty:
            print("분모 데이터 없음")
            return pd.DataFrame()

        compile_summary = df_denom.groupby('PRODUCT_TYPE', dropna=False)['IN_QTY'].sum().reset_index()
        compile_summary.rename(columns={'IN_QTY': 'Compile_수량'}, inplace=True)

        # ===================================================================
        # [수정] 물량비 계산: compile_summary 생성 직후 → 중복 방지
        # ===================================================================
        total = df[df['REJ_GROUP'] == '분모'].copy()
        total_volume = total['IN_QTY'].sum() # 전체 compile 수량
        if total_volume == 0:
            print("전체 분모 수량이 0입니다. 물량비 계산 불가")
            compile_summary['물량비(%)'] = 0.0
        else:
            compile_summary['물량비(%)'] = (compile_summary['Compile_수량'] / total_volume * 100).round(2)
            print(f"전체 수량: {total_volume:,} 매 | 물량비 계산 완료")

        # ===================================================================
        # 3. 병합: 불량개수 + Compile 수량
        # ===================================================================
        result = pd.merge(defect_summary,compile_summary,on='PRODUCT_TYPE',how='left')
        # ===================================================================
        # 4. 불량률 계산
        # ===================================================================
        result['불량률(%)'] = ((result['불량개수'] / result['Compile_수량']) * 100).round(2) #계산용 컬럼 (float) → GAP 분석에 사용
        result['전체 불량률(%)'] = ((result['불량개수'] / total_volume) * 100).round(2) #계산용 컬럼 (float) → GAP 분석에 사용
        # ===================================================================
        # 5. 최종 정리
        # ===================================================================
        result = result[[
            'REJ_GROUP', 'PRODUCT_TYPE', '불량개수', 'Compile_수량', '불량률(%)' ,  '전체 불량률(%)', '물량비(%)'
        ]].sort_values(['REJ_GROUP', '불량률(%)'], ascending=[True, False])

        print(f"제품 영향성 Ref 데이터 생성 완료: {len(result):,} 건")
        return result

    def _analyze_product_influence_gap(self):
        """
        제품별 불량률 GAP 분석: 6개월 기준(Ref) vs 금일(Daily)
        - 기준: REJ_GROUP + PRODUCT_TYPE
        - 출력: 불량률(%) GAP, 전체 불량률(%) GAP
        - 필터: _get_top3_rej_groups() 기반
        """

        # 1. Ref 데이터 확인
        if 'product_influence_ref' not in self.data:
            print("product_influence_ref 데이터 없음")
            return pd.DataFrame()
        
        ref_df = self.data['product_influence_ref']
        if ref_df.empty:
            print("product_influence_ref 데이터가 비어 있음")
            return pd.DataFrame()

        # 2. Daily 데이터 확인
        if 'product_influence_daily' not in self.data:
            print("product_influence_daily 데이터 없음")
            return pd.DataFrame()
        
        daily_df = self.data['product_influence_daily']
        if daily_df.empty:
            print("product_influence_daily 데이터가 비어 있음")
            return pd.DataFrame()

        # 3. 컬럼 선택 및 이름 변경
        key_cols = ['REJ_GROUP', 'PRODUCT_TYPE']
        ref = ref_df[key_cols + ['불량개수', 'Compile_수량','불량률(%)', '전체 불량률(%)', '물량비(%)']].copy()
        ref.rename(columns={
            '불량개수' : 'Ref_불량개수',
            'Compile_수량' : 'Ref_Compile_수량',
            '불량률(%)': 'Ref_불량률(%)',
            '전체 불량률(%)': 'Ref_전체_불량률(%)',
            '물량비(%)' : 'Ref_물량비(%)'
        }, inplace=True)

        daily = daily_df[key_cols + ['불량개수', 'Compile_수량','불량률(%)', '전체 불량률(%)', '물량비(%)']].copy()
        daily.rename(columns={
            '불량개수' : 'Daily_불량개수',
            'Compile_수량' : 'Daily_Compile_수량',
            '불량률(%)': 'Daily_불량률(%)',
            '전체 불량률(%)': 'Daily_전체_불량률(%)',
            '물량비(%)' : 'Daily_물량비(%)'
        }, inplace=True)

        # 4. 병합 (외부 조인 → 누락 데이터 보존)
        gap = pd.merge(daily, ref, on=key_cols, how='outer').fillna(0.0)

        # 5. GAP 계산
        gap['불량률_GAP(%)'] = (gap['Daily_불량률(%)'] - gap['Ref_불량률(%)']).round(2)
        gap['전체_불량률_GAP(%)'] = (gap['Daily_전체_불량률(%)'] - gap['Ref_전체_불량률(%)']).round(2)
        gap['물량비_GAP(%)'] = (gap['Daily_물량비(%)'] - gap['Ref_물량비(%)']).round(2)
        gap['물량비_불량GAP'] = ((gap['Ref_불량률(%)'] - gap['Ref_전체_불량률(%)']) * gap['물량비_GAP(%)']).round(2)

        # 5. 상위 3개 REJ_GROUP 필터링
        top3_rej_groups = self._get_top3_rej_groups()
        if not top3_rej_groups:
            print("상위 3개 REJ_GROUP 없음 → 전체 데이터 사용")
            filtered_gap = gap
        else:
            print(f"필터링 기준: {top3_rej_groups}")
            filtered_gap = gap[gap['REJ_GROUP'].isin(top3_rej_groups)]

        if filtered_gap.empty:
            print("필터링 후 데이터 없음")
            return pd.DataFrame()

        # 5. 각 REJ_GROUP별로 불량률_GAP(%) 기준 상위 3개씩 추출
        top3_per_group_list = []

        for rej_group in top3_rej_groups:
            group_data = filtered_gap[filtered_gap['REJ_GROUP'] == rej_group]
            if group_data.empty:
                continue
            # GAP 기준 상위 3개
            top3_in_group = group_data.nlargest(3, '물량비_불량GAP')
            top3_per_group_list.append(top3_in_group)

        # 6. 병합
        if not top3_per_group_list:
            print("각 그룹별 상위 3개 추출 실패")
            return pd.DataFrame()

        final_result = pd.concat(top3_per_group_list, ignore_index=True)

        # 7. 정렬: REJ_GROUP → 전체_불량률_영향성 내림차순
        final_result = final_result.sort_values(
            ['REJ_GROUP', '물량비_불량GAP'],
            ascending=[True, False]
        ).reset_index(drop=True)

        print(f"최종 출력: 각 REJ_GROUP별 GAP 상위 3개 제품")
        print(f"결과 (총 {len(final_result)} 건):\n{final_result}")

        # # 8. CSV 저장 (전체 필터링 결과 + 최종 리포트용)
        # try:
        #     PROJECT_ROOT = Path(__file__).parent.parent
        #     output_dir = PROJECT_ROOT / "validation_outputs"
        #     output_dir.mkdir(exist_ok=True, parents=True)

        #     current_date = datetime.now().strftime("%Y%m%d")
        #     csv_path = output_dir / f"제품_영향성_GAP_{current_date}.csv"

        #     # 전체 필터링 결과 저장 (디버깅용)
        #     debug_output = filtered_gap.sort_values(['REJ_GROUP', '불량률_GAP(%)'], ascending=[True, False])
        #     debug_output.to_csv(csv_path, index=False, encoding='utf-8-sig')
        #     print(f"전체 필터링 결과 저장: {csv_path}")

        # except Exception as e:
        #     print(f"CSV 저장 실패: {e}")


        return final_result  

    def generate(self):
        """데일리 리포트 생성"""
        try:
            logger.info("리포트 생성 시작")
            # ===================================================================
            # 모든 데이터에 PRODUCT_TYPE 일괄 병합 (가장 먼저 실행)
            # ===================================================================

            for key in ['DATA_LOT_3210_wafering_300', 'DATA_WAF_3210_wafering_300']:
                if key in self.data and not self.data[key].empty:
                    self.data[key] = self._merge_product_type(self.data[key])
                    if 'PRODUCT_TYPE' in self.data[key].columns:
                        sample = self.data[key].sample(1)[['PROD_ID', 'PRODUCT_TYPE']].to_dict('records')
                else:
                    print(f"⚠️ {key} 없거나 빈 데이터")


            product_influence_ref = self._create_product_influence_ref() #[신규] 제품 영향성 Ref 데이터 생성

            # 3010 보고서 생성
            data_3010_details = self._create_3010_wafering_300()

            # 1. DATA_3210_wafering_300 생성 + 저장
            data_3210_details = self._create_DATA_3210_wafering_300()
            self.data['DATA_3210_wafering_300'] = data_3210_details

            # 2. 제품 영향성 분석
            product_influence_ref = self._create_product_influence_ref()
            product_influence_daily = self._create_product_influence_daily()

            self.data['product_influence_ref'] = product_influence_ref
            self.data['product_influence_daily'] = product_influence_daily

            # 3. GAP 분석 실행 
            product_influence_gap = self._analyze_product_influence_gap()

            # 2. DATA_3210_wafering_300_3months 생성 + 저장 (핵심!)
            data_3210_3months = self._create_DATA_3210_wafering_300_3months()
            self.data['DATA_3210_wafering_300_3months'] = data_3210_3months  

            data_waf_details = self._create_DATA_WAF_3210_wafering_300()
            data_lot_details = self._create_DATA_LOT_3210_wafering_300()

            report = {
                'DATA_3010_wafering_300' : data_3010_details,
                'DATA_3210_wafering_300_details': data_3210_details,
                'DATA_3210_wafering_300_3months': data_3210_3months,
                'DATA_WAF_3210_wafering_300_details': data_waf_details,
                'DATA_LOT_3210_wafering_300_details': data_lot_details,
                'product_influence_gap' : product_influence_gap,
                'raw_data': self.data
            }
            
            # Excel 생성 시 report 전체 전달
            try:
                excel_path = self._export_to_excel(report, output_dir="./daily_reports_debug")
                report['excel_report'] = str(excel_path)
                print(f"Excel 보고서도 생성됨: {excel_path}")
            except Exception as e:
                print(f"Excel 생성 실패: {e}")
                report['excel_report'] = None

            logger.info("리포트 생성 완료")
            return report
        except Exception as e:
            logger.error(f"리포트 생성 실패: {e}")
            raise
    
    def _create_3010_wafering_300(self):
        """3010 수율 데이터 분석 및 그래프 생성 (WF RTY만, 최신 일실적 기준)"""
        details = {}

        if 'DATA_3010_wafering_300' not in self.data or self.data['DATA_3010_wafering_300'].empty:
            print("DATA_3010_wafering_300 데이터 없음 또는 비어 있음")
            return details

        df = self.data['DATA_3010_wafering_300'].copy()

         # --- 전처리 ---
        df['rate'] = pd.to_numeric(df['rate'], errors='coerce')
        df['item_type'] = df['item_type'].astype(str).str.strip()

        # dt_range_raw: 문자열 정리
        df['dt_range_raw'] = df['dt_range'].astype(str).str.strip()

        # item_type에 따라 파싱 전략 분기
        def parse_date(row):
            raw = row['dt_range_raw']
            item_type = row['item_type']
            
            if item_type in ['월실적', '월사업계획']:
                return pd.to_datetime(raw, format='%Y-%m', errors='coerce')
            else:
                return pd.to_datetime(raw, format='%Y-%m-%d', errors='coerce')

        df['dt_range'] = df.apply(parse_date, axis=1)

        # month_str 생성
        df['month_str'] = df['dt_range'].dt.strftime('%Y-%m')
        current_month = (datetime.now() - timedelta(days=1)).strftime('%Y-%m')

        # ──────────────────────────────────────────────────
        # 1. 월 목표/실적
        # ──────────────────────────────────────────────────
        monthly_plan = df[
            (df['item_type'] == '월사업계획') &
            (df['month_str'] == current_month)
        ].copy()
        monthly_plan_val = float(monthly_plan['rate'].iloc[0]) if not monthly_plan.empty else 0.0

        monthly_actual = df[
            (df['item_type'] == '월실적') &
            (df['month_str'] == current_month)
        ].copy()
        monthly_actual_val = float(monthly_actual['rate'].iloc[0]) if not monthly_actual.empty else 0.0

        # ──────────────────────────────────────────────────
        # 2. 기준일: 어제
        # ──────────────────────────────────────────────────
        target_date = (datetime.now().date() - timedelta(days=1))  # 2026-02-03
        print(f"기준일: {target_date}")

        # ──────────────────────────────────────────────────
        # 3. 재사용 함수 정의
        # ──────────────────────────────────────────────────
        def get_latest_or_target(df, item_type, target_date):
            # 동일 날짜 찾기
            same_day = df[
                (df['item_type'] == item_type) &
                (df['dt_range'].notna()) &
                (df['dt_range'].dt.date == target_date)
            ]
            if not same_day.empty:
                return same_day.iloc[0]

            # 없으면 최신 날짜 사용
            latest = df[
                (df['item_type'] == item_type) &
                (df['dt_range'].notna())
            ]
            if not latest.empty:
                return latest.sort_values('dt_range', ascending=False).iloc[0]
            return None

        # ──────────────────────────────────────────────────
        # 4. 일 실적: 어제 기준 → 없으면 최신
        # ──────────────────────────────────────────────────
        daily_actual_row = get_latest_or_target(df, '일실적', target_date)
        if daily_actual_row is not None:
            daily_actual_val = float(daily_actual_row['rate'])
            daily_actual_date = daily_actual_row['dt_range'].strftime('%Y-%m-%d')
        else:
            daily_actual_val = 0.0
            daily_actual_date = "N/A"
            print("일 실적: 데이터 없음")

        # ──────────────────────────────────────────────────
        # 5. 일 목표: 어제 기준 → 없으면 최신
        # ──────────────────────────────────────────────────
        daily_plan_row = get_latest_or_target(df, '일사업계획', target_date)
        if daily_plan_row is not None:
            daily_plan_val = float(daily_plan_row['rate'])
            daily_plan_date = daily_plan_row['dt_range'].strftime('%Y-%m-%d')
        else:
            daily_plan_val = 0.0
            print("일 목표: 데이터 없음")

        # ──────────────────────────────────────────────────
        # 4. 그래프 생성
        # ──────────────────────────────────────────────────
        # PROJECT_ROOT 및 날짜 폴더
        PROJECT_ROOT = Path(__file__).parent.parent
        base_date = (datetime.now().date() - timedelta(days=1))
        date_folder_name = base_date.strftime("%Y%m%d")
        debug_dir = PROJECT_ROOT / "daily_reports_debug" / date_folder_name
        debug_dir.mkdir(exist_ok=True, parents=True)

        chart_path = debug_dir / "3010_yield_chart.png"

        if chart_path.exists():
            chart_path.unlink() #파일 삭제
            print(f"기존 그래프 파일 삭제됨 : {chart_path}")

        fig, ax = plt.subplots(figsize=(12, 6))
        # X축 레이블: [월, 일] → 각각 2개의 카테고리 (WF RTY, WF OAY)
        # 현재는 WF RTY만 사용 중이므로, WF RTY만 표시
        categories = ['WF RTY']
        x_labels = ['월', '일']
        x = np.arange(len(x_labels))  # 월, 일 위치

        # 막대 너비
        bar_width = 0.35

        # 목표/실적 값
        monthly_values = [monthly_plan_val, monthly_actual_val]
        daily_values = [daily_plan_val, daily_actual_val]

        # 색상
        goal_color = 'steelblue'   # 목표
        actual_color = 'orange'     # 실적

        # 월 그룹
        bar1 = ax.bar(x[0] - bar_width/2, monthly_values[0], bar_width, label='목표', color=goal_color)
        bar2 = ax.bar(x[0] + bar_width/2, monthly_values[1], bar_width, label='실적', color=actual_color)

        # 일 그룹
        bar3 = ax.bar(x[1] - bar_width/2, daily_values[0], bar_width, color=goal_color)
        bar4 = ax.bar(x[1] + bar_width/2, daily_values[1], bar_width, color=actual_color)

        # X축 레이블 설정
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, fontsize=12, fontweight='bold')
        ax.set_xlabel('기간', fontsize=12)

        # Y축 범위
        all_vals = monthly_values + daily_values
        min_ylim = min(88.0, min(all_vals) - 0.3)
        max_ylim = max(98.0, max(all_vals) + 0.3)

        ax.set_ylim(min_ylim, max_ylim)        
        ax.set_ybound(min_ylim, max_ylim)      

        # 제목
        ax.set_title(f'WF RTY 수율 비교 (월/일 목표 vs 실적) - 기준일: {daily_actual_date}', fontsize=14, fontweight='bold')
        ax.set_ylabel('수율 (%)', fontsize=12)
        ax.set_xlabel('기간', fontsize=12)

        # 범례 (목표, 실적)
        ax.legend(loc='upper right', fontsize=10)

        # ──────────────────────────────────────────────────
        # 값 표시: 막대 바로 위
        # ──────────────────────────────────────────────────
        def autolabel(rects, values, color='white'):
            for i, rect in enumerate(rects):
                height = rect.get_height()
                ax.text(
                    rect.get_x() + rect.get_width() / 2.,  # 막대 중앙
                    height + 0.05,                         # 막대 바로 위 (약간 높이)
                    f'{values[i]:.2f}%',                   # 값 표시
                    ha='center', va='bottom',               # 수평 중앙, 수직 아래
                    fontsize=9, fontweight='bold', color=color
                )

        autolabel([bar1[0], bar2[0]], monthly_values, 'black')
        autolabel([bar3[0], bar4[0]], daily_values, 'black')

        # ──────────────────────────────────────────────────
        # Gap 표시: 막대 중간에 수직 정렬
        # ──────────────────────────────────────────────────
        monthly_gap = monthly_actual_val - monthly_plan_val
        daily_gap = daily_actual_val - daily_plan_val

        gap_x = [x[0], x[1]]
        gap_y = [(monthly_plan_val + monthly_actual_val) / 2, (daily_plan_val + daily_actual_val) / 2]

        monthly_gap_color = 'orange' if monthly_gap < 0 else 'steelblue'
        daily_gap_color = 'orange' if daily_gap < 0 else 'steelblue'

        ax.text(
            gap_x[0], gap_y[0],
            f'{monthly_gap:+.2f}%',
            ha='center', va='bottom',  # 수평/수직 중앙
            fontsize=9, fontweight='bold', color=monthly_gap_color
        )
        ax.text(
            gap_x[1], gap_y[1],
            f'{daily_gap:+.2f}%',
            ha='center', va='bottom',  # 수평/수직 중앙
            fontsize=9, fontweight='bold', color=daily_gap_color
        )

        # 그리드
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        # 여백 조정
        plt.tight_layout()
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()

        # Base64 인코딩
        with open(chart_path, "rb") as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode()

        # ──────────────────────────────────────────────────
        # 5. 표 생성 (DataFrame)
        # ──────────────────────────────────────────────────
        table_data = {
            '항목': ['WF RTY'],
            '월 목표': [monthly_plan_val],
            '월 실적': [monthly_actual_val],
            '일 목표': [daily_plan_val],
            '일 실적': [daily_actual_val],
            'Gap(월)': [monthly_actual_val - monthly_plan_val],
            'Gap(일)': [daily_actual_val - daily_plan_val],
            '기준일': [daily_actual_date]
        }
        table_df = pd.DataFrame(table_data)

        # details 업데이트
        details.update({
            'chart_path': str(chart_path),
            'img_base64': img_base64,
            'table_df': table_df,
            'summary': table_df,
            'daily_actual_date': daily_actual_date  # Excel에 표시용
        })

        return details

    def _create_DATA_3210_wafering_300(self):
        """3210 불량률 상세 분석 """
        details = {}
        
        if 'DATA_3210_wafering_300' not in self.data or self.data['DATA_3210_wafering_300'].empty:
            print("DATA_3210_wafering_300 데이터 없음 또는 비어 있음")
            return details

        df = self.data['DATA_3210_wafering_300'].copy()

        # 컬럼 타입 변환
        numeric_cols = ['LOSS_RATIO', 'GOAL_RATIO', 'GOAL_RATIO_SUM', 'GAP_RATIO', 'LOSS_QTY', 'MGR_QTY']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 그룹별 집계
        summary = df.groupby(['BASE_DT_NM', 'REJ_GROUP'], dropna=False).agg(
            AVG_LOSS_RATIO=('LOSS_RATIO', 'sum'),
            AVG_GOAL_RATIO=('GOAL_RATIO', 'mean'),
            TOTAL_MGR_QTY=('MGR_QTY', 'mean')
        ).reset_index()

        # 백분율 계산
        summary['LOSS_RATIO_PCT'] = (summary['AVG_LOSS_RATIO'] * 100).round(2)
        summary['GOAL_RATIO_PCT'] = (summary['AVG_GOAL_RATIO'] * 100).round(2)
        summary['GAP_PCT'] = (summary['LOSS_RATIO_PCT'] - summary['GOAL_RATIO_PCT']).round(2)

        # 정렬: GAP 큰 순서대로
        summary = summary.sort_values('GAP_PCT', ascending=False).reset_index(drop=True)

        base_date = summary['BASE_DT_NM'].iloc[0] if len(summary) > 0 else "Unknown"
        print(f"분석 대상일: {base_date}")

        # 출력 디렉터리
        PROJECT_ROOT = Path(__file__).parent.parent
        base_date = (datetime.now().date() - timedelta(days=1))
        date_folder_name = base_date.strftime("%Y%m%d")
        debug_dir = PROJECT_ROOT / "daily_reports_debug" / date_folder_name
        debug_dir.mkdir(exist_ok=True, parents=True)

        # ──────────────────────────────────────────────────
        # 1. 그래프 저장 → Base64 인코딩
        # ──────────────────────────────────────────────────
        chart_path = debug_dir / "prime_gap_chart.png"

        if chart_path.exists():
            chart_path.unlink() #파일 삭제
            print(f"기존 그래프 파일 삭제됨 : {chart_path}")


        plt.figure(figsize=(10, 6))
        x = np.arange(len(summary))
        bars = plt.bar(x, summary['GAP_PCT'],
                    color=summary['GAP_PCT'].apply(lambda x: 'orange' if x > 0 else 'steelblue'), linewidth=1)

        # for i, bar in enumerate(bars):
        #     if summary['GAP_PCT'].iloc[i] > 0:
        #         bar.set_edgecolor('red')
        #         bar.set_linewidth(2)

        plt.title(f"Gap 분석 - {base_date}", fontsize=14, fontweight='bold')
        plt.xlabel('REJ_GROUP', fontsize=12)
        plt.ylabel('GAP (%)', fontsize=12)
        plt.xticks(x, summary['REJ_GROUP'], rotation=45, ha='right')

        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, height + 0.01 * (1 if height >= 0 else -1),
                    f"{height:.2f}%", ha='center', va='bottom' if height >= 0 else 'top',
                    fontsize=9, fontweight='bold')

        plt.ylim(min(-0.15, summary['GAP_PCT'].min() - 0.05), max(1.3, summary['GAP_PCT'].max() + 0.05))
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()

        # Base64 인코딩
        with open(chart_path, "rb") as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode()

        # ──────────────────────────────────────────────────
        # 2. 상위 3개 불량 상세분석
        # ──────────────────────────────────────────────────
        top3_rej_groups = summary.nlargest(3, 'GAP_PCT')['REJ_GROUP'].tolist()
        print(f"상위 3개 불량: {top3_rej_groups}")

        yesterday_mid_list = []
        for rej_group in top3_rej_groups:
            group_df = df[df['REJ_GROUP'] == rej_group].copy()

            # MID_GROUP 매핑 적용
            mid_mapping = REJ_GROUP_TO_MID_MAPPING.get(rej_group, {})
            group_df['MID_GROUP'] = group_df['AFT_BAD_RSN_CD'].map(mid_mapping)
            group_df['MID_GROUP'] = group_df['MID_GROUP'].fillna(group_df['AFT_BAD_RSN_CD'])

            # MID_GROUP별 평균 LOSS_RATIO 계산
            mid_agg = group_df.groupby('MID_GROUP', dropna=False).agg(
                YESTERDAY_LOSS_RATIO=('LOSS_RATIO', 'mean')
            ).reset_index()

            mid_agg['REJ_GROUP'] = rej_group
            mid_agg['YESTERDAY_LOSS_PCT'] = (mid_agg['YESTERDAY_LOSS_RATIO'] * 100).round(2)
            yesterday_mid_list.append(mid_agg[['REJ_GROUP', 'MID_GROUP', 'YESTERDAY_LOSS_RATIO', 'YESTERDAY_LOSS_PCT']])

        # 전체 yesterday MID_GROUP 실적
        yesterday_mid_summary = pd.concat(yesterday_mid_list, ignore_index=True) if yesterday_mid_list else pd.DataFrame()

        # ──────────────────────────────────────────────────
        # 3. 세부분석: 상위 3개 REJ_GROUP에 해당하는 함수만 실행
        # ──────────────────────────────────────────────────
        detailed_analysis = []

        if not top3_rej_groups:
            detailed_analysis.append("[세부분석] 상위 3개 불량 그룹 없음")
        else:
            print(f"분석 대상 REJ_GROUP: {top3_rej_groups}")
            df_wafer = self.data.get('DATA_WAF_3210_wafering_300')
            df_lot = self.data.get('DATA_LOT_3210_wafering_300')

            if df_wafer is None:
                detailed_analysis.append("[세부분석] DATA_WAF_3210_wafering_300 데이터 없음")
            else:

                # REJ_GROUP → 분석 함수 매핑
                REJ_GROUP_TO_ANALYZER = {
                    'FLATNESS': analyze_flatness,
                    'WARP&BOW': analyze_warp,
                    'GROWING': analyze_growing,
                    'BROKEN': analyze_broken,
                    'NANO': analyze_nano,
                    'PIT': analyze_pit,
                    'SCRATCH': analyze_scratch,
                    'CHIP': analyze_chip,
                    'EDGE': analyze_edge,
                    'HUMAN_ERR': analyze_HUMAN_ERR,
                    'VISUAL': analyze_VISUAL,
                    'NOSALE': analyze_NOSALE,
                    'OTHER': analyze_OTHER,
                    'GR_보증': analyze_GR,
                    'SAMPLE' : analyze_sample,
                    'PARTICLE': analyze_particle
                }

                for rej in top3_rej_groups:
                    rej = rej.strip()
                    if rej not in REJ_GROUP_TO_ANALYZER:
                        detailed_analysis.append(f"[{rej} 분석] 매핑된 분석 함수 없음")
                        continue

                    print(f"  → {rej} 분석 시작")
                    analyzer_func = REJ_GROUP_TO_ANALYZER[rej]

                    # 함수 시그니처 기반 자동 인자 바인딩
                    sig = signature(analyzer_func)
                    params = sig.parameters

                    args = []

                    for param_name in params.keys():
                        if param_name.endswith('wafer'):
                            if df_wafer is not None:
                                df_target = df_wafer[df_wafer['REJ_GROUP'] == rej].copy()
                                args.append(df_target)
                        elif param_name.endswith('lot'):
                            if df_lot is not None:
                                args.append(df_lot)  
                            else:
                                result = [f"[{rej} 분석] DATA_LOT_3210_wafering_300 없음"]
                                break
                    else:
                        # 모든 인자 준비 완료 → 함수 호출
                        result = analyzer_func(*args)

                    detailed_analysis.extend(result)

        # ──────────────────────────────────────────────────
        #  5. details에 top3 + yesterday_mid_summary 저장
        # ──────────────────────────────────────────────────
        details.update({
            'summary': summary,
            'top3_rej_groups': top3_rej_groups,
            'yesterday_mid_summary': yesterday_mid_summary,  # 핵심: MID_GROUP 실적 저장
            'chart_path': str(chart_path),
            'img_base64': img_base64,
            'detailed_analysis': detailed_analysis
        })

        self.top3_rej_groups = top3_rej_groups


        return details


    def _create_DATA_3210_wafering_300_3months(self):
        """3210 불량률 상세 분석(3개월) """
        details = {}
        
        if 'DATA_3210_wafering_300_3months' not in self.data or self.data['DATA_3210_wafering_300_3months'].empty:
            print("DATA_3210_wafering_300_3months 데이터 없음 또는 비어 있음")
            return details

        df = self.data['DATA_3210_wafering_300_3months'].copy()

        # 컬럼 타입 변환
        numeric_cols = ['LOSS_RATIO', 'LOSS_QTY', 'MGR_QTY']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # REJ_GROUP별로 중분류(MID_GROUP) 적용
        summary_list = []
        for rej_group, group_df in df.groupby('REJ_GROUP', dropna=False):
            # 해당 REJ_GROUP의 매핑 가져오기
            mid_mapping = REJ_GROUP_TO_MID_MAPPING.get(rej_group, {})
            
            # AFT_BAD_RSN_CD 기준으로 MID_GROUP 생성
            group_df = group_df.copy()
            group_df['MID_GROUP'] = group_df['AFT_BAD_RSN_CD'].map(mid_mapping)
            
            #  매핑되지 않은 경우: 원래 AFT_BAD_RSN_CD 값 유지
            group_df['MID_GROUP'] = group_df['MID_GROUP'].fillna(group_df['AFT_BAD_RSN_CD'])

            # 그룹 집계: REJ_GROUP + MID_GROUP + AFT_BAD_RSN_CD
            agg_df = group_df.groupby(['REJ_GROUP', 'MID_GROUP', 'AFT_BAD_RSN_CD'], dropna=False).agg(
                AVG_LOSS_RATIO=('LOSS_RATIO', 'mean'),
                TOTAL_MGR_QTY=('MGR_QTY', 'mean'),
                COUNT_DAYS=('LOSS_RATIO', 'count')
            ).reset_index()

            summary_list.append(agg_df)

        # 전체 요약 병합
        summary_3months = pd.concat(summary_list, ignore_index=True)
        summary_3months['LOSS_RATIO_PCT'] = (summary_3months['AVG_LOSS_RATIO'] * 100).round(2)

    # yesterday_mid_summary 가져오기
        yesterday_mid = self.data.get('DATA_3210_wafering_300', {}).get('yesterday_mid_summary', pd.DataFrame())
        if yesterday_mid.empty:
            details['summary'] = summary_3months
            return details

        # 상위 3개 REJ_GROUP 가져오기 (Gap 기준)
        top3_rej_groups = self.data.get('DATA_3210_wafering_300', {}).get('top3_rej_groups', [])

        # 3개월 평균 (Ref) 준비
        ref_3months = summary_3months[summary_3months['REJ_GROUP'].isin(yesterday_mid['REJ_GROUP'])].copy()
        ref_3months = ref_3months.groupby(['REJ_GROUP', 'MID_GROUP'], dropna=False).agg(
            REF_AVG_LOSS_RATIO=('AVG_LOSS_RATIO', 'mean')
        ).reset_index()

        # 병합 → Gap 계산 (전체 사용)
        merged = pd.merge(
            yesterday_mid,
            ref_3months,
            on=['REJ_GROUP', 'MID_GROUP'],
            how='inner'
        )

        merged['GAP'] = merged['YESTERDAY_LOSS_PCT'] - merged['REF_AVG_LOSS_RATIO']
        merged['Gap'] = merged['GAP'].round(2)
        merged['실적(%)'] = merged['YESTERDAY_LOSS_PCT']
        merged['Ref(3개월)'] = merged['REF_AVG_LOSS_RATIO'].round(2)
        merged['범례'] = merged['MID_GROUP']

        # 개별 플롯 생성
        plot_paths = self._create_top3_midgroup_plot_per_group(merged, top3_rej_groups)
        # 각 그룹별 표도 상위 3개만
        group_tables = {}
        analysis_text = "[ Prime 주요 열위 불량 세부코드 분석 Ref.(3개월) 比 일실적 변동 (상위 3개) ]\n"
        for rej in top3_rej_groups:
            df_group = merged[merged['REJ_GROUP'] == rej].copy()
            if df_group.empty:
                continue
            top3 = df_group.nlargest(3, 'Gap')[['MID_GROUP', '실적(%)', 'Ref(3개월)', 'Gap']].copy()
            group_tables[rej] = top3

            if len(top3) > 0:
                top_row = top3.iloc[0]
                analysis_text += f"\n {rej} 최대 Gap: {top_row['MID_GROUP']} ({top_row['Gap']:.2f}%)"

        # 최종 details 업데이트
        details.update({
            'summary': summary_3months,
            'top3_midgroup_analysis': {
                'tables': group_tables,
                'plot_paths': plot_paths,
                'analysis': analysis_text.strip()
            }
        })

        return details

    def _create_top3_midgroup_plot_per_group(self, merged_df, top3_rej_groups):
        """
        각 REJ_GROUP별로 Gap 상위 3개 MID_GROUP만 추출하여 개별 막대그래프 생성
        → 결과: {'GR_보증': 'path1.png', 'SAMPLE': 'path2.png', ...}
        """
        # PROJECT_ROOT 및 날짜 폴더
        PROJECT_ROOT = Path(__file__).parent.parent
        base_date = (datetime.now().date() - timedelta(days=1))
        date_folder_name = base_date.strftime("%Y%m%d")
        debug_dir = PROJECT_ROOT / "daily_reports_debug" / date_folder_name
        debug_dir.mkdir(exist_ok=True, parents=True)

        plot_paths = {}

        for rej_group in top3_rej_groups:
            try:
                # 해당 REJ_GROUP 데이터 필터링
                group_df = merged_df[merged_df['REJ_GROUP'] == rej_group].copy()
                if group_df.empty:
                    print(f"{rej_group}: 분석 데이터 없음")
                    continue

                # Gap 기준 상위 3개만 추출
                top3_mids = group_df.nlargest(3, 'Gap')

                # 파일명
                safe_rej = "".join(c if c.isalnum() else "_" for c in rej_group)
                plot_path = debug_dir  / f"prime_midgroup_top3_gap_{safe_rej}.png"

                # 기존 파일 삭제
                if plot_path.exists():
                    plot_path.unlink()

                plt.figure(figsize=(8, 5))
                x = np.arange(len(top3_mids))
                bars = plt.bar(x, top3_mids['Gap'],
                            color=top3_mids['Gap'].apply(lambda x: 'orange' if x > 0 else 'steelblue'), linewidth=1)

                # # Gap > 0인 경우 빨간 테두리 강조
                # for i, bar in enumerate(bars):
                #     if top3_mids['Gap'].iloc[i] > 0:
                #         bar.set_edgecolor('red')
                #         bar.set_linewidth(2)

                plt.title(f"[ {rej_group} 상위 3개 MID_GROUP Gap 분석 ]", fontsize=12, fontweight='bold')
                plt.xlabel('MID_GROUP', fontsize=11)
                plt.ylabel('Gap (%)', fontsize=11)
                plt.xticks(x, top3_mids['MID_GROUP'], rotation=0, ha='center')  #  여기서 rotation=0 → 수평

                # 값 표시
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width() / 2, height + 0.01 * (1 if height >= 0 else -1),
                            f"{height:.2f}%", ha='center', va='bottom' if height >= 0 else 'top',
                            fontsize=12, fontweight='bold')

                # y축 범위
                plt.ylim(min(-0.15, top3_mids['Gap'].min() - 0.05), max(1.3, top3_mids['Gap'].max() + 0.05))
                plt.grid(axis='y', linestyle='--', alpha=0.7)
                plt.tight_layout()

                # 저장
                plt.savefig(str(plot_path), dpi=300, bbox_inches='tight')
                plt.close()

                if plot_path.exists():
                    plot_paths[rej_group] = str(plot_path)
                else:
                    raise RuntimeError(f"파일 생성 실패: {plot_path}")

            except Exception as e:
                print(f"{rej_group} 플롯 생성 실패: {e}")
                continue

        return plot_paths

    def _create_DATA_WAF_3210_wafering_300(self):
        """3210 WAF 상세 분석"""
        details = {}
        key = 'DATA_WAF_3210_wafering_300'
        if key in self.data and not self.data[key].empty:
            df = self.data[key].copy()
        else:
            print("⚠️ DATA_WAF_3210_wafering_300 없거나 빈 데이터")

        return details
    
    def _create_DATA_LOT_3210_wafering_300(self):
        """3210 LOT 상세 분석 - 캐시된 3개월 데이터 + self.data의 당일 데이터 모두 활용"""

        details = {}

        # ===================================================================
        # 1. [신규] data_cache에서 3개월 데이터 직접 로드 (장기 분석용)
        # ===================================================================
        PROJECT_ROOT = Path(__file__).parent.parent  

        # 어제 날짜 폴더 생성
        base_date = (datetime.now().date() - timedelta(days=1))
        date_folder_name = base_date.strftime("%Y%m%d")  # 예: 20260204

        # 출력 폴더: daily_reports_debug/YYYYMMDD
        debug_dir = PROJECT_ROOT / "daily_reports_debug" / date_folder_name
        debug_dir.mkdir(exist_ok=True, parents=True)  # 폴더 생성

        target_months = []
        current = base_date.replace(day=1)
        for _ in range(3):
            # 전월로 이동
            current = (current - timedelta(days=1)).replace(day=1)
            month_str = current.strftime("%Y%m")
            target_months.append(month_str)

        # 역순 정렬 (과거 → 최근)
        target_months = sorted(target_months)

        print(f"[캐시 필터링] 최근 3개월 대상 월: {target_months}")

        cache_dir = PROJECT_ROOT / "data_cache"
        pattern = "DATA_LOT_3210_wafering_300_*.parquet"
        parquet_files = list(cache_dir.glob(pattern))

        df_cached_3months = pd.DataFrame()

        if parquet_files:
            valid_files = []
            for file_path in parquet_files:
                try:
                    stem = file_path.stem  # 전체 이름 (확장자 제외)
                    date_part = stem.split('_')[-1]  # '202506'

                    if len(date_part) != 6 or not date_part.isdigit():
                        continue  # 형식 맞지 않으면 건너뜀

                    file_ym = date_part  # '202506' 형식
                except Exception as e:
                    print(f"[캐시] {file_path.name}에서 월 정보 추출 실패 → 건너뜀: {e}")
                    continue

                if file_ym in target_months:
                    valid_files.append(file_path)

            print(f"[캐시 필터링] 전체 {len(parquet_files)}개 중 대상 {len(valid_files)}개 파일 선정: {[f.name for f in valid_files]}")

            dfs = []
            for file_path in valid_files:
                try:
                    df_part = pd.read_parquet(file_path)
                    print(f"[캐시] {file_path.name} 로드 완료: {len(df_part):,} 건")
                    dfs.append(df_part)
                except Exception as e:
                    print(f"[캐시] {file_path.name} 읽기 실패: {e}")

            if dfs:
                df_cached_3months = pd.concat(dfs, ignore_index=True)
                print(f"[캐시] 총 {len(df_cached_3months):,} 건 데이터 병합 완료")
            else:
                print("[캐시] 모든 파일 로드 실패 → 3개월 데이터 없음")
        else:
            print("[캐시] data_cache에 DATA_LOT_3210_wafering_300_*.parquet 파일 없음")

        # ===================================================================
        # 2. [기존] self.data에서 당일 데이터 사용 (실시간 리포트용)
        # ===================================================================
        df_self_data = pd.DataFrame()
        if 'DATA_LOT_3210_wafering_300' in self.data and not self.data['DATA_LOT_3210_wafering_300'].empty:
            df_self_data = self.data['DATA_LOT_3210_wafering_300']
            print(f"[self.data] DATA_LOT_3210_wafering_300 데이터 건수: {len(df_self_data):,} 건")
        else:
            print("[self.data] DATA_LOT_3210_wafering_300 없거나 빈 데이터")


        # ===================================================================
        # [핵심] MS6 기반 PRODUCT_TYPE 병합
        # ===================================================================
        if not df_cached_3months.empty:
            df_cached_3months = self._merge_product_type(df_cached_3months)

        if not df_self_data.empty:
            df_self_data = self._merge_product_type(df_self_data)

        print(f"PRODUCT_TYPE 병합 완료: 3개월 {df_cached_3months['PRODUCT_TYPE'].notna().sum()}건, 당일 {df_self_data['PRODUCT_TYPE'].notna().sum()}건")

        # ===================================================================
        # 3. [핵심] 3개월 데이터 기반 Loss Rate 분석
        # ===================================================================
        if not df_cached_3months.empty:
            # 3개월 수량 합계 → 평균으로 변환 (3으로 나눔)
            total_months = 3

            # 분모: REJ_GROUP == "분모" 인 IN_QTY 합계
            denominator_data = df_cached_3months[df_cached_3months['REJ_GROUP'] == '분모']
            total_in_qty = denominator_data['IN_QTY'].sum() 
            avg_in_qty = total_in_qty / total_months  # 3개월 평균 전체 분모

            if avg_in_qty == 0:
                print(" 분모(IN_QTY)가 0입니다. Loss Rate 계산 불가")
                return details

            # ===================================================================
            #  1. 전체 (Total) CRET_CD별 Loss Rate
            # ===================================================================

            valid_cached = df_cached_3months[df_cached_3months['REJ_GROUP'].notna()]
            total_loss_by_cret = valid_cached.groupby('CRET_CD')['LOSS_QTY'].sum() / total_months #FS/HG/RESC 별 loss_qty 3개월 평균

            # ===================================================================
            #  2. 당일 CRET_CD별 LOSS_QTY
            # ===================================================================
            daily_loss_by_cret = pd.Series(dtype='int64')
            total_daily_qty = 0

            if not df_self_data.empty:
                valid_daily = df_self_data[df_self_data['REJ_GROUP'].notna()]
                daily_loss_by_cret = valid_daily.groupby('CRET_CD')['LOSS_QTY'].sum()

                denominator_daily = df_self_data[df_self_data['REJ_GROUP'] == '분모']
                total_daily_qty = denominator_daily['IN_QTY'].sum()  #  정의 추가
            else:
                print("[self.data] DATA_LOT_3210_wafering_300 없거나 빈 데이터")

            # ===================================================================
            #  3.  전체 비교 표 생성 (모수 포함)
            # ===================================================================
            cret_list = ['FS', 'HG', 'RESC']
            report_table_total = []

            #  원시 데이터 저장용
            ref_qty_dict = {}
            daily_qty_dict = {}

            for cret_cd in cret_list:
                ref_qty = total_loss_by_cret.get(cret_cd, 0)
                daily_qty = daily_loss_by_cret.get(cret_cd, 0)

                ref_rate = (ref_qty / avg_in_qty) * 100 if avg_in_qty != 0 else 0
                daily_rate = (daily_qty / total_daily_qty) * 100 if avg_in_qty != 0 else 0
                gap = daily_rate - ref_rate

                report_table_total.append({
                    '구분': cret_cd,
                    'Ref.(3개월)': int(ref_qty),
                    '일': int(daily_qty),
                    'Ref.(3개월)%': f"{ref_rate:.2f}%",
                    '일%': f"{daily_rate:.2f}%",
                    'Gap': f"{gap:+.2f}%"
                })

                #  원시 데이터 저장
                ref_qty_dict[cret_cd] = int(ref_qty)
                daily_qty_dict[cret_cd] = int(daily_qty)

            #  모수 저장
            ref_qty_dict['모수'] = int(avg_in_qty) #3개월 평균 분모 -> ref 분모
            daily_qty_dict['모수'] = int(total_daily_qty) #일 분모


            report_table_total.append({
                '구분': '모수',
                'Ref.(3개월)': ref_qty_dict['모수'],
                '일': daily_qty_dict['모수'],
                'Ref.(3개월)%': "",
                '일%': "",
                'Gap': ""
            })

            #  details에 저장 (표 X, 값 O)
            details['rc_hg_ref_qty_total'] = ref_qty_dict
            details['rc_hg_daily_qty_total'] = daily_qty_dict
            details['rc_hg_avg_in_qty'] = avg_in_qty

            report_table_total_df = pd.DataFrame(report_table_total)
            details['summary'] = report_table_total_df

            # ===================================================================
            #  4. 그룹별 비교 표 생성 + 그래프 생성 (모수 제외)
            # ===================================================================
            rej_groups = ['PARTICLE', 'FLATNESS', 'WARP&BOW', 'NANO']
            details['rc_hg_ref_qty_by_group'] = {}
            details['rc_hg_daily_qty_by_group'] = {}
            details['rc_hg_gap_data_by_group'] = {}
            details['loss_rate_table_by_group'] = {}
            details['rc_hg_gap_chart_path_by_group'] = {}

            for group in rej_groups:
                # 각 그룹별 3개월 데이터 필터링
                group_data = df_cached_3months[df_cached_3months['REJ_GROUP'] == group]
                group_loss_by_cret = group_data.groupby('CRET_CD')['LOSS_QTY'].sum() / total_months
                # 각 그룹별 당일 데이터 필터링
                group_daily_loss_by_cret = pd.Series(dtype='int64')
                if not df_self_data.empty:
                    group_self_data = df_self_data[df_self_data['REJ_GROUP'] == group]
                    group_daily_loss_by_cret = group_self_data.groupby('CRET_CD')['LOSS_QTY'].sum()

                group_table = []
                gap_data = {}
                ref_qty_dict_group = {}
                daily_qty_dict_group = {}

                for cret_cd in cret_list:
                    ref_qty = group_loss_by_cret.get(cret_cd, 0)
                    daily_qty = group_daily_loss_by_cret.get(cret_cd, 0)

                    ref_rate = (ref_qty / avg_in_qty) * 100 if avg_in_qty != 0 else 0
                    daily_rate = (daily_qty / avg_in_qty) * 100 if avg_in_qty != 0 else 0
                    gap = daily_rate - ref_rate

                    group_table.append({
                        '구분': cret_cd,
                        'Ref.(3개월)': int(ref_qty),
                        '일': int(daily_qty),
                        'Ref.(3개월)%': f"{ref_rate:.2f}%",
                        '일%': f"{daily_rate:.2f}%",
                        'Gap': f"{gap:+.2f}%"
                    })

                    gap_data[cret_cd] = gap
                    ref_qty_dict_group[cret_cd] = int(ref_qty)
                    daily_qty_dict_group[cret_cd] = int(daily_qty)

                # 기존 방식과 동일하게 DataFrame으로 저장
                group_table_df = pd.DataFrame(group_table)
                if group_table_df.empty:
                    group_table_df = pd.DataFrame(columns=['구분', 'Ref.(3개월)', '일', 'Ref.(3개월)%', '일%', 'Gap'])
                details['loss_rate_table_by_group'][group] = group_table_df

                #  저장
                details['rc_hg_ref_qty_by_group'][group] = ref_qty_dict_group
                details['rc_hg_daily_qty_by_group'][group] = daily_qty_dict_group
                details['rc_hg_gap_data_by_group'][group] = gap_data  # 그래프용

                fig, ax = plt.subplots(figsize=(8, 4))

                categories = ['FS', 'HG', 'RESC']
                values = [float(gap_data.get(c, 0.0)) for c in categories]

                # 색상 설정: 양수=주황, 음수=파랑, 0=회색
                
                colors = ['orange' if v > 0 else 'steelblue' if v < 0 else 'gray' for v in values]

                # bar (수직 막대)
                bars = ax.bar(categories, values, color=colors, width=0.6)

                # 제목 및 라벨
                ax.set_title(f'RC/HG 보상({group})', fontsize=12, fontweight='bold')
                ax.set_ylabel('Gap (%)', fontsize=10)  # Y축이 Gap
                ax.set_xlabel('구분', fontsize=10)     # X축이 구분

                min_ylim = min(0, min(values) - 0.3)
                max_ylim = max(0, max(values) + 0.3)

                ax.set_ylim(min_ylim, max_ylim)        
                ax.set_ybound(min_ylim, max_ylim)      

                ax.grid(True, axis='y', linestyle='--', alpha=0.7)  # Y축 기준 그리드

                # 막대 위에 값 표시
                for bar, val in zip(bars, values):
                    height = bar.get_height()
                    if height >= 0:
                        y_pos = height + 0.005
                        va = 'bottom'
                    else:
                        y_pos = height - 0.005
                        va = 'top'
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        y_pos,
                        f"{val:+.2f}%",
                        ha='center',
                        va=va,
                        fontsize=9,
                        fontweight='bold',
                        color='black'
                    )

                plt.tight_layout()

                graph_path = debug_dir / f"RC_HG_보상_{group}.png"
                if graph_path.exists():
                    graph_path.unlink()
                    print(f"기존 그래프 파일 삭제됨: {graph_path}")

                plt.savefig(graph_path, dpi=150, bbox_inches='tight')
                plt.close()

                details['rc_hg_gap_chart_path_by_group'][group] = str(graph_path)

            # ===================================================================
            #  7. 전체 RC/HG 보상 그래프 생성
            # ===================================================================
            total_gap_data = {}
            for row in report_table_total:
                if row['구분'] in ['FS', 'HG', 'RESC']:
                    gap_str = row['Gap'].replace('%', '').replace('+', '')
                    total_gap_data[row['구분']] = float(gap_str)

            categories = ['FS', 'HG', 'RESC']
            values = [total_gap_data.get(c, 0.0) for c in categories]

            colors = ['orange' if total_gap_data.get(c, 0) > 0 else 
                    'steelblue' if total_gap_data.get(c, 0) < 0 else 'gray' for c in categories]

            fig, ax = plt.subplots(figsize=(8, 4))
            bars = ax.bar(categories, values, color=colors,  width=0.6)

            ax.set_title('RC/HG 보상(Ref.비 수준)', fontsize=12, fontweight='bold')
            ax.set_ylabel('Gap (%)', fontsize=10)
            ax.set_xlabel('구분', fontsize=10)
            
            min_ylim = min(0, min(values) - 0.3)
            max_ylim = max(0, max(values) + 0.3)

            ax.set_ylim(min_ylim, max_ylim)        
            ax.set_ybound(min_ylim, max_ylim)    

            ax.grid(True, axis='y', linestyle='--', alpha=0.7)

            for bar, val in zip(bars, values):
                height = bar.get_height()
                if height >= 0:
                    y_pos = height + 0.005
                    va = 'bottom'
                else:
                    y_pos = height - 0.005
                    va = 'top'
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    y_pos,
                    f"{val:+.2f}%",
                    ha='center',
                    va=va,
                    fontsize=9,
                    fontweight='bold',
                    color='black'
                )

            plt.tight_layout()
            total_graph_path = debug_dir / "RC_HG_보상_전체.png"
            if total_graph_path.exists():
                total_graph_path.unlink()
                print(f"기존 전체 그래프 파일 삭제됨: {total_graph_path}")

            plt.savefig(total_graph_path, dpi=150, bbox_inches='tight')
            plt.close()

            details['rc_hg_gap_chart_path_total'] = str(total_graph_path)

            # ===================================================================
            # 8. 기본 정보 추가
            # ===================================================================
            details['cache_data_available'] = not df_cached_3months.empty
            details['self_data_available'] = not df_self_data.empty
            details['cache_total_count'] = len(df_cached_3months) if not df_cached_3months.empty else 0
            details['self_data_count'] = len(df_self_data) if not df_self_data.empty else 0
            details['avg_in_qty'] = avg_in_qty

        else:
            # 빈 값 저장
            details['rc_hg_ref_qty_total'] = {}
            details['rc_hg_daily_qty_total'] = {}
            details['rc_hg_ref_qty_by_group'] = {}
            details['rc_hg_daily_qty_by_group'] = {}
            details['rc_hg_avg_in_qty'] = 0
            details['rc_hg_gap_chart_path_by_group'] = {}
            details['rc_hg_gap_chart_path_total'] = ""

        return details


    def _export_to_excel(self, report, output_dir="./daily_reports_debug"):
        """Excel 보고서 생성"""
        try:
            PROJECT_ROOT = Path(__file__).parent.parent
            base_date = (datetime.now().date() - timedelta(days=1))
            date_folder_name = base_date.strftime("%Y%m%d")
            debug_dir = PROJECT_ROOT / "daily_reports_debug" / date_folder_name
            debug_dir.mkdir(exist_ok=True, parents=True)

            excel_path = debug_dir / "daily_report.xlsx"

            # 기존 파일 삭제
            if excel_path.exists():
                try:
                    excel_path.unlink()
                    print(f"기존 파일 삭제됨: {excel_path}")
                except PermissionError:
                    raise PermissionError(f"엑셀을 닫고 다시 시도하세요: {excel_path}")

            # 워크북 생성
            wb = Workbook()
            ws = wb.active
            ws.title = "Prime 분석"

            # ──────────────────────────────────────────────────
            # 1. [3010 수율 분석] 제목 및 그래프 삽입 (가장 위)
            # ──────────────────────────────────────────────────
            ws.merge_cells('A1:G1')
            ws['A1'] = "[ WF RTY 수율 비교 (월/일 목표 vs 실적) ]"
            ws['A1'].font = Font(size=14, bold=True)
            ws['A1'].alignment = Alignment(horizontal='left')

            data_3010_details = report.get('DATA_3010_wafering_300', {})
            chart_path_3010 = data_3010_details.get('chart_path')

            if not chart_path_3010:
                ws['A2'] = "[차트 없음: chart_path 없음]"
                ws['A2'].font = Font(size=10, color="FF0000")
                print("3010: 삽입할 chart_path 없음")
            else:
                chart_path_3010 = Path(chart_path_3010)
                if not chart_path_3010.exists():
                    ws['A2'] = f"[차트 파일 없음: {chart_path_3010.name}]"
                    ws['A2'].font = Font(size=10, color="FF0000")
                    print(f"3010: 차트 파일 없음: {chart_path_3010}")
                else:
                    try:
                        img = ExcelImage(str(chart_path_3010))
                        img.width = 600
                        img.height = 300
                        ws.add_image(img, 'A2')
                    except Exception as e:
                        ws['A2'] = f"[이미지 삽입 실패: {e}]"
                        ws['A2'].font = Font(size=10, color="FF0000")

            # 3010 표 삽입 (H2 \~ K6)
            table_df_3010 = data_3010_details.get('table_df')
            if table_df_3010 is not None and not table_df_3010.empty:
                start_row = 4
                start_col = 8  # H열
                
                # 수정: table_df_3010 복사 후 포맷팅
                table_df_3010_fmt = table_df_3010.copy()

                # 포맷팅할 컬럼들 (예: 수율(%) → 94.28%)
                pct_cols = ['월 목표', '월 실적', '일 목표', '일 실적', 'Gap(월)', 'Gap(일)']  # 실제 컬럼명 확인 필요
                for col in pct_cols:
                    if col in table_df_3010_fmt.columns:
                        table_df_3010_fmt[col] = pd.to_numeric(table_df_3010_fmt[col], errors='coerce') / 100.0 # 수율(%) 컬럼을 숫자형으로 유지 (예: 94.28 → 0.9428)

                for r_idx, row in enumerate(dataframe_to_rows(table_df_3010_fmt, index=False, header=True), start_row):
                    for c_idx, value in enumerate(row, start_col):
                        cell = ws.cell(row=r_idx, column=c_idx, value=value)
                        cell.border = Border(
                            left=Side(style='thin'),
                            right=Side(style='thin'),
                            top=Side(style='thin'),
                            bottom=Side(style='thin')
                        )
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')

                        if r_idx == start_row: #헤더행
                            cell.font = Font(bold=True, size=10)
                            cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        else:
                            if c_idx in [start_col, start_col + 1, start_col + 2, start_col + 3, start_col + 4, start_col + 5, start_col + 6]:
                                cell.number_format = '0.00%'

                            if c_idx in [start_col + 5, start_col + 6]:
                                try:
                                    gap_val = float(value) if pd.notna(value) else 0.0
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=True, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=True, size=9)
                                except:
                                    pass

            else:
                ws['H2'] = "표 없음"
                ws['H2'].font = Font(size=10, color="FF0000")

            # ──────────────────────────────────────────────────
            # 2. 기존 Prime 분석 그래프 삽입 (A10부터 시작)
            # ──────────────────────────────────────────────────
            next_start_row = 17

            data_3210_details = report.get('DATA_3210_wafering_300_details', {})
            chart_path = data_3210_details.get('chart_path')

            ws.merge_cells(f'A{next_start_row}:D{next_start_row}')
            ws[f'A{next_start_row}'] = "[ Prime 불량 목표 比 일실적 변동 ]"
            ws[f'A{next_start_row}'].font = Font(size=14, bold=True)
            ws[f'A{next_start_row}'].alignment = Alignment(horizontal='left')

            if not chart_path:
                ws[f'A{next_start_row + 1}'] = "[차트 없음: chart_path 없음]"
                ws[f'A{next_start_row + 1}'].font = Font(size=10, color="FF0000")
            else:
                chart_path = Path(chart_path)
                if not chart_path.exists():
                    ws[f'A{next_start_row + 1}'] = f"[차트 파일 없음: {chart_path.name}]"
                    ws[f'A{next_start_row + 1}'].font = Font(size=10, color="FF0000")
                else:
                    try:
                        img = ExcelImage(str(chart_path))
                        img.width = 500
                        img.height = 350
                        ws.add_image(img, f'A{next_start_row + 1}')
                    except Exception as e:
                        ws[f'A{next_start_row + 1}'] = f"[이미지 삽입 실패: {e}]"
                        ws[f'A{next_start_row + 1}'].font = Font(size=10, color="FF0000")

            # ──────────────────────────────────────────────────
            # 3. 기존 요약 표 삽입 (G11 \~ K15)
            # ──────────────────────────────────────────────────
            table_df_for_row_height = None

            if 'summary' in data_3210_details:
                table_df = data_3210_details['summary'][['REJ_GROUP', 'GOAL_RATIO_PCT', 'LOSS_RATIO_PCT', 'GAP_PCT']].copy()
                table_df.columns = ['구분', '목표(%)', '실적(%)', 'GAP(%)']

                for col in ['목표(%)', '실적(%)', 'GAP(%)']:
                    table_df[col] = table_df[col] / 100 # 숫자형 유지 (예: 0.33 → 0.0033)

                table_df_for_row_height = table_df

                start_row = next_start_row + 1
                start_col = 8

                for r_idx, row in enumerate(dataframe_to_rows(table_df, index=False, header=True), start_row):
                    for c_idx, value in enumerate(row, start_col):
                        cell = ws.cell(row=r_idx, column=c_idx, value=value)
                        cell.border = Border(
                            left=Side(style='thin'),
                            right=Side(style='thin'),
                            top=Side(style='thin'),
                            bottom=Side(style='thin')
                        )
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')

                        if r_idx == start_row:
                            cell.font = Font(bold=True, size=10)
                            cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        else:
                            if c_idx in [start_col, start_col + 1, start_col + 2, start_col + 3]:  # H, I, K열
                                cell.number_format = '0.00%'
                            if c_idx == start_col + 3:
                                try:
                                    if isinstance(value, str):
                                        clean_val = value.replace('%', '').replace('+', '').strip()
                                        gap_val = float(clean_val) if clean_val else 0.0
                                    else:
                                        gap_val = float(value)

                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=True, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=True, size=9)
                                except:
                                    pass

            # 행 높이 조정 (기존 표)
            if table_df_for_row_height is not None:
                for row in range(next_start_row + 1, next_start_row + 1 + len(table_df_for_row_height) + 1):
                    ws.row_dimensions[row].height = 18
            else:
                print("요약 표 없음 → 행 높이 조정 생략")

            # ──────────────────────────────────────────────────
            # 4. [Prime 주요 열위 불량 세부코드 분석] 섹션
            # ──────────────────────────────────────────────────
            row_start = next_start_row + 20  # 여유 있게 시작
            ws.merge_cells(f'A{row_start-1}:F{row_start-1}')
            ws[f'A{row_start-1}'] = "[ Prime 주요 열위 불량 세부코드 분석 Ref.(3개월) 比 일실적 변동 (상위 3개) ]"
            ws[f'A{row_start-1}'].font = Font(size=12, bold=True)
            ws[f'A{row_start-1}'].alignment = Alignment(horizontal='left')

            mid_analysis = report.get('DATA_3210_wafering_300_3months', {}).get('top3_midgroup_analysis', {})
            plot_paths = mid_analysis.get('plot_paths', {})
            group_tables = mid_analysis.get('tables', {})
            detailed_analysis = data_3210_details.get('detailed_analysis', [])

            # 1. 안전한 파싱
            groups = []
            current_group = None
            current_items = []

            for line in detailed_analysis:
                stripped = line.strip()
                if not stripped:
                    continue

                if stripped.startswith("[") and "분석" in stripped:
                    content = stripped.strip("[]")
                    if " 분석" in content:
                        current_group = content.replace(" 분석", "").strip()
                    elif "분석" in content:
                        current_group = content.replace("분석", "").strip()
                    else:
                        current_group = content.strip()

                    if current_group and current_items:
                        groups.append((current_group, current_items))
                    current_items = []
                    continue

                if stripped.startswith("→  → "):
                    judgment = stripped.replace("→  → ", "").strip()
                    if current_items and isinstance(current_items[-1], dict):
                        current_items[-1]['judgment'] = judgment
                    continue

                if stripped.startswith("→ ") and current_group:
                    content = stripped[2:].strip()
                    if content.startswith("- "):
                        current_items.append({
                            'type': 'sub',
                            'content': content[2:].strip(),
                            'details': [],
                            'judgment': None
                        })
                    else:
                        current_items.append({
                            'type': 'item',
                            'content': content,
                            'judgment': None
                        })
                    continue

                if ":" in stripped and current_items and isinstance(current_items[-1], dict):
                    current_items[-1]['details'].append(stripped)

            if current_group and current_items:
                groups.append((current_group, current_items))

            # 2. 보고서 문장 생성
            formatted_analysis = []
            for i, (group_name, items) in enumerate(groups):
                formatted_analysis.append(f"{i+1}. {group_name} 분석")
                item_idx = 1
                for item in items:
                    if item['type'] == 'item':
                        formatted_analysis.append(f"  {item_idx}) {item['content']}")
                        item_idx += 1
                    elif item['type'] == 'sub':
                        details_str = ", ".join(item['details']) if item['details'] else ""
                        judgment_str = f" → {item['judgment']}" if item.get('judgment') else ""
                        combined = f"{item['content']} : {details_str}{judgment_str}".rstrip(" : ")
                        formatted_analysis.append(f"  {item_idx}) {combined}")
                        item_idx += 1

            # 3. 그래프 + 표 + 분석 텍스트를 같은 행에 배치
            if not plot_paths:
                ws[f'A{row_start}'] = "MID_GROUP 분석 그래프 없음"
                ws[f'A{row_start}'].font = Font(size=10, color="FF0000")
                row_start += 3
            else:
                for rej_group, plot_path in plot_paths.items():
                    # 현재 행 저장
                    current_row = row_start

                    # ──────────────────────────────────────────────────
                    # 1. 그래프 삽입 (A열)
                    # ──────────────────────────────────────────────────
                    if not Path(plot_path).exists():
                        ws.cell(row=current_row, column=1, value=f"{rej_group} 그래프 없음").font = Font(size=9, color="FF0000")
                        # 그래프 없으면 표도 생략
                        row_start += 3
                        continue

                    try:
                        img = ExcelImage(plot_path)
                        img.width = 400
                        img.height = 200
                        ws.add_image(img, f'A{current_row}')
                    except Exception as e:
                        ws.cell(row=current_row, column=1, value=f"{rej_group} 그래프 삽입 실패: {e}").font = Font(size=9, color="FF0000")

                    # ──────────────────────────────────────────────────
                    # 2. F열: 분석 텍스트 삽입
                    # ──────────────────────────────────────────────────
                    group_num = None
                    for i, (g, _) in enumerate(groups):
                        if rej_group.strip() in g.strip() or g.strip() in rej_group.strip():
                            group_num = i + 1
                            break

                    if group_num is None:
                        group_lines = [f"  1) 분석 없음"]
                    else:
                        group_key = f"{rej_group} 분석"
                        group_lines = [line for line in formatted_analysis if line.startswith(f"{group_num}. {group_key}")]

                    for i, line in enumerate(group_lines):
                        ws.cell(row=current_row + i, column=6, value=line).font = Font(size=9)

                    # ──────────────────────────────────────────────────
                    # 3. G열: 표 삽입
                    # ──────────────────────────────────────────────────
                    table_df = group_tables.get(rej_group)
                    if table_df is not None and not table_df.empty:
                        headers = ['MID_GROUP', '실적(%)', 'Ref(3개월)', 'Gap']
                        for c_idx, header in enumerate(headers, 8):  # G열 = 7
                            cell = ws.cell(row=current_row, column=c_idx, value=header)
                            cell.font = Font(bold=True, size=10)
                            cell.fill = PatternFill("solid", fgColor="D3D3D3")
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                         top=Side(style='thin'), bottom=Side(style='thin'))
                        
                        table_df_fmt = table_df.copy() # table_df 복사 후 포맷팅

                        # 실적(%), Ref(3개월), Gap 포맷팅
                        for col in ['실적(%)', 'Ref(3개월)', 'Gap']:
                            if col in table_df_fmt.columns:
                                table_df_fmt[col] = pd.to_numeric(table_df_fmt[col], errors='coerce') / 100.0 # % 컬럼을 소수형으로 변환 (실적(%), Ref(3개월), Gap)

                        for r_idx, row in enumerate(dataframe_to_rows(table_df_fmt, index=False, header=False), current_row + 1):
                            for c_idx, value in enumerate(row, 8):
                                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                                cell.font = Font(size=9)
                                cell.alignment = Alignment(horizontal='center', vertical='center')
                                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                             top=Side(style='thin'), bottom=Side(style='thin'))
                                if c_idx in [9,10,11]:
                                    cell.number_format = '0.00%'
                                if c_idx == 11:
                                    try:
                                        if isinstance(value, str):
                                            clean_val = value.replace('%', '').replace('+', '').strip()
                                            gap_val = float(clean_val) if clean_val else 0.0
                                        else:
                                            gap_val = float(value)
                                        if gap_val > 0:
                                            cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                            cell.font = Font(color="FF0000", bold=True, size=9)
                                        elif gap_val < 0:
                                            cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                            cell.font = Font(color="0000FF", bold=True, size=9)
                                    except:
                                        pass

                        # 표 높이 기준으로 다음 시작 위치 결정
                        table_height = len(table_df) + 1
                    else:
                        ws.cell(row=current_row, column=8, value=f"{rej_group} 표 없음").font = Font(size=9, color="FF0000")
                        table_height = 1

                    row_start = current_row + max(len(group_lines), table_height) + 5 

            # ──────────────────────────────────────────────────
            # 5. [RC/HG 보상 영향성 분석] 섹션
            # ──────────────────────────────────────────────────
            ws['A65'] = "[ RC/HG 보상 영향성 분석 ]"
            ws['A65'].font = Font(size=12, bold=True)
            ws['A65'].alignment = Alignment(horizontal='left')

            current_date = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
            debug_dir = PROJECT_ROOT / "daily_reports_debug" / current_date

            # 전체 그래프 파일 경로
            total_chart_path = debug_dir / "RC_HG_보상_전체.png"

            # 그룹별 그래프 파일 경로
            group_chart_paths = {
                'PARTICLE': debug_dir / "RC_HG_보상_PARTICLE.png",
                'FLATNESS': debug_dir / "RC_HG_보상_FLATNESS.png",
                'WARP&BOW': debug_dir / "RC_HG_보상_WARP&BOW.png",
                'NANO': debug_dir / "RC_HG_보상_NANO.png"
            }

            # 표 데이터는 report에서 가져옴 (이건 유지)
            data_3210_details = report.get('DATA_LOT_3210_wafering_300_details', {})
            loss_rate_table_total = data_3210_details.get('summary')  # DataFrame
            loss_rate_table_by_group = data_3210_details.get('loss_rate_table_by_group', {})  # dict of DataFrame

            current_row = 66  # A65 다음 행
            SECTION_HEIGHT = 9  # 그래프 + 표 포함 고정 간격 (행 단위)


            # 안전한 % → float 변환 함수 (전역 사용)
            def safe_pct_to_float(x):
                try:
                    if pd.isna(x) or x == '' or x is None:
                        return 0.0
                    cleaned = str(x).strip().replace('%', '').replace('+', '').replace('-', '')
                    if cleaned == '':
                        return 0.0
                    return float(cleaned) / 100.0
                except:
                    return 0.0

            # 1. 전체 그래프 + 표
            if total_chart_path.exists():
                # 그래프 삽입 (A열)
                try:
                    img = ExcelImage(str(total_chart_path))
                    img.width = 400
                    img.height = 200
                    ws.add_image(img, f'A{current_row}')
                except Exception as e:
                    ws[f'A{current_row}'] = f"[RC/HG 전체 그래프 삽입 실패: {e}]"
                    ws[f'A{current_row}'].font = Font(size=10, color="FF0000")

                # 표 삽입 (H열) → 기존 방식 그대로
                if isinstance(loss_rate_table_total, pd.DataFrame) and not loss_rate_table_total.empty:
                    headers = ['구분', 'Ref.(3개월)', '일', 'Ref.(3개월)%', '일%', 'Gap']
                    start_row = current_row + 1 # 그래프 아래 6행 여유
                    start_col = 8

                    # 헤더 삽입
                    for c_idx, header in enumerate(headers, start_col):
                        cell = ws.cell(row=start_row, column=c_idx, value=header)
                        cell.font = Font(bold=True, size=10)
                        cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                    top=Side(style='thin'), bottom=Side(style='thin'))


                    # 데이터 포맷팅: % 컬럼을 소수형으로 변환
                    table_total_fmt = loss_rate_table_total.copy()
                    pct_columns = ['Ref.(3개월)%', '일%', 'Gap']
                    for col in pct_columns:
                        if col in table_total_fmt.columns:
                            table_total_fmt[col] = table_total_fmt[col].apply(safe_pct_to_float)

                    # 데이터 행 삽입
                    for r_idx, row in enumerate(dataframe_to_rows(table_total_fmt, index=False, header=False), start_row + 1):
                        for c_idx, value in enumerate(row, start_col):
                            cell = ws.cell(row=r_idx, column=c_idx, value=value)
                            cell.font = Font(size=9)
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))

                            # Ref.(3개월)%, 일%, Gap 컬럼에 number_format 적용 (J, K, L열)
                            if c_idx in [11, 12, 13]:  # J, K, L열
                                cell.number_format = '0.00%'

                            # Gap 열 색상 강조
                            if c_idx == 13 :
                                try:
                                    gap_val = float(value) if pd.notna(value) else 0.0
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=True, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=True, size=9)
                                except:
                                    pass

                    # 행 높이 조정
                    for row in range(start_row, start_row + len(loss_rate_table_total) + 1):
                        ws.row_dimensions[row].height = 18

                else:
                    ws.cell(row=current_row + 2, column=8, value="[RC/HG 전체 표 없음]").font = Font(size=10, color="FF0000")
          
                current_row += SECTION_HEIGHT  # 다음 섹션으로

            # 2. 그룹별 그래프 + 표 (PARTICLE, FLATNESS, WARP&BOW 순서)
            for group in ['PARTICLE', 'FLATNESS', 'WARP&BOW', 'NANO']:
                chart_path = group_chart_paths[group]

                if chart_path.exists():
                    # 그래프 삽입 (A열)
                    try:
                        img = ExcelImage(str(chart_path))
                        img.width = 400
                        img.height = 200
                        ws.add_image(img, f'A{current_row}')
                    except Exception as e:
                        ws[f'A{current_row}'] = f"[RC/HG {group} 그래프 삽입 실패: {e}]"
                        ws[f'A{current_row}'].font = Font(size=10, color="FF0000")

                #  표 삽입 (H열) → 기존 방식 그대로
                table_data = loss_rate_table_by_group.get(group)

                # 타입 및 유효성 검사
                if not isinstance(table_data, pd.DataFrame):
                    ws.cell(row=current_row + 6, column=8, value=f"[{group} 표: 유효하지 않은 형식]").font = Font(size=10, color="FF0000")
                    current_row += SECTION_HEIGHT
                    continue

                if table_data.empty:
                    ws.cell(row=current_row + 6, column=8, value=f"[{group} 표 없음]").font = Font(size=10, color="FF0000")
                    current_row += SECTION_HEIGHT
                    continue

                headers = ['구분', 'Ref.(3개월)', '일', 'Ref.(3개월)%', '일%', 'Gap']
                start_row = current_row + 3
                start_col = 8

                # 헤더 삽입
                for c_idx, header in enumerate(headers, start_col):
                    cell = ws.cell(row=start_row, column=c_idx, value=header)
                    cell.font = Font(bold=True, size=10)
                    cell.fill = PatternFill("solid", fgColor="D3D3D3")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                top=Side(style='thin'), bottom=Side(style='thin'))

                # 데이터 포맷팅: % 컬럼을 소수형으로 변환
                table_group_fmt = table_data.copy()
                pct_columns = ['Ref.(3개월)%', '일%', 'Gap']
                for col in pct_columns:
                    if col in table_group_fmt.columns:
                        table_group_fmt[col] = table_group_fmt[col].apply(safe_pct_to_float)

                # 데이터 삽입 (dataframe_to_rows 사용)
                for r_idx, row in enumerate(dataframe_to_rows(table_group_fmt, index=False, header=False), start_row + 1):
                    for c_idx, value in enumerate(row, start_col):
                        cell = ws.cell(row=r_idx, column=c_idx, value=value)
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                    top=Side(style='thin'), bottom=Side(style='thin'))

                        if c_idx in [11, 12, 13]:  # J, K, L열
                            cell.number_format = '0.00%'

                        # Gap 열 색상 강조
                        if c_idx == 13:
                            try:
                                gap_val = float(value) if pd.notna(value) else 0.0
                                if gap_val > 0:
                                    cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                    cell.font = Font(color="FF0000", bold=True, size=9)
                                elif gap_val < 0:
                                    cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                    cell.font = Font(color="0000FF", bold=True, size=9)
                            except:
                                pass

                # 행 높이 조정
                for row in range(start_row, start_row + len(table_data) + 1):
                    ws.row_dimensions[row].height = 18

                current_row += SECTION_HEIGHT  # 고정 간격 유지

            # ──────────────────────────────────────────────────
            # 6. [ 제품 영향성 분석 ] 섹션
            # ──────────────────────────────────────────────────
            current_row = current_row + 1
            ws[f'A{current_row}'] = "[ 제품 영향성 분석 ]"
            ws[f'A{current_row}'].font = Font(size=12, bold=True)
            current_row += 1

            # 데이터 가져오기
            product_influence_gap = report.get('product_influence_gap')

            # 대상 REJ_GROUP 확인
            top3_rej_groups = report.get('DATA_3210_wafering_300_details', {}).get('top3_rej_groups', [])
            target_rej_groups = ['PARTICLE', 'FLATNESS', 'NANO', 'WARP&BOW', 'GROWING', 'SCRATCH', 'VISUAL', 'SAMPLE']
            valid_rej_groups = [g for g in top3_rej_groups if g in target_rej_groups]

            if not isinstance(product_influence_gap, pd.DataFrame) or product_influence_gap.empty:
                ws.cell(row=current_row, column=8, value="[제품 영향성 분석: 데이터 없음]").font = Font(size=10, color="FF0000")
                current_row += 10
            elif not valid_rej_groups:
                ws.cell(row=current_row, column=8, value="[제품 영향성 분석: 대상 그룹 없음]").font = Font(size=10, color="FF0000")
                current_row += 10
            else:
                rej_group = valid_rej_groups[0]
                df_group = product_influence_gap[product_influence_gap['REJ_GROUP'] == rej_group]

                if df_group.empty:
                    ws.cell(row=current_row, column=8, value=f"[{rej_group} 데이터 없음]").font = Font(size=10, color="FF0000")
                    current_row += 10
                else: 
                    # 그래프 1: 물량비_불량GAP
                    chart1_path = debug_dir / f"{rej_group}_물량비_불량GAP_temp.png"
                    try:
                        fig1, ax1 = plt.subplots(figsize=(6, 4))
                        x = []
                        y = []
                        for _, row in df_group.iterrows():
                            x.append(str(row['PRODUCT_TYPE']))  # str
                            val = pd.to_numeric(row['물량비_불량GAP'], errors='coerce')
                            y.append(float(val) if pd.notna(val) else 0.0)  # float (Python 기본 타입)

                        if len(x) == 0 or len(y) == 0:
                           raise ValueError("데이터 없음")
                        ax1.bar(x, y, color='orange')
                        ax1.set_title(f'{rej_group} 제품 Ref. 물량 비 불량 변동', fontsize=12, fontweight='bold')
                        ax1.set_xlabel('제품', fontsize=10)
                        ax1.set_ylabel('물량비_불량GAP', fontsize=10)
                        ax1.tick_params(axis='x', rotation=0)
                        ax1.grid(axis='y', linestyle='--', alpha=0.7)
                        plt.tight_layout()
                        plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
                        plt.close()

                        if chart1_path.exists():
                            img1 = ExcelImage(str(chart1_path))
                            img1.width = 400
                            img1.height = 200
                            ws.add_image(img1, f'A{current_row}') #A열에 삽입

                    except Exception as e:
                        ws[f'A{current_row}'] = f"[그래프1 생성 실패: {e}]"
                        ws[f'A{current_row}'].font = Font(size=10, color="FF0000")


                    # 그래프 2: 물량비_GAP(%)
                    chart2_path = debug_dir / f"{rej_group}_물량비_GAP_temp.png"
                    try:
                        fig2, ax2 = plt.subplots(figsize=(6, 4))
                        x = []
                        y = []
                        for _, row in df_group.iterrows():
                            x.append(str(row['PRODUCT_TYPE']))
                            val = pd.to_numeric(row['물량비_GAP(%)'], errors='coerce')
                            y.append(float(val) if pd.notna(val) else 0.0)

                        if len(x) == 0 or len(y) == 0:
                            raise ValueError("데이터 없음")
                        ax2.bar(x, y, color='orange')
                        ax2.set_title(f'{rej_group} 제품 Ref. 비 물량 변동', fontsize=12, fontweight='bold')
                        ax2.set_xlabel('제품', fontsize=10)
                        ax2.set_ylabel('물량비_GAP(%)', fontsize=10)
                        ax2.tick_params(axis='x', rotation=0)
                        ax2.grid(axis='y', linestyle='--', alpha=0.7)
                        plt.tight_layout()
                        plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
                        plt.close()

                        if chart2_path.exists():
                            img2 = ExcelImage(str(chart2_path))
                            img2.width = 400
                            img2.height = 200
                            ws.add_image(img2, f'F{current_row}')

                    except Exception as e:
                        ws[f'F{current_row}'] = f"[그래프2 생성 실패: {e}]"
                        ws[f'F{current_row}'].font = Font(size=10, color="FF0000")

                    current_row += 8

                    # 표 삽입
                    headers = ['제품', 'Ref. 제품 불량률', '물량比 불량 Gap', '물량비 Gap', 'Ref.(6개월) 수량', '일 수량', 'Ref.(6개월) 물량비', '일 물량비']
                    start_row = current_row +2
                    start_col = 1

                    # 헤더 삽입
                    for c_idx, header in enumerate(headers, start_col):
                        cell = ws.cell(row=start_row, column=c_idx, value=header)
                        cell.font = Font(bold=True, size=10)
                        cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                    top=Side(style='thin'), bottom=Side(style='thin'))

                    table_data = [] # df_group에서 필요한 컬럼만 추출하여 새 테이블 생성
                    for _, row in df_group.iterrows():
                        table_data.append({
                            '제품': row['PRODUCT_TYPE'],
                            'Ref. 제품 불량률': row['Ref_불량률(%)'],           # Ref. 제품 불량률
                            '물량比 불량 Gap': row['물량비_불량GAP'],            # 불량률 차이
                            '물량비 Gap': row['물량비_GAP(%)'],                 # 물량비 차이
                            'Ref.(6개월) 수량': row['Ref_Compile_수량'],             # 6개월 Compile 수량
                            '일 수량': row['Daily_Compile_수량'],                    # 금일 Compile 수량
                            'Ref.(6개월) 물량비': row['Ref_물량비(%)'],                # Ref 물량비 (%)
                            '일 물량비': row['Daily_물량비(%)']                        # 금일 물량비 (%)
                        })
                    
                    table_df = pd.DataFrame(table_data, columns=headers)  # 컬럼 순서 보장

                    table_df_fmt = table_df.copy()
                    pct_columns = ['Ref. 제품 불량률', '물량比 불량 Gap', '물량비 Gap', 'Ref.(6개월) 물량비', '일 물량비']
                    for col in pct_columns:
                        if col in table_df_fmt.columns:
                            # 이미 숫자형이므로, % 표시를 위해 100으로 나눔
                            table_df_fmt[col] = pd.to_numeric(table_df_fmt[col], errors='coerce') / 100.0


                    # 데이터 삽입
                    for r_idx, row in enumerate(dataframe_to_rows(table_df_fmt, index=False, header=False), start_row + 1):
                        for c_idx, value in enumerate(row, start_col):
                            if isinstance(value, (np.integer, np.int64)):
                                value = int(value)
                            elif isinstance(value, (np.floating, np.float64)):
                                value = float(value)
                            elif isinstance(value, (np.bool_, bool)):
                                value = bool(value)
                            elif pd.isna(value):
                                value = None
                            cell = ws.cell(row=r_idx, column=c_idx, value=value)
                            cell.font = Font(size=9)
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))

                            if c_idx in [11,12,13,14,15]:  # K, L열
                                cell.number_format = '0.00%'

                            if c_idx == 11:  # 물량비_불량GAP
                                try:
                                    gap_val = float(value) if pd.notna(value) else 0.0
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=True, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=True, size=9)
                                except:
                                    pass

                    for row in range(start_row, start_row + len(table_data) + 1):
                        ws.row_dimensions[row].height = 18

                    current_row += len(table_data) + 3


            # ──────────────────────────────────────────────────
            # 6. 열 너비 조정
            # ──────────────────────────────────────────────────
            ws.column_dimensions['A'].width = 12
            ws.column_dimensions['B'].width = 12
            ws.column_dimensions['C'].width = 12
            ws.column_dimensions['D'].width = 12
            ws.column_dimensions['E'].width = 12
            ws.column_dimensions['F'].width = 12  
            ws.column_dimensions['G'].width = 12
            ws.column_dimensions['H'].width = 12
            ws.column_dimensions['I'].width = 12
            ws.column_dimensions['J'].width = 12

            # ──────────────────────────────────────────────────
            # 6. 상세분석 텍스트 (A38 부터)
            # ──────────────────────────────────────────────────
            start_detail_row = 38
            for i, line in enumerate(detailed_analysis):
                ws.cell(row=start_detail_row + i, column=6, value=line).font = Font(size=10)

            # ──────────────────────────────────────────────────
            # 7. 저장
            # ──────────────────────────────────────────────────
            wb.save(str(excel_path))
            print(f"Excel 저장 성공: {excel_path}")

            if not Path(excel_path).exists():
                raise RuntimeError(f"저장 완료했지만 파일이 존재하지 않음: {excel_path}")

            return str(excel_path)

        except Exception as e:
            print(f"Excel 생성 실패: {repr(e)}")
            raise
