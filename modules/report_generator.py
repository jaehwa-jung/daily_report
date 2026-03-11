import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import traceback
import os
from datetime import datetime, timedelta
import logging
from logging import FileHandler
import matplotlib
from pathlib import Path
import base64
from analysis.defect_analyzer import analyze_flatness, analyze_warp, analyze_growing, analyze_broken, analyze_nano, analyze_pit, analyze_scratch, analyze_chip, analyze_edge, analyze_HUMAN_ERR, analyze_VISUAL, analyze_NOSALE, analyze_OTHER, analyze_GR, analyze_sample,analyze_particle
from config.mappings import REJ_GROUP_TO_MID_MAPPING
import tempfile
from inspect import signature
import re
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from matplotlib.ticker import PercentFormatter

# =========================================================
# Excel Export Utilities (공통화: 이미지/표/시트 빌더)
# - PNG 파일 I/O 최소화: (가능하면) 메모리 이미지로 처리
# - DataFrame 표 생성 공통화
# - 커서 기반 SheetBuilder 제공
# =========================================================
from io import BytesIO

try:
    from PIL import Image as PILImage  # optional (권장)
except Exception:  # pragma: no cover
    PILImage = None


# ---- 공통 스타일 상수 (재사용) ----
_THIN = Side(style="thin")
_BORDER_THIN = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)

HEADER_FONT = Font(bold=True, size=10)
BODY_FONT = Font(size=9)
CENTER_WRAP = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)

HEADER_FILL = PatternFill("solid", fgColor="D3D3D3")


def add_png_image(ws, image_path, anchor, width=None, height=None):
    """PNG 파일 경로를 openpyxl Image로 삽입 (공통)."""
    img = ExcelImage(str(image_path))
    if width is not None:
        img.width = width
    if height is not None:
        img.height = height
    ws.add_image(img, anchor)
    return img


def fig_to_excel_image(fig, width=None, height=None, dpi=150):
    """matplotlib figure를 메모리(PNG)로 렌더링 후 ExcelImage로 변환."""
    bio = BytesIO()
    fig.savefig(bio, format="png", dpi=dpi, bbox_inches="tight")
    bio.seek(0)

    # openpyxl Image는 경로 또는 PIL Image를 받을 수 있음 (버전에 따라 다름)
    if PILImage is not None:
        pil_img = PILImage.open(bio)
        img = ExcelImage(pil_img)
    else:
        # PIL이 없으면 BytesIO를 임시파일로 처리하는 fallback
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp.write(bio.getvalue())
            tmp_path = tmp.name
        img = ExcelImage(tmp_path)

    if width is not None:
        img.width = width
    if height is not None:
        img.height = height
    return img


def write_df_table(
    ws,
    df,
    start_row,
    start_col,
    *,
    header_fill=HEADER_FILL,
    header_font=HEADER_FONT,
    body_font=BODY_FONT,
    alignment=CENTER_WRAP,
    border=_BORDER_THIN,
    number_formats=None,  # 예: {"월 목표": "0.00%", "Gap(월)": "0.00%"}
):
    """DataFrame을 표 형태로 시트에 쓰는 공통 함수."""
    if df is None or getattr(df, "empty", True):
        ws.cell(start_row, start_col, "표 없음").font = Font(size=10, color="FF0000")
        return start_row + 1

    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), start_row):
        for c_offset, value in enumerate(row):
            c_idx = start_col + c_offset
            cell = ws.cell(row=r_idx, column=c_idx, value=value)

            cell.border = border
            cell.alignment = alignment
            if r_idx == start_row:
                cell.font = header_font
                cell.fill = header_fill
            else:
                cell.font = body_font

                if number_formats:
                    col_name = df.columns[c_offset] if c_offset < len(df.columns) else None
                    fmt = number_formats.get(col_name) if col_name else None
                    if fmt:
                        cell.number_format = fmt

    return start_row + len(df) + 1


class SheetBuilder:
    """시트에 순차적으로 내용을 쌓아가는 커서 기반 빌더."""
    def __init__(self, ws, start_row=1, start_col=1):
        self.ws = ws
        self.row = start_row
        self.col = start_col

    def blank(self, n=1):
        self.row += n
        return self

    def title(self, text, merge_from=None, merge_to=None, font=None, alignment=LEFT):
        if font is None:
            font = Font(size=14, bold=True)
        if merge_from and merge_to:
            self.ws.merge_cells(f"{merge_from}:{merge_to}")
            cell = self.ws[merge_from]
        else:
            cell = self.ws.cell(self.row, self.col)
        cell.value = text
        cell.font = font
        cell.alignment = alignment
        if not (merge_from and merge_to):
            self.row += 1
        return self

    def image_from_path(self, image_path, anchor, width=None, height=None):
        add_png_image(self.ws, image_path, anchor, width=width, height=height)
        return self

    def table(self, df, start_row=None, start_col=None, **kwargs):
        if start_row is None:
            start_row = self.row
        if start_col is None:
            start_col = self.col
        self.row = write_df_table(self.ws, df, start_row, start_col, **kwargs)
        return self



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
    _ms6_mapping_cache = None

    def __init__(self, data):
        self.data = data

    def _calculate_product_influence(self, df, target_rej_groups):
        """제품 영향성 공통 집계 로직 (Ref/Daily 공통)"""
        if df is None or getattr(df, "empty", True):
            return pd.DataFrame()

        df = df.copy()

        # 숫자 컬럼 타입 보정
        for col in ["IN_QTY", "LOSS_QTY"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            else:
                df[col] = 0

        # 1) 불량개수
        df_defect = df[
            df["REJ_GROUP"].isin(target_rej_groups) & (df["PRODUCT_TYPE"] != "Unknown")
        ].copy()

        if df_defect.empty:
            return pd.DataFrame()

        defect_summary = (
            df_defect.groupby(["REJ_GROUP", "PRODUCT_TYPE"], dropna=False)["LOSS_QTY"]
            .sum()
            .reset_index()
        )
        defect_summary.rename(columns={"LOSS_QTY": "불량개수"}, inplace=True)

        # 2) Compile 수량 (분모)
        df_denom = df[(df["REJ_GROUP"] == "분모") & (df["PRODUCT_TYPE"] != "Unknown")].copy()
        if df_denom.empty:
            return pd.DataFrame()

        compile_summary = (
            df_denom.groupby("PRODUCT_TYPE", dropna=False)["IN_QTY"]
            .sum()
            .reset_index()
        )
        compile_summary.rename(columns={"IN_QTY": "Compile_수량"}, inplace=True)

        total_volume = compile_summary["Compile_수량"].sum()
        if total_volume == 0:
            compile_summary["물량비(%)"] = 0.0
        else:
            compile_summary["물량비(%)"] = (compile_summary["Compile_수량"] / total_volume * 100).round(2)

        # 3) 병합
        result = pd.merge(defect_summary, compile_summary, on="PRODUCT_TYPE", how="left")

        # 4) 불량률 계산
        if total_volume == 0:
            result["불량률(%)"] = 0.0
            result["전체 불량률(%)"] = 0.0
        else:
            result["불량률(%)"] = ((result["불량개수"] / result["Compile_수량"]) * 100).round(2)
            result["전체 불량률(%)"] = ((result["불량개수"] / total_volume) * 100).round(2)

        # 5) 최종 정리
        result = result[
            ["REJ_GROUP", "PRODUCT_TYPE", "불량개수", "Compile_수량", "불량률(%)", "전체 불량률(%)", "물량비(%)"]
        ].sort_values(["REJ_GROUP", "불량률(%)"], ascending=[True, False])

        return result

    
    # ──────────────────────────────────────────────────
    # [신규] MS6.csv 기반 제품 정보 병합 함수
    # ──────────────────────────────────────────────────
    @classmethod
    def _load_ms6_mapping(cls):
        """MS6.csv에서 (MS6 -> 제품1) 매핑을 1회만 로드하여 캐시"""
        if cls._ms6_mapping_cache is not None:
            return cls._ms6_mapping_cache

        project_root = Path(__file__).parent.parent
        ms6_path = project_root / "queries" / "MS6.csv"

        if not ms6_path.exists():
            cls._ms6_mapping_cache = {}
            return cls._ms6_mapping_cache

        try:
            df_ms6 = pd.read_csv(ms6_path, dtype=str, encoding="utf-8")
        except UnicodeDecodeError:
            df_ms6 = pd.read_csv(ms6_path, dtype=str, encoding="cp949")

        if "MS6" not in df_ms6.columns or "제품1" not in df_ms6.columns:
            cls._ms6_mapping_cache = {}
            return cls._ms6_mapping_cache

        df_ms6 = df_ms6.dropna(subset=["MS6", "제품1"]).copy()
        cls._ms6_mapping_cache = dict(
            zip(df_ms6["MS6"].astype(str).str.strip(), df_ms6["제품1"].astype(str).str.strip())
        )
        return cls._ms6_mapping_cache

    def _merge_product_type(self, df):
        """df에 PROD_ID 기준으로 MS6.csv의 '제품1' 컬럼을 병합하여 'PRODUCT_TYPE' 추가 (캐시 적용)"""
        if df is None or getattr(df, "empty", True):
            # empty or None
            try:
                df = df.copy()
            except Exception:
                df = pd.DataFrame()
            df["PRODUCT_TYPE"] = "Unknown"
            return df

        if "PROD_ID" not in df.columns:
            df = df.copy()
            df["PRODUCT_TYPE"] = "Unknown"
            return df

        mapping = self._load_ms6_mapping()
        df = df.copy()
        df["MS6"] = df["PROD_ID"].astype(str).str[:6]
        df["PRODUCT_TYPE"] = df["MS6"].map(mapping).fillna("Unknown")
        return df

    def _get_top3_rej_groups(self):
        """
        안전하게 상위 3개 REJ_GROUP 목록 가져오기
        """
        return self.data.get('DATA_3210_wafering_300', {}).get('top3_rej_groups', [])


    def _create_product_influence_ref(self):
        """전 반기(6개월) 데이터 기반 제품 영향성 Ref 데이터 생성"""
        PROJECT_ROOT = Path(__file__).parent.parent
        cache_dir = PROJECT_ROOT / "data_cache"
        pattern = "DATA_LOT_3210_wafering_300_*.parquet"
        parquet_files = list(cache_dir.glob(pattern))

        # 대상 월 설정: 202506 ~ 202512
        target_months = [f"2025{str(m).zfill(2)}" for m in range(6, 13)]

        target_rej_groups = ["PARTICLE", "FLATNESS", "NANO", "WARP&BOW", "GROWING", "SCRATCH", "VISUAL", "SAMPLE"]

        df_list = []
        for file_path in parquet_files:
            try:
                stem = file_path.stem
                date_part = stem.split("_")[-1]
                if len(date_part) == 6 and date_part.isdigit() and date_part in target_months:
                    df_part = pd.read_parquet(file_path)
                    df_part = self._merge_product_type(df_part)

                    for col in ["IN_QTY", "LOSS_QTY"]:
                        if col in df_part.columns:
                            df_part[col] = pd.to_numeric(df_part[col], errors="coerce").fillna(0).astype("int64")
                        else:
                            df_part[col] = 0

                    if "PRODUCT_TYPE" not in df_part.columns:
                        continue
                    df_list.append(df_part)
            except Exception:
                continue

        if not df_list:
            return pd.DataFrame()

        df_full = pd.concat(df_list, ignore_index=True)

        if "PRODUCT_TYPE" not in df_full.columns:
            return pd.DataFrame()

        return self._calculate_product_influence(df_full, target_rej_groups)

    def _create_product_influence_daily(self):
        """금일 DATA_LOT_3210_wafering_300 데이터 기반 제품 영향성 분석"""
        key = "DATA_LOT_3210_wafering_300"
        if key not in self.data or self.data[key].empty:
            return pd.DataFrame()

        df = self.data[key].copy()

        if "PRODUCT_TYPE" not in df.columns:
            df = self._merge_product_type(df)
            if "PRODUCT_TYPE" not in df.columns:
                return pd.DataFrame()

        target_rej_groups = ["PARTICLE", "FLATNESS", "NANO", "WARP&BOW", "GROWING", "SCRATCH", "VISUAL", "SAMPLE"]
        return self._calculate_product_influence(df, target_rej_groups)

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
        # print(f"결과 (총 {len(final_result)} 건):\n{final_result}")

        return final_result  

    def generate(self):
        """데일리 리포트 생성"""
        try:
            logger.info("리포트 생성 시작")
            # ==================================================================
            # 모든 데이터에 PRODUCT_TYPE 일괄 병합 (가장 먼저 실행)
            # ===================================================================

            for key in ['DATA_LOT_3210_wafering_300', 'DATA_WAF_3210_wafering_300']:
                if key in self.data and not self.data[key].empty:
                    self.data[key] = self._merge_product_type(self.data[key])
                    if 'PRODUCT_TYPE' in self.data[key].columns:
                        sample = self.data[key].sample(1)[['PROD_ID', 'PRODUCT_TYPE']].to_dict('records')
                else:
                    print(f"{key} 없거나 빈 데이터")


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

            data_lot_details = self._create_DATA_LOT_3210_wafering_300()
            data_waf_details = self._create_DATA_WAF_3210_wafering_300()

            # DATA_1511_SMAX_wafering_300 = self.data.get('DATA_1511_SMAX_wafering_300')
            self._create_DATA_1511_SMAX_wafering_300()
            self._calculate_loss_rate_by_process() # LOSS_RATE 계산(smax 1511 보고서 사용)

            self._plot_rej_group_top3_eqp_trend(output_dir="./daily_reports_debug")

            report = {
                'DATA_3010_wafering_300' : data_3010_details,
                'DATA_3210_wafering_300_details': data_3210_details,
                'DATA_3210_wafering_300_3months': data_3210_3months,
                'DATA_LOT_3210_wafering_300_details': data_lot_details,
                'DATA_WAF_3210_wafering_300_details': data_waf_details,
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
            
            # 빈 값 또는 이상한 값 필터링
            if pd.isna(raw) or raw in ['', 'None', 'nan', 'NaT']:
                return pd.NaT

            try:
                if item_type in ['월실적', '월사업계획']:
                    return pd.to_datetime(raw, format='%Y-%m', errors='coerce')
                else:
                    return pd.to_datetime(raw, errors='coerce')
            except:
                return pd.NaT

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
            print(f"[DEBUG] 조회: item_type={item_type}, target_date={target_date}")
            
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
                latest_sorted = latest.sort_values('dt_range', ascending=False)
                return latest_sorted.iloc[0]
            
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

        # ──────────────────────────────────────────────────
        # 5. 일 목표: 어제 기준 → 없으면 최신
        # ──────────────────────────────────────────────────
        daily_plan_row = get_latest_or_target(df, '일사업계획', target_date)
        if daily_plan_row is not None:
            daily_plan_val = float(daily_plan_row['rate'])
            daily_plan_date = daily_plan_row['dt_range'].strftime('%Y-%m-%d')
        else:
            daily_plan_val = 0.0

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
        bar_width = 0.32

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
        def autolabel(rects, values, color='black', fontsize=10):
            for i, rect in enumerate(rects):
                height = rect.get_height()
                ax.text(
                    rect.get_x() + rect.get_width() / 2.,  # 막대 중앙
                    height + 0.08,                         # 막대 바로 위 (약간 높이)
                    f'{values[i]:.2f}%',                   # 값 표시
                    ha='center', va='bottom',               # 수평 중앙, 수직 아래
                    fontsize=fontsize, fontweight='bold', color=color
                )

        autolabel([bar1[0]], [monthly_values[0]], 'black', fontsize=10)
        autolabel([bar2[0]], [monthly_values[1]], 'black', fontsize=10)
        autolabel([bar3[0]], [daily_values[0]], 'black', fontsize=10)
        autolabel([bar4[0]], [daily_values[1]], 'black', fontsize=10)

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
            fontsize=11, fontweight='bold', color=monthly_gap_color,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.9)
        )
        ax.text(
            gap_x[1], gap_y[1],
            f'{daily_gap:+.2f}%',
            ha='center', va='bottom',  # 수평/수직 중앙
            fontsize=11, fontweight='bold', color=daily_gap_color,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.9)
        )

        # 그리드
        ax.grid(axis='y', linestyle='--', alpha=0.7, zorder=0)

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
            'Gap(월)': [monthly_plan_val - monthly_actual_val],
            'Gap(일)': [daily_plan_val - daily_actual_val],
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
            df_wafer = self.data.get('DATA_WAF_3210_wafering_300')
            df_lot = self.data.get('DATA_LOT_3210_wafering_300')

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

                analyzer_func = REJ_GROUP_TO_ANALYZER[rej]

                # 함수 시그니처 기반 자동 인자 바인딩
                sig = signature(analyzer_func)
                params = sig.parameters

                # 정밀 인자 준비
                bound_args = {}
                missing = []

                for param_name, param in params.items():
                    if 'wafer' in param_name.lower():
                        if df_wafer is not None:
                            df_target = df_wafer[df_wafer['REJ_GROUP'] == rej].copy()
                            bound_args[param_name] = df_target
                        else:
                            missing.append(param_name)
                    elif 'lot' in param_name.lower():
                        if df_lot is not None:
                            bound_args[param_name] = df_lot
                        else:
                            missing.append(param_name)
                    else:
                        # 기본값 있으면 건너뜀
                        if param.default != param.empty:
                            continue
                        else:
                            missing.append(param_name)

                if missing:
                    error_msg = f"[{rej} 분석] 필수 인자 누락: {missing}"
                    detailed_analysis.append(error_msg)
                    continue

                try:
                    result = analyzer_func(**bound_args)
                    detailed_analysis.extend(result)
                except Exception as e:
                    error_msg = f"[{rej} 분석] 실행 오류: {e}"
                    detailed_analysis.append(error_msg)

                # detailed_analysis.extend(result)

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
        print(f"✅ [DEBUG 0] 원본 데이터 행 수: {len(df)}")
        print(f"✅ [DEBUG 0] 원본 데이터 컬럼: {df.columns.tolist()}")

        # 컬럼 타입 변환
        numeric_cols = ['LOSS_QTY', 'MGR_QTY']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 2. 분모 계산: BASE_DT_NM 기준 MGR_QTY 중복 제거 후 전체 합계
        #    (동일 일자의 MGR_QTY 는 동일하므로, 일자별 고유 값의 합 = 기간 총 투입량)
        mgr_qty_daily = df[['BASE_DT_NM', 'MGR_QTY']].drop_duplicates(subset=['BASE_DT_NM', 'MGR_QTY'])
        total_mgr_qty = mgr_qty_daily['MGR_QTY'].sum()
        print(f"✅ [DEBUG 1] 일자별 MGR_QTY 행 수: {len(mgr_qty_daily)}, 총 투입량: {total_mgr_qty:,.0f}")


        if total_mgr_qty == 0:
            print("기간 내 MGR_QTY 합계가 0 입니다.")
            return details

        print(f"✅ [DEBUG 2] REJ_GROUP 종류: {df['REJ_GROUP'].unique()}")
        print(f"✅ [DEBUG 2] AFT_BAD_RSN_CD 샘플: {df['AFT_BAD_RSN_CD'].unique()[:5]}")


        # 3. 분자 계산: AFT_BAD_RSN_CD 별 LOSS_QTY 전체 합계
        summary_list = []
        for rej_group, group_df in df.groupby('REJ_GROUP', dropna=False):
            # 해당 REJ_GROUP의 매핑 가져오기
            mid_mapping = REJ_GROUP_TO_MID_MAPPING.get(rej_group, {})
            
            # AFT_BAD_RSN_CD 기준으로 MID_GROUP 생성
            group_df = group_df.copy()
            group_df['MID_GROUP'] = group_df['AFT_BAD_RSN_CD'].map(mid_mapping)
            
            #  매핑되지 않은 경우: 원래 AFT_BAD_RSN_CD 값 유지
            group_df['MID_GROUP'] = group_df['MID_GROUP'].fillna(group_df['AFT_BAD_RSN_CD'])

            # [DEBUG 3] MID_GROUP 생성 후 null 확인
            null_count = group_df['MID_GROUP'].isnull().sum()
            if null_count > 0:
                print(f"⚠️ [DEBUG 3] {rej_group} 에서 MID_GROUP null {null_count}개 발생")
            # [DEBUG 3] MID_GROUP 생성 후 null 확인

            # 그룹 집계: REJ_GROUP + MID_GROUP + AFT_BAD_RSN_CD
            agg_df = group_df.groupby(['REJ_GROUP', 'MID_GROUP', 'AFT_BAD_RSN_CD'], dropna=False).agg(
                TOTAL_LOSS_QTY=('LOSS_QTY', 'sum'),
                COUNT_DAYS=('BASE_DT_NM', 'nunique')
            ).reset_index()

            # [DEBUG 4] 집계 후 LOSS_QTY 합계 확인
            group_loss_sum = agg_df['TOTAL_LOSS_QTY'].sum()
            print(f"✅ [DEBUG 4] {rej_group} 집계된 총 불량량: {group_loss_sum:,.0f}")
            # [DEBUG 4] 집계 후 LOSS_QTY 합계 확인

            # 4. LOSS_RATIO 계산: 분자 (TOTAL_LOSS_QTY) / 분모 (total_mgr_qty)
            agg_df['AVG_LOSS_RATIO'] = agg_df['TOTAL_LOSS_QTY'] / total_mgr_qty
            agg_df['LOSS_RATIO_PCT'] = (agg_df['AVG_LOSS_RATIO'] * 100).round(2)

            # 참조용: 분모 정보 저장 (검증용)
            agg_df['TOTAL_MGR_QTY'] = total_mgr_qty

            summary_list.append(agg_df)

        # 전체 요약 병합
        summary_3months = pd.concat(summary_list, ignore_index=True)
        summary_3months['LOSS_RATIO_PCT'] = (summary_3months['AVG_LOSS_RATIO'] * 100).round(2)


        # [DEBUG 5] summary_3months 최종 확인
        print(f"✅ [DEBUG 5] summary_3months 행 수: {len(summary_3months)}")
        print(f"✅ [DEBUG 5] summary_3months MID_GROUP 종류: {summary_3months['MID_GROUP'].unique()}")
        print(f"✅ [DEBUG 5] summary_3months AVG_LOSS_RATIO 합계: {summary_3months['AVG_LOSS_RATIO'].sum():.6f}")
        print(f"✅ [DEBUG 5] summary_3months 샘플:\n{summary_3months[['REJ_GROUP', 'MID_GROUP', 'TOTAL_LOSS_QTY', 'AVG_LOSS_RATIO']].head(10)}")


    # yesterday_mid_summary 가져오기
        yesterday_mid = self.data.get('DATA_3210_wafering_300', {}).get('yesterday_mid_summary', pd.DataFrame())
        if yesterday_mid.empty:
            details['summary'] = summary_3months
            return details

        print(f"✅ [DEBUG 6] yesterday_mid 행 수: {len(yesterday_mid)}")
        print(f"✅ [DEBUG 6] yesterday_mid MID_GROUP 종류: {yesterday_mid['MID_GROUP'].unique()}")

        # 상위 3개 REJ_GROUP 가져오기 (Gap 기준)
        top3_rej_groups = self.data.get('DATA_3210_wafering_300', {}).get('top3_rej_groups', [])
        print(f"✅ [DEBUG 7] top3_rej_groups: {top3_rej_groups}")

        # 3개월 평균 (Ref) 준비
        ref_3months = summary_3months[summary_3months['REJ_GROUP'].isin(yesterday_mid['REJ_GROUP'])].copy()
        print(f"✅ [DEBUG 8] ref_3months 필터링 후 행 수: {len(ref_3months)}")
        # REJ_GROUP + MID_GROUP 기준 집계 (AFT_BAD_RSN_CD 는 제거하고 MID_GROUP 수준으로 비교)
        ref_3months = ref_3months.groupby(['REJ_GROUP', 'MID_GROUP'], dropna=False).agg(
            REF_AVG_LOSS_RATIO=('AVG_LOSS_RATIO', 'sum'),  # 동일 MID_GROUP 내 불량코드 합계 비율
            REF_LOSS_QTY=('TOTAL_LOSS_QTY', 'sum')
        ).reset_index()

        print(f"✅ [DEBUG 9] ref_3months 집계 후 행 수: {len(ref_3months)}")
        print(f"✅ [DEBUG 9] ref_3months 샘플:\n{ref_3months[['REJ_GROUP', 'MID_GROUP', 'REF_AVG_LOSS_RATIO', 'REF_LOSS_QTY']].head(10)}")
        print(f"yesterday_mid:", yesterday_mid.head(5))

        # 병합 → Gap 계산 (전체 사용)
        merged = pd.merge(
            yesterday_mid,
            ref_3months,
            on=['REJ_GROUP', 'MID_GROUP'],
            how='inner'
        )
        print(f"✅ [DEBUG 10] merged 행 수: {len(merged)} (어제: {len(yesterday_mid)}, 3 개월: {len(ref_3months)})")

        merged['GAP'] = merged['YESTERDAY_LOSS_PCT'] - (merged['REF_AVG_LOSS_RATIO'] * 100)
        merged['Gap'] = merged['GAP'].round(2)
        merged['실적(%)'] = merged['YESTERDAY_LOSS_PCT']
        merged['Ref(3개월)'] = (merged['REF_AVG_LOSS_RATIO'] * 100).round(2)
        merged['범례'] = merged['MID_GROUP']

        # [DEBUG 11] 최종 Ref(3 개월) 값 확인
        print(f"✅ [DEBUG 11] merged Ref(3개월) 평균: {merged['Ref(3개월)'].mean():.2f}%")
        print(f"✅ [DEBUG 11] merged Ref(3개월) 0 인 행: {(merged['Ref(3개월)'] == 0).sum()}개")

        # 개별 플롯 생성
        plot_paths = self._create_top3_midgroup_plot_per_group(merged, top3_rej_groups)

        # 각 그룹별 표도 상위 3개만
        group_tables = {}
        analysis_text = "[ Prime 주요 열위 불량 세부코드 분석 Ref.(3개월) 比 일 실적 변동 (상위 3개) ]\n"
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

        # print(f"[캐시 필터링] 최근 3개월 대상 월: {target_months}")

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

            # print(f"[캐시 필터링] 전체 {len(parquet_files)}개 중 대상 {len(valid_files)}개 파일 선정: {[f.name for f in valid_files]}")

            dfs = []
            for file_path in valid_files:
                try:
                    df_part = pd.read_parquet(file_path)
                    # print(f"[캐시] {file_path.name} 로드 완료: {len(df_part):,} 건")
                    dfs.append(df_part)
                except Exception as e:
                    print(f"[캐시] {file_path.name} 읽기 실패: {e}")

            if dfs:
                df_cached_3months = pd.concat(dfs, ignore_index=True)
                # print(f"[캐시] 총 {len(df_cached_3months):,} 건 데이터 병합 완료")
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
            # print(f"[self.data] DATA_LOT_3210_wafering_300 데이터 건수: {len(df_self_data):,} 건")
        else:
            print("[self.data] DATA_LOT_3210_wafering_300 없거나 빈 데이터")


        # ===================================================================
        # [핵심] MS6 기반 PRODUCT_TYPE 병합
        # ===================================================================
        if not df_cached_3months.empty:
            df_cached_3months = self._merge_product_type(df_cached_3months)

        if not df_self_data.empty:
            df_self_data = self._merge_product_type(df_self_data)

        # print(f"PRODUCT_TYPE 병합 완료: 3개월 {df_cached_3months['PRODUCT_TYPE'].notna().sum()}건, 당일 {df_self_data['PRODUCT_TYPE'].notna().sum()}건")

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
                self.avg_in_qty = 0 # 인스턴스 변수에 0 저장
                self.total_daily_qty = 0 # 인스턴스 변수에 0 저장
                return details

            self.avg_in_qty = avg_in_qty # 인스턴스 변수에 저장 → WAF 분석에서 사용

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

            self.total_daily_qty = total_daily_qty # 인스턴스 변수에 저장

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
                daily_rate = (daily_qty / total_daily_qty) * 100 if total_daily_qty != 0 else 0
                gap = ref_rate - daily_rate 

                report_table_total.append({
                    '구분': cret_cd,
                    'Ref.(3개월)': int(ref_qty),
                    '일': int(daily_qty),
                    'Ref.(3개월)%': f"{ref_rate:+.2f}%",
                    '일%': f"{daily_rate:+.2f}%",
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
                    daily_rate = (daily_qty / total_daily_qty) * 100 if total_daily_qty != 0 else 0
                    gap = ref_rate - daily_rate 

                    group_table.append({
                        '구분': cret_cd,
                        'Ref.(3개월)': int(ref_qty),
                        '일': int(daily_qty),
                        'Ref.(3개월)%': f"{ref_rate:+.2f}%",
                        '일%': f"{daily_rate:+.2f}%",
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
                    ax.text(bar.get_x() + bar.get_width() / 2, y_pos, f"{val:+.2f}%", ha='center', va=va, fontsize=9, fontweight='bold', color='black')

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
                ax.text(bar.get_x() + bar.get_width() / 2,  y_pos, f"{val:+.2f}%",  ha='center',  va=va, fontsize=9,  fontweight='bold', color='black')

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
            self.avg_in_qty = 0
            self.total_daily_qty = 0

        return details


    def _load_waf_60days_from_mixed_cache(self):
        """
        BASE_DT 기준으로 월별 + 일별 캐시에서 데이터를 로드만 함
        → 이후 _create_DATA_WAF_3210_wafering_300 내에서 
        공정별 REG_DTTM 컬럼 기준으로 재분리
        """
        PROJECT_ROOT = Path(__file__).parent.parent
        cache_dir = PROJECT_ROOT / "data_cache"

        if not cache_dir.exists():
            print(f"[WAF 혼합 로드] 캐시 디렉토리 없음: {cache_dir}")
            return pd.DataFrame()

        # ===================================================================
        # [1] 기준일 설정: 어제 기준 최근 90일 (여유)
        # ===================================================================
        base_date = datetime.now().date() - timedelta(days=1)
        target_dates_60days = {
            (base_date - timedelta(days=i)).strftime("%Y%m%d")
            for i in range(70)  # 여유 있게 70일 (일별 파일용)
        }
        target_months_90days = {
            (base_date - timedelta(days=i)).strftime("%Y%m")
            for i in range(70)  # 월별 파일용
        }

        print(f"[WAF 혼합 로드] 대상 기간: {(base_date - timedelta(days=69)).strftime('%Y%m%d')} \\~ {base_date.strftime('%Y%m%d')}")

        # ===================================================================
        # [2] 월별 파일 로드 (예: DATA_WAF_3210_wafering_300_202601.parquet)
        # ===================================================================
        monthly_pattern = "DATA_WAF_3210_wafering_300_*.parquet"
        monthly_files = list(cache_dir.glob(monthly_pattern))

        valid_monthly_files = []
        for file_path in monthly_files:
            try:
                # 파일명에서 마지막 언더스코어 뒤의 6자리 추출 (YYYYMM)
                stem = file_path.stem  # 전체 이름
                parts = stem.split('_')
                if len(parts) < 1:
                    continue
                date_part = parts[-1]  # '202601' or '20260301'

                # 길이로 구분: 6자리 → 월별, 8자리 → 일별
                if len(date_part) == 6 and date_part.isdigit():
                    ym = date_part  # '202601'
                    if ym in target_months_90days:
                        valid_monthly_files.append((file_path, ym))
            except Exception as e:
                print(f"[월별 파일 파싱 실패] {file_path.name}: {e}")

        print(f"[월별 파일] {len(valid_monthly_files)}개 발견: {[ym for _, ym in valid_monthly_files]}")

        dfs_monthly = []
        for file_path, ym in valid_monthly_files:
            try:
                df = pd.read_parquet(file_path)
                print(f"[월별 로드] {file_path.name} → {len(df):,} 건")

                # BASE_DT가 있는 경우, 해당 월 데이터만 필터링
                if 'BASE_DT' in df.columns:
                    df['BASE_DT'] = df['BASE_DT'].astype(str)
                    df = df[df['BASE_DT'].str.startswith(ym)].copy()
                    print(f"[필터링] {ym} → {len(df):,} 건")

                dfs_monthly.append(df)
            except Exception as e:
                print(f"[월별 로드 실패] {file_path.name}: {e}")

        # ===================================================================
        # [3] 일별 파일 로드 (예: DATA_WAF_3210_wafering_300_20260301.parquet)
        # ===================================================================
        daily_files = []
        for date_str in target_dates_60days:
            file_path = cache_dir / f"DATA_WAF_3210_wafering_300_{date_str}.parquet"
            if file_path.exists():
                daily_files.append(file_path)

        print(f"[일별 파일] {len(daily_files)}개 발견")

        dfs_daily = []
        for file_path in daily_files:
            try:
                df = pd.read_parquet(file_path)
                print(f"[일별 로드] {file_path.name} → {len(df):,} 건")
                dfs_daily.append(df)
            except Exception as e:
                print(f"[일별 로드 실패] {file_path.name}: {e}")

        # ===================================================================
        # [4] 병합
        # ===================================================================
        all_dfs = dfs_monthly + dfs_daily
        if not all_dfs:
            print("[WAF 혼합 로드] 사용 가능한 데이터 없음")
            return pd.DataFrame()

        df_combined = pd.concat(all_dfs, ignore_index=True)
        print(f"[WAF 혼합 로드 완료] 총 {len(df_combined):,} 건 병합")

        df_fs = df_combined[df_combined['CRET_CD'] == 'FS'].copy() # CRET_CD가 FS인 데이터만 사용.
        print(f"[CRET_CD 필터링] 원본 {len(df_combined):,} 건 → 'FS' 데이터: {len(df_fs):,} 건")

        df_combined = df_fs

        # ===================================================================
        # 🔍 BASE_DT 기준 데이터 기간 출력
        # ===================================================================
        if 'BASE_DT' in df_combined.columns:
            base_dt_series = pd.to_datetime(df_combined['BASE_DT'], format='%Y%m%d', errors='coerce')
            valid_dates = base_dt_series.dropna()

            if len(valid_dates) > 0:
                start_date = valid_dates.min().strftime('%Y-%m-%d')
                end_date = valid_dates.max().strftime('%Y-%m-%d')
                print(f"[WAF 데이터 기간] BASE_DT 기준: {start_date} \\\~ {end_date}")
            else:
                print("[WAF 데이터 기간] BASE_DT에서 유효한 날짜 없음")
        else:
            print("[WAF 데이터 기간] BASE_DT 컬럼 없음")

        return df_combined

    def _create_DATA_WAF_3210_wafering_300(self):
        """
        3210 WAF 상세 분석 - 캐시된 3개월 데이터 + self.data의 당일 데이터 모두 활용
        """
        details = {}
        PROJECT_ROOT = Path(__file__).parent.parent

        # ===================================================================
        # 1. [신규] data_cache에서 3개월 데이터 직접 로드 (장기 분석용)
        # ===================================================================
        base_date = (datetime.now().date() - timedelta(days=1))
        target_months = []
        current = base_date.replace(day=1)
        for _ in range(3):
            current = (current - timedelta(days=1)).replace(day=1)
            month_str = current.strftime("%Y%m")
            target_months.append(month_str)
        target_months = sorted(target_months)  # 과거 → 최근

        # print(f"[WAF 캐시 필터링] 최근 3개월 대상 월: {target_months}")

        cache_dir = PROJECT_ROOT / "data_cache"
        pattern = "DATA_WAF_3210_wafering_300_*.parquet"
        parquet_files = list(cache_dir.glob(pattern))

        df_cached_3months = pd.DataFrame()

        if parquet_files:
            valid_files = []
            for file_path in parquet_files:
                try:
                    stem = file_path.stem
                    date_part = stem.split('_')[-1]  # '202506'

                    if len(date_part) != 6 or not date_part.isdigit():
                        continue

                    file_ym = date_part
                except Exception as e:
                    print(f"[WAF 캐시] {file_path.name}에서 월 정보 추출 실패 → 건너뜀: {e}")
                    continue

                if file_ym in target_months:
                    valid_files.append(file_path)

            # print(f"[WAF 캐시 필터링] 전체 {len(parquet_files)}개 중 대상 {len(valid_files)}개 파일 선정: {[f.name for f in valid_files]}")

            dfs = []
            for file_path in valid_files:
                try:
                    df_part = pd.read_parquet(file_path)
                    print(f"[WAF 캐시] {file_path.name} 로드 완료: {len(df_part):,} 건")
                    dfs.append(df_part)
                except Exception as e:
                    print(f"[WAF 캐시] {file_path.name} 읽기 실패: {e}")

            if dfs:
                df_cached_3months = pd.concat(dfs, ignore_index=True)
                print(f"[WAF 캐시] 총 {len(df_cached_3months):,} 건 데이터 병합 완료")
            else:
                print("[WAF 캐시] 모든 파일 로드 실패 → 3개월 데이터 없음")
        else:
            print("[WAF 캐시] data_cache에 DATA_WAF_3210_wafering_300_*.parquet 파일 없음")

        # ===================================================================
        # 2. [기존] self.data에서 당일 데이터 사용
        # ===================================================================
        df_self_data = pd.DataFrame()
        if 'DATA_WAF_3210_wafering_300' in self.data and not self.data['DATA_WAF_3210_wafering_300'].empty:
            df_self_data = self.data['DATA_WAF_3210_wafering_300'].copy()
            print(f"[self.data] DATA_WAF_3210_wafering_300 데이터 건수: {len(df_self_data):,} 건")
        else:
            print("[self.data] DATA_WAF_3210_wafering_300 없거나 빈 데이터")

        # ===================================================================
        # 3. [핵심] PRODUCT_TYPE 병합
        # ===================================================================
        if not df_cached_3months.empty:
            df_cached_3months = self._merge_product_type(df_cached_3months)

        if not df_self_data.empty:
            df_self_data = self._merge_product_type(df_self_data)

        print(f"[WAF] PRODUCT_TYPE 병합 완료: 3개월 {df_cached_3months['PRODUCT_TYPE'].notna().sum()}건, 당일 {df_self_data['PRODUCT_TYPE'].notna().sum()}건")

        # ===================================================================
        # 3. [핵심] 3개월 데이터 기반 Loss Rate 분석
        # ===================================================================

        avg_in_qty = getattr(self, 'avg_in_qty', 0)
        total_daily_qty = getattr(self, 'total_daily_qty', 0)

        # 강제 float 변환 (Decimal → float)
        try:
            avg_in_qty = float(avg_in_qty)
            total_daily_qty = float(total_daily_qty)
        except (TypeError, ValueError) as e:
            return details  # 분석 중단

        if avg_in_qty == 0:
            print(" 분모(IN_QTY)가 0입니다. Loss Rate 계산 불가")
            return details
        
        if total_daily_qty == 0:
            print("total_daily_qty = 0 → Daily 분석 생략 (Ref만 분석)")

        def calculate_loss_metrics(group_data, eqp_col, denominator):
            """
            실제 LOSS_QTY 합계 (Count) 와 불량률 (Rate) 을 모두 반환
            """
            if eqp_col not in group_data.columns or denominator == 0:
                return {}
            
            valid = group_data.dropna(subset=[eqp_col]).copy()
            if valid.empty:
                return {}
            
            # LOSS_QTY numeric 변환
            valid.loc[:,'LOSS_QTY'] = pd.to_numeric(valid['LOSS_QTY'], errors='coerce').fillna(0.0).astype(float)
            
            # 장비별 LOSS_QTY 합계
            loss_sum = valid.groupby(eqp_col)['LOSS_QTY'].sum()
            
            #  count (int) 와 rate (float, %) 를 모두 반환
            return {
                eqp: {
                    'count': int(qty), 
                    'rate': round(qty / denominator * 100, 4)  # % 단위 (예: 3.05)
                } 
                for eqp, qty in loss_sum.items()
            }

        # ===================================================================
        # [분석] Ref(3개월) 장비별 불량률 계산
        # ===================================================================

        ref_results = {}
        daily_results = {}

        # 1) PIT
        df_pit = df_cached_3months[df_cached_3months['REJ_GROUP'] == 'PIT']
        if not df_pit.empty:
            eqp_col = ['EQP_NM_300_WF_3670']
            pit_ref = {}
            for eqp in eqp_col:
                res = calculate_loss_metrics(df_pit, eqp, avg_in_qty)
                if res:
                    pit_ref[eqp] = res
            ref_results['PIT'] = pit_ref


        df_pit_d = df_self_data[df_self_data['REJ_GROUP'] == 'PIT']
        if not df_pit_d.empty:
            eqp_col = ['EQP_NM_300_WF_3670']
            pit_daily = {}
            for eqp in eqp_col:
                res = calculate_loss_metrics(df_pit, eqp, avg_in_qty)
                if res:
                    pit_daily[eqp] = res
            daily_results['PIT'] = pit_daily

        # 2) SCRATCH
        df_scratch = df_cached_3months[df_cached_3months['REJ_GROUP'] == 'SCRATCH']
        if not df_scratch.empty:
            eqps_scratch = ['EQP_NM_300_WF_3670', 'EQP_NM_300_WF_6100']
            scratch_ref  = {}
            for eqp in eqps_scratch:
                res = calculate_loss_metrics(df_scratch, eqp, avg_in_qty)
                if res:
                    scratch_ref[eqp] = res
            ref_results['SCRATCH'] = scratch_ref

        df_scratch_d = df_self_data[df_self_data['REJ_GROUP'] == 'SCRATCH']
        if not df_scratch_d.empty:
            eqps_scratch = ['EQP_NM_300_WF_3670', 'EQP_NM_300_WF_6100']
            scratch_daily = {}
            for eqp in eqps_scratch:
                res = calculate_loss_metrics(df_scratch_d, eqp, total_daily_qty)
                if res:
                    scratch_daily[eqp] = res
            daily_results['SCRATCH'] = scratch_daily


        # 3) EDGE
        df_edge = df_cached_3months[df_cached_3months['REJ_GROUP'] == 'EDGE']
        if not df_edge.empty:
            eqps = ['EQP_NM_300_WF_3335', 'EQP_NM_300_WF_3696', 'EQP_NM_300_WF_7000']
            edge_ref = {}
            for eqp in eqps:
                res = calculate_loss_metrics(df_edge, eqp, avg_in_qty)
                if res:
                    edge_ref[eqp] = res
            ref_results['EDGE'] = edge_ref

        df_edge_d = df_self_data[df_self_data['REJ_GROUP'] == 'EDGE']
        if not df_edge_d.empty:
            eqps = ['EQP_NM_300_WF_3335', 'EQP_NM_300_WF_3696', 'EQP_NM_300_WF_7000']
            edge_daily = {}
            for eqp in eqps:
                res = calculate_loss_metrics(df_edge_d, eqp, total_daily_qty)
                if res:
                    edge_daily[eqp] = res
            daily_results['EDGE'] = edge_daily


        # 4) BROKEN
        df_broken = df_cached_3months[df_cached_3months['REJ_GROUP'] == 'BROKEN']
        if not df_broken.empty:
            eqps = ['EQP_NM_300_WF_3670', 'EQP_NM_300_WF_6100', 'EQP_NM_300_WF_6500']
            broken_ref = {}
            for eqp in eqps:
                res = calculate_loss_metrics(df_broken, eqp, avg_in_qty)
                if res:
                    broken_ref[eqp] = res
            ref_results['BROKEN'] = broken_ref

        df_broken_d = df_self_data[df_self_data['REJ_GROUP'] == 'BROKEN']
        if not df_broken_d.empty:
            eqps = ['EQP_NM_300_WF_3670', 'EQP_NM_300_WF_6100', 'EQP_NM_300_WF_6500']
            broken_daily = {}
            for eqp in eqps:
                res = calculate_loss_metrics(df_broken_d, eqp, total_daily_qty)
                if res:
                    broken_daily[eqp] = res
            daily_results['BROKEN'] = broken_daily


        # 5) CHIP
        df_chip = df_cached_3months[df_cached_3months['REJ_GROUP'] == 'CHIP']
        if not df_chip.empty:
            chip_ref = {}
            cond_edge = df_chip['AFT_BAD_RSN_CD'] == 'EDGE-CHIP'
            cond_lap = df_chip['AFT_BAD_RSN_CD'] == 'CHIP-LAP'
            cond_eg1af = df_chip['AFT_BAD_RSN_CD'] == 'CHIP_EG1AF'
            cond_eg1bf = df_chip['AFT_BAD_RSN_CD'] == 'CHIP_EG1BF'

            if not df_chip[cond_edge].empty:
                for eqp in ['EQP_NM_300_WF_3335', 'EQP_NM_300_WF_3696']:
                    res = calculate_loss_metrics(df_chip[cond_edge], eqp, avg_in_qty)
                    if res:
                        chip_ref[f'EDGE-CHIP_{eqp}'] = res
            if not df_chip[cond_lap].empty:
                res = calculate_loss_metrics(df_chip[cond_lap], 'EQP_NM_300_WF_3670', avg_in_qty)
                if res:
                    chip_ref['CHIP-LAP_EQP_NM_300_WF_3670'] = res
            if not df_chip[cond_eg1af].empty:
                for eqp in ['EQP_NM_300_WF_3335', 'EQP_NM_300_WF_3696']:
                    res = calculate_loss_metrics(df_chip[cond_eg1af], eqp, avg_in_qty)
                    if res:
                        chip_ref[f'CHIP_EG1AF_{eqp}'] = res
            if not df_chip[cond_eg1bf].empty:
                res = calculate_loss_metrics(df_chip[cond_eg1bf], 'EQP_NM_300_WF_3300', avg_in_qty)
                if res:
                    chip_ref['CHIP_EG1BF_EQP_NM_300_WF_3300'] = res
            ref_results['CHIP'] = chip_ref

        df_chip_d = df_self_data[df_self_data['REJ_GROUP'] == 'CHIP']
        if not df_chip_d.empty:
            chip_daily = {}
            cond_edge = df_chip_d['AFT_BAD_RSN_CD'] == 'EDGE-CHIP'
            cond_lap = df_chip_d['AFT_BAD_RSN_CD'] == 'CHIP-LAP'
            cond_eg1af = df_chip_d['AFT_BAD_RSN_CD'] == 'CHIP_EG1AF'
            cond_eg1bf = df_chip_d['AFT_BAD_RSN_CD'] == 'CHIP_EG1BF'

            if not df_chip_d[cond_edge].empty:
                for eqp in ['EQP_NM_300_WF_3335', 'EQP_NM_300_WF_3696']:
                    res = calculate_loss_metrics(df_chip_d[cond_edge], eqp, total_daily_qty)
                    if res:
                        chip_daily[f'EDGE-CHIP_{eqp}'] = res
            if not df_chip_d[cond_lap].empty:
                res = calculate_loss_metrics(df_chip_d[cond_lap], 'EQP_NM_300_WF_3670', total_daily_qty)
                if res:
                    chip_daily['CHIP-LAP_EQP_NM_300_WF_3670'] = res
            if not df_chip_d[cond_eg1af].empty:
                for eqp in ['EQP_NM_300_WF_3335', 'EQP_NM_300_WF_3696']:
                    res = calculate_loss_metrics(df_chip_d[cond_eg1af], eqp, total_daily_qty)
                    if res:
                        chip_daily[f'CHIP_EG1AF_{eqp}'] = res
            if not df_chip_d[cond_eg1bf].empty:
                res = calculate_loss_metrics(df_chip_d[cond_eg1bf], 'EQP_NM_300_WF_3300', total_daily_qty)
                if res:
                    chip_daily['CHIP_EG1BF_EQP_NM_300_WF_3300'] = res
            daily_results['CHIP'] = chip_daily

        # 6) VISUAL
        df_visual = df_cached_3months[df_cached_3months['REJ_GROUP'] == 'VISUAL']
        if not df_visual.empty:
            cond = df_visual['AFT_BAD_RSN_CD'].isin(['B_PARTICLE', 'B_PAR2'])
            visual_filtered = df_visual[cond]
            eqp_col = ['EQP_NM_300_WF_6100']  
            visual_ref = {}
            for eqp in eqp_col:
                if eqp in visual_filtered.columns:
                    res = calculate_loss_metrics(visual_filtered, eqp, avg_in_qty)
                    if res:
                        visual_ref[eqp] = res
            if visual_ref:
                ref_results['VISUAL'] = visual_ref

        # 2) VISUAL (daily_results)
        df_visual_d = df_self_data[df_self_data['REJ_GROUP'] == 'VISUAL']
        if not df_visual_d.empty:
            cond = df_visual_d['AFT_BAD_RSN_CD'].isin(['B_PARTICLE', 'B_PAR2'])
            visual_filtered_d = df_visual_d[cond]
            eqp_col = ['EQP_NM_300_WF_6100']  
            visual_daily = {}
            for eqp in eqp_col:
                if eqp in visual_filtered_d.columns:
                    res = calculate_loss_metrics(visual_filtered_d, eqp, total_daily_qty)
                    if res:
                        visual_daily[eqp] = res
            if visual_daily:
                daily_results['VISUAL'] = visual_daily

        # ===================================================================
        # 8. Gap 계산 (각 공정별 상위 3개 장비만)
        # ===================================================================

        gap_results = {}

        def extract_process(eqp_col):
            match = re.search(r'(\d{4})$', eqp_col)
            return match.group(1) if match else eqp_col

        for group, ref_dict in ref_results.items():
            if group not in daily_results:
                continue
            daily_group = daily_results[group]

            if not isinstance(ref_dict, dict):
                continue
            if not isinstance(daily_group, dict):
                daily_group = {}

            # SCRATCH, EDGE, BROKEN, CHIP: 중첩 구조 (eqp_col → 장비)
            if group in ['SCRATCH', 'EDGE', 'BROKEN', 'CHIP']:
                gap_sub = {}
                for eqp_col, ref_rates in ref_dict.items():
                    if not isinstance(ref_rates, dict):
                        continue

                    daily_rates = daily_group.get(eqp_col, {})
                    if not isinstance(daily_rates, dict):
                        daily_rates = {}

                    gap_col = {}
                    for eqp_name, data in ref_rates.items():
                        if not isinstance(data, dict):
                            continue
                        ref_rate = data.get('rate', 0.0)

                        daily_data = daily_rates.get(eqp_name, {})
                        if not isinstance(daily_data, dict):
                            daily_rate = 0.0
                        else:
                            daily_rate = daily_data.get('rate', 0.0)

                        gap = (daily_rate - ref_rate) / 100.0
                        gap_col[eqp_name] = gap

                    if isinstance(gap_col, dict) and gap_col:
                        gap_sub[eqp_col] = gap_col

                if isinstance(gap_sub, dict) and gap_sub:
                    gap_results[group] = gap_sub

            else:  # 단일 그룹 (PIT, VISUAL 등)
                gap_sub = {}
                for eqp_col, ref_rates in ref_dict.items():
                    if isinstance(ref_rates, dict):
                        # case 1: 정상 dict → 장비별 데이터 있음
                        gap_col = {}
                        for eqp_name, data in ref_rates.items():
                            if not isinstance(data, dict):
                                continue
                            ref_rate = data.get('rate', 0.0)

                            daily_data = daily_group.get(eqp_name, {})
                            if not isinstance(daily_data, dict):
                                daily_rate = 0.0
                            else:
                                daily_rate = daily_data.get('rate', 0.0)

                            gap = (daily_rate - ref_rate) / 100.0
                            gap_col[eqp_name] = gap

                        if isinstance(gap_col, dict) and gap_col:
                            gap_sub[eqp_col] = gap_col

                    elif isinstance(ref_rates, (int, float)):
                        # case 2: ref_rates가 숫자 → 장비 정보 없음
                        proc_match = re.search(r'(\d{4})$', eqp_col)
                        eqp_name_fallback = proc_match.group(1) if proc_match else eqp_col

                        ref_rate = float(ref_rates)

                        daily_rate = 0.0
                        if isinstance(daily_group, dict):
                            daily_data = daily_group.get(eqp_col, {})
                            if isinstance(daily_data, dict):
                                daily_rate = daily_data.get('rate', 0.0)
                            elif isinstance(daily_data, (int, float)):
                                daily_rate = float(daily_data)

                        gap = (daily_rate - ref_rate) / 100.0

                        gap_col = {eqp_name_fallback: gap}
                        if isinstance(gap_col, dict) and gap_col:
                            gap_sub[eqp_col] = gap_col

                    else:
                        # case 3: 예상치 못한 타입
                        print(f"[WARN] {group} - {eqp_col}: ref_rates 타입 오류 → {type(ref_rates)}")
                        continue

                if isinstance(gap_sub, dict) and gap_sub:
                    gap_results[group] = gap_sub
            
        # [핵심] 60일 데이터 로드 (BASE_DT 기준)
        df_60days_raw = self._load_waf_60days_from_mixed_cache()
        if df_60days_raw.empty:
            print("[WAF] 60일 원본 데이터 없음")
            # 기존 로직 계속
        else:
            # print(f"[WAF] 60일 원본 데이터 로드 완료: {len(df_60days_raw):,} 건")
            # 공정별 데이터셋 생성
            process_datasets = {}
            # 1. 3670
            eqp_col_3670 = 'EQP_NM_300_WF_3670'
            reg_col_3670 = 'REG_DTTM_300_WF_3670'
            if reg_col_3670 in df_60days_raw.columns:
                df_3670 = df_60days_raw.dropna(subset=[reg_col_3670, eqp_col_3670]).copy()
                df_3670['process_datetime'] = pd.to_datetime(
                    df_3670[reg_col_3670],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_3670['base_dt_7hours'] = (df_3670['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                # 3개 컬럼만 선택 + EQP 컬럼 이름 통일
                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP', 'BASE_DT','LOSS_QTY', eqp_col_3670]
                missing_cols = [c for c in selected_cols if c not in df_3670.columns]
                if missing_cols:
                    print(f"[3670] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_3670 = df_3670[selected_cols].rename(columns={eqp_col_3670: 'EQP_NM'})
                    process_datasets['3670'] = df_3670

            # 2. 6100
            eqp_col_6100 = 'EQP_NM_300_WF_6100'
            reg_col_6100 = 'REG_DTTM_300_WF_6100'
            if reg_col_6100 in df_60days_raw.columns:
                df_6100 = df_60days_raw.dropna(subset=[reg_col_6100, eqp_col_6100]).copy()
                df_6100['process_datetime'] = pd.to_datetime(
                    df_6100[reg_col_6100],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_6100['base_dt_7hours'] = (df_6100['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP', 'BASE_DT','LOSS_QTY', eqp_col_6100]
                missing_cols = [c for c in selected_cols if c not in df_6100.columns]
                if missing_cols:
                    print(f"[6100] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_6100 = df_6100[selected_cols].rename(columns={eqp_col_6100: 'EQP_NM'})
                    process_datasets['6100'] = df_6100

            # 3. 3335
            eqp_col_3335 = 'EQP_NM_300_WF_3335'
            reg_col_3335 = 'REG_DTTM_300_WF_3335'
            if reg_col_3335 in df_60days_raw.columns:
                df_3335 = df_60days_raw.dropna(subset=[reg_col_3335, eqp_col_3335]).copy()
                df_3335['process_datetime'] = pd.to_datetime(
                    df_3335[reg_col_3335],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_3335['base_dt_7hours'] = (df_3335['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP', 'BASE_DT','LOSS_QTY', eqp_col_3335]
                missing_cols = [c for c in selected_cols if c not in df_3335.columns]
                if missing_cols:
                    print(f"[3335] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_3335 = df_3335[selected_cols].rename(columns={eqp_col_3335: 'EQP_NM'})
                    process_datasets['3335'] = df_3335

            # 4. 3696
            eqp_col_3696 = 'EQP_NM_300_WF_3696'
            reg_col_3696 = 'REG_DTTM_300_WF_3696'
            if reg_col_3696 in df_60days_raw.columns:
                df_3696 = df_60days_raw.dropna(subset=[reg_col_3696, eqp_col_3696]).copy()
                df_3696['process_datetime'] = pd.to_datetime(
                    df_3696[reg_col_3696],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_3696['base_dt_7hours'] = (df_3696['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP','BASE_DT', 'LOSS_QTY', eqp_col_3696]
                missing_cols = [c for c in selected_cols if c not in df_3696.columns]
                if missing_cols:
                    print(f"[3696] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_3696 = df_3696[selected_cols].rename(columns={eqp_col_3696: 'EQP_NM'})
                    process_datasets['3696'] = df_3696

            # 5. 6500
            eqp_col_6500 = 'EQP_NM_300_WF_6500'
            reg_col_6500 = 'REG_DTTM_300_WF_6500'
            if reg_col_6500 in df_60days_raw.columns:
                df_6500 = df_60days_raw.dropna(subset=[reg_col_6500, eqp_col_6500]).copy()
                df_6500['process_datetime'] = pd.to_datetime(
                    df_6500[reg_col_6500],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_6500['base_dt_7hours'] = (df_6500['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP', 'BASE_DT','LOSS_QTY', eqp_col_6500]
                missing_cols = [c for c in selected_cols if c not in df_6500.columns]
                if missing_cols:
                    print(f"[6500] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_6500 = df_6500[selected_cols].rename(columns={eqp_col_6500: 'EQP_NM'})
                    process_datasets['6500'] = df_6500
            # 6. 3300
            eqp_col_3300 = 'EQP_NM_300_WF_3300'
            reg_col_3300 = 'REG_DTTM_300_WF_3300'
            if reg_col_3300 in df_60days_raw.columns:
                df_3300 = df_60days_raw.dropna(subset=[reg_col_3300, eqp_col_3300]).copy()
                df_3300['process_datetime'] = pd.to_datetime(
                    df_3300[reg_col_3300],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_3300['base_dt_7hours'] = (df_3300['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP', 'BASE_DT','LOSS_QTY', eqp_col_3300]
                missing_cols = [c for c in selected_cols if c not in df_3300.columns]
                if missing_cols:
                    print(f"[3300] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_3300 = df_3300[selected_cols].rename(columns={eqp_col_3300: 'EQP_NM'})
                    process_datasets['3300'] = df_3300

            # 7. 7000
            eqp_col_7000 = 'EQP_NM_300_WF_7000'
            reg_col_7000 = 'REG_DTTM_300_WF_7000'
            if reg_col_7000 in df_60days_raw.columns:
                df_7000 = df_60days_raw.dropna(subset=[reg_col_7000, eqp_col_7000]).copy()
                df_7000['process_datetime'] = pd.to_datetime(
                    df_7000[reg_col_7000],
                    format='%Y%m%d%H%M%S%f',
                    errors='coerce'
                )

                # 시간 기준 보정: 07:00 \~ 익일 06:59 까지를 전날 기준일로 묶기 위해 -7시간 이동
                df_7000['base_dt_7hours'] = (df_7000['process_datetime'] - pd.Timedelta(hours=7)).dt.strftime('%Y%m%d') 

                selected_cols = ['process_datetime','base_dt_7hours','REJ_GROUP', 'BASE_DT','LOSS_QTY', eqp_col_7000]
                missing_cols = [c for c in selected_cols if c not in df_7000.columns]
                if missing_cols:
                    print(f"[7000] 누락된 컬럼: {missing_cols} → 건너뜀")
                else:
                    df_7000 = df_7000[selected_cols].rename(columns={eqp_col_7000: 'EQP_NM'})
                    process_datasets['7000'] = df_7000

            # 저장
            self.data['WAF_PROCESS_DATASETS'] = process_datasets

        # ===================================================================
        # 9. details에 저장
        # ===================================================================

        ref_results = ref_results if isinstance(ref_results, dict) else {}
        daily_results = daily_results if isinstance(daily_results, dict) else {}
        gap_results = gap_results if isinstance(gap_results, dict) else {}

        details['waf_analysis_ref'] = ref_results
        details['waf_analysis_daily'] = daily_results
        details['waf_analysis_gap'] = gap_results
        details['df_cached_3months'] = df_cached_3months
        details['df_self_data'] = df_self_data
        details['avg_in_qty'] = avg_in_qty
        details['total_daily_qty'] = total_daily_qty

        self.data['waf_analysis_gap'] = gap_results
        self.data['DATA_3210_wafering_300_details'] = details  # top3_rej_groups 포함

        return details
    

    def _create_DATA_1511_SMAX_wafering_300(self):
        """
        self.data['DATA_1511_SMAX_wafering_300'] 에서
        공정별, 장비별, 일별 IN_QTY 추출 → 분모용 데이터셋 생성
        """
        # ===================================================================
        # [1] 원본 데이터 확인
        # ===================================================================
        df_raw = self.data.get('DATA_1511_SMAX_wafering_300')
        if df_raw is None or df_raw.empty:
            print("[SMAX] 원본 데이터 없음: DATA_1511_SMAX_wafering_300")
            return


        # ===================================================================
        # [2] 기준일 설정: 어제 기준 최근 60일
        # ===================================================================
        base_date = datetime.now().date() - timedelta(days=1)
        target_dates = {
            (base_date - timedelta(days=i)).strftime("%Y%m%d")
            for i in range(60)
        }
        # print("SMAX 컬럼:", df_raw.columns.tolist())
        df_filtered = df_raw[df_raw['base_dt'].astype(str).isin(target_dates)].copy()
        if df_filtered.empty:
            print("[SMAX] 필터링 후 데이터 없음 (최근 60일)")
            return

        # ===================================================================
        # [3] 필수 컬럼 확인
        # ===================================================================
        required_cols = ['base_dt', 'oper_id', 'eqp_name', 'prodc_qty']
        missing_cols = [c for c in required_cols if c not in df_filtered.columns]
        if missing_cols:
            print(f"[SMAX] 필수 컬럼 누락: {missing_cols}")
            return

        # 숫자형 변환
        df_filtered['prodc_qty'] = pd.to_numeric(df_filtered['prodc_qty'], errors='coerce').fillna(0)

        # ===================================================================
        # [4] 공정별 데이터셋 분리
        # ===================================================================
        inqty_datasets = {}
        target_procs = ['3670', '6100', '3335', '3696', '6500', '3300', '7000']

        for proc in target_procs:
            df_proc = df_filtered[df_filtered['oper_id'] == proc].copy()
            if df_proc.empty:
                print(f"[SMAX] 공정 {proc} 데이터 없음")
                continue

            # 필요한 컬럼만 유지
            df_proc = df_proc[['base_dt', 'eqp_name', 'prodc_qty']].copy()
            inqty_datasets[proc] = df_proc

        # ===================================================================
        # [5] 저장: self.data에 저장
        # ===================================================================
        self.data['SMAX_INQTY_DATASETS'] = inqty_datasets


    def _calculate_loss_rate_by_process(self):
        """
        WAF_PROCESS_DATASETS (분자) + SMAX_INQTY_DATASETS (분모)
        → 공정별, 장비별, 일별 LOSS_RATE 계산
        """
        if 'WAF_PROCESS_DATASETS' not in self.data:
            print("[LOSS_RATE] 분자 데이터 없음: WAF_PROCESS_DATASETS")
            return {}

        if 'SMAX_INQTY_DATASETS' not in self.data:
            print("[LOSS_RATE] 분모 데이터 없음: SMAX_INQTY_DATASETS")
            return {}

        waf_datasets = self.data['WAF_PROCESS_DATASETS']  # 분자
        smax_datasets = self.data['SMAX_INQTY_DATASETS']  # 분모
        loss_rate_results = {}

        for proc in waf_datasets.keys():
            if proc not in smax_datasets:
                print(f"[LOSS_RATE] 공정 {proc}: 분모 데이터 없음")
                continue

            # ===================================================================
            # [1] 분자 데이터 준비 (LOSS_QTY)
            # ===================================================================
            df_num = waf_datasets[proc].copy()
            df_num = df_num.rename(columns={
                'base_dt_7hours' : 'base_dt_7hours',
                'EQP_NM': 'eqp_name'
            })
            df_num['base_dt'] = df_num['base_dt_7hours'].astype(str)
            df_num['eqp_name'] = df_num['eqp_name'].astype(str)

            # REJ_GROUP 기준으로 LOSS_QTY 합계
            df_num_grouped = df_num.groupby(['base_dt', 'eqp_name', 'REJ_GROUP'])['LOSS_QTY'].sum().reset_index()
            # ===================================================================
            # [2] 분모 데이터 준비 (IN_QTY)
            # ===================================================================
            df_denom = smax_datasets[proc].copy()
            df_denom['base_dt'] = df_denom['base_dt'].astype(str)
            df_denom['eqp_name'] = df_denom['eqp_name'].astype(str)

            # 장비별 투입량 합계 (일별)
            df_denom_grouped = df_denom.groupby(['base_dt', 'eqp_name'])['prodc_qty'].sum().reset_index()
            df_denom_grouped = df_denom_grouped.rename(columns={'prodc_qty': 'IN_QTY'})

            # ===================================================================
            # [3] 병합: 분자 + 분모
            # ===================================================================
            df_merge = pd.merge(
                df_denom_grouped,
                df_num_grouped,
                on=['base_dt', 'eqp_name'],
                how='left'
            )

            # ===================================================================
            # [4] LOSS_RATE 계산
            # ===================================================================
            df_merge['LOSS_QTY'] = df_merge['LOSS_QTY'].astype(float)
            df_merge['IN_QTY'] = df_merge['IN_QTY'].fillna(0).astype(float)

            # 방어적 나누기
            df_merge['LOSS_RATE'] = (
                df_merge['LOSS_QTY'] / (df_merge['IN_QTY'] + 1e-9)
            ) * 100
            df_merge['LOSS_RATE'] = df_merge['LOSS_RATE'].round(4)

            # 날짜 정렬
            df_merge = df_merge.sort_values(['base_dt', 'eqp_name']).reset_index(drop=True)

            # ===================================================================
            # [5] 저장
            # ===================================================================
            loss_rate_results[proc] = df_merge

        # 최종 저장
        self.data['LOSS_RATE_BY_EQP'] = loss_rate_results
        return loss_rate_results


    def _plot_rej_group_top3_eqp_trend(self, output_dir="./daily_reports_debug"):
        """
        REJ_GROUP별 상위 3개 장비에 대해 IN_QTY(막대) + LOSS_RATE(선) 이중축 그래프 생성 → PNG 저장
        → 생성된 파일 경로를 반환 (엑셀 삽입용)
        """

        # ===================================================================
        # [1] LOSS_RATE_BY_EQP 존재 확인
        # ===================================================================
        if 'LOSS_RATE_BY_EQP' not in self.data:
            print("[ERROR] self.data에 'LOSS_RATE_BY_EQP' 없음")
            return {}
        else:
            print(f"[OK] LOSS_RATE_BY_EQP 존재 → {len(self.data['LOSS_RATE_BY_EQP'])}개 공정")

        # ===================================================================
        # [2] top3_rej_groups 및 valid_groups 확인
        # ===================================================================
        top3_rej_groups = self.data.get('DATA_3210_wafering_300', {}).get('top3_rej_groups', [])
        # top3_rej_groups = ['PIT', 'EDGE', 'VISUAL']
        valid_groups = [g for g in top3_rej_groups if g in ['PIT', 'SCRATCH', 'EDGE', 'BROKEN', 'CHIP', 'VISUAL']]

        if not valid_groups:
            print("[WARNING] 유효한 REJ_GROUP 없음 → 그래프 생성 중단")
            return {}

        # ===================================================================
        # [3] waf_analysis_gap 확인
        # ===================================================================
        waf_gap_data = self.data.get('waf_analysis_gap', {})
        if not waf_gap_data:
            print("[ERROR] self.data에 'waf_analysis_gap' 없음")
            return {}
        else:
            print(f"[OK] waf_analysis_gap 존재 → {len(waf_gap_data)}개 그룹")

        # ===================================================================
        # [4] LOSS_RATE_BY_EQP 통합
        # ===================================================================
        try:
            df_all_loss = pd.concat([
                df.assign(PROC_CD=proc) for proc, df in self.data['LOSS_RATE_BY_EQP'].items() if not df.empty
            ], ignore_index=True)

            if df_all_loss.empty:
                print("[ERROR] LOSS_RATE_BY_EQP 통합 결과가 빈 데이터프레임")
                return {}

            df_all_loss['base_dt'] = pd.to_datetime(df_all_loss['base_dt'], format='%Y%m%d', errors='coerce')

        except Exception as e:
            print(f"[ERROR] df_all_loss 생성 실패: {e}")
            traceback.print_exc()
            return {}

        # ===================================================================
        # [5] 출력 디렉토리 생성 및 확인
        # ===================================================================
        PROJECT_ROOT = Path(__file__).parent.parent
        base_date = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
        debug_dir = PROJECT_ROOT / output_dir / base_date 
        
        if not debug_dir.exists():
            print(f"[ERROR] 디렉토리가 실제로 생성되지 않음: {debug_dir}")
            return {}
        else:
            print(f"[OK] 디렉토리 존재 확인")

        # ===================================================================
        # [6] 각 REJ_GROUP별 그래프 생성
        # ===================================================================
        graph_paths = {}  # { 'SCRATCH': [path1, path2, ...], 'BROKEN': [...], ... }

        for rej_group in valid_groups:
            gap_data = waf_gap_data.get(rej_group, {})
            if not gap_data:
                graph_paths[rej_group] = []
                continue

            # 상위 3개 장비 추출
            top3_eqps = []
            if isinstance(gap_data, dict) and isinstance(next(iter(gap_data.values()), {}), dict):
                for eqp_col, rates in gap_data.items():
                    sorted_rates = sorted(rates.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                    top3_eqps.extend([eqp for eqp, _ in sorted_rates])
            else:
                sorted_rates = sorted(gap_data.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                top3_eqps.extend([eqp for eqp, _ in sorted_rates])

            top3_eqps = list(dict.fromkeys(top3_eqps))[:3]  # 중복 제거 후 상위 3개

            if not top3_eqps:
                graph_paths[rej_group] = []
                continue

            # 데이터 필터링
            df_rej = df_all_loss[
                (df_all_loss['REJ_GROUP'] == rej_group) &
                (df_all_loss['eqp_name'].isin(top3_eqps))
            ][['base_dt', 'eqp_name', 'IN_QTY', 'LOSS_RATE']].copy()

            if df_rej.empty:
                graph_paths[rej_group] = []
                continue

            # 날짜 확보: 각 장비별로 reindex
            df_rej['base_dt_dt'] = pd.to_datetime(df_rej['base_dt'], format='%Y%m%d')
            df_rej = df_rej.sort_values('base_dt_dt')

            latest_date = df_rej['base_dt_dt'].max()
            start_date = latest_date - timedelta(days=59)
            all_dates = pd.date_range(start=start_date, end=latest_date, freq='D')
            all_dates_str = all_dates.strftime('%Y%m%d')

            # 장비별 그래프 생성
            eqp_graph_paths = []
            for eqp in top3_eqps:
                df_eqp = df_rej[df_rej['eqp_name'] == eqp].copy()

                if df_eqp.empty:
                    continue

                # 해당 장비만 reindex
                df_eqp = df_eqp.set_index('base_dt').reindex(all_dates_str, fill_value=0).reset_index()
                df_eqp['base_dt_dt'] = pd.to_datetime(df_eqp['index'], format='%Y%m%d')
                df_eqp = df_eqp.sort_values('base_dt_dt')

                # 그래프 생성
                plt.figure(figsize=(12, 6))
                ax1 = plt.gca()

                # 막대: IN_QTY
                ax1.bar(df_eqp['base_dt_dt'], df_eqp['IN_QTY'],
                        color='lightgray', alpha=0.7, label='IN_QTY', width=0.8)
                ax1.set_xlabel('Date', fontsize=12, fontweight='bold')
                ax1.set_ylabel('IN 수량', color='lightgray', fontsize=12, fontweight='bold')
                ax1.tick_params(axis='y', labelcolor='lightgray')
                ax1.grid(axis='y', linestyle='--', alpha=0.3)

                # 선: LOSS_RATE
                ax2 = ax1.twinx()
                loss_rate = df_eqp['LOSS_RATE'].values
                ax2.plot(df_eqp['base_dt_dt'], loss_rate,
                        marker='o', linestyle='-', linewidth=2, markersize=4,
                        color='darkred', label='LOSS_RATE')
                ax2.set_ylabel('불량률(%)', color='darkred', fontsize=12, fontweight='bold')
                ax2.tick_params(axis='y', labelcolor='darkred')
                ax2.set_ylim(0, max(loss_rate.max() * 1.5, 1))

                # 제목: 장비별
                plt.title(f'{rej_group} - {eqp} 불량률',
                        fontsize=14, fontweight='bold', pad=20)

                # 범례
                lines1, labels1 = ax1.get_legend_handles_labels()
                lines2, labels2 = ax2.get_legend_handles_labels()
                ax1.legend(lines1 + lines2, labels1 + labels2,
                        loc='upper left', fontsize=10, framealpha=0.9)

                # X축
                plt.xticks(rotation=45, ha='right')
                ax1.set_xlim(all_dates[0], all_dates[-1])
                plt.tight_layout()

                # 파일 저장: 장비명 포함
                safe_rej = "".join(c if c.isalnum() else "_" for c in rej_group)
                safe_eqp = "".join(c if c.isalnum() else "_" for c in eqp)
                filename = f"loss_rate_{safe_rej}_{safe_eqp}_{base_date}.png"
                filepath = debug_dir / filename

                # 기존 파일이 있으면 삭제
                if filepath.exists():
                    filepath.unlink()
                    print(f"[INFO] 기존 파일 삭제: {filepath}")
                plt.savefig(filepath, dpi=300, bbox_inches='tight')
                plt.close()

                eqp_graph_paths.append(str(filepath))
                print(f"[SUCCESS] 개별 그래프 생성: {filepath}")

            graph_paths[rej_group] = eqp_graph_paths  # 장비별 여러 그래프 저장

            # 결과 저장
            if not hasattr(self, 'report'):
                self.report = {}
            self.data['EQP_TREND_GRAPHS'] = graph_paths
            print(f"[OK] EQP_TREND_GRAPHS 저장: {list(graph_paths.keys())}")

        return graph_paths  # 반환 형식: {'SCRATCH': [path1, path2], 'BROKEN': [...], ...}


    def _export_to_excel(self, report, output_dir="./daily_reports_debug"):
        """Excel 보고서 생성 (기존 출력 형식과 동일하게 정확히 재현)"""
        try:

            # self.data → report 복사
            if hasattr(self, 'data'):
                eqp_graphs = self.data.get('EQP_TREND_GRAPHS')
                if eqp_graphs:
                    report['EQP_TREND_GRAPHS'] = eqp_graphs
                    print(f"[INFO] EQP_TREND_GRAPHS 복사됨: {list(eqp_graphs.keys())}")

            PROJECT_ROOT = Path(__file__).parent.parent
            base_date = (datetime.now().date() - timedelta(days=1))
            date_folder_name = base_date.strftime("%Y%m%d")
            debug_dir = PROJECT_ROOT / output_dir / date_folder_name
            debug_dir.mkdir(exist_ok=True, parents=True)

            excel_path = debug_dir / f"Daily_Report_{date_folder_name}.xlsx"

            # 기존 파일 삭제
            if excel_path.exists():
                try:
                    excel_path.unlink()
                    print(f"기존 파일 삭제됨: {excel_path}")
                except PermissionError:
                    raise PermissionError(f"엑셀을 닫고 다시 시도하세요: {excel_path}")

            wb = Workbook()
            ws = wb.active
            ws.title = "Prime 분석"

            # ──────────────────────────────────────────────────
            # 1. [3010 수율 분석] 제목 및 그래프 삽입 (가장 위)
            # ──────────────────────────────────────────────────
            ws.merge_cells('A1:G1')
            ws['A1'] = "[ WF RTY 수율 비교 (월/일) ]"
            ws['A1'].font = Font(size=14, bold=True)
            ws['A1'].alignment = Alignment(horizontal='left')

            data_3010_details = report.get('DATA_3010_wafering_300', {})
            chart_path_3010 = data_3010_details.get('chart_path')
            table_df_3010 = data_3010_details.get('table_df')


            # ──────────────────────────────────────────────────
            # [추가] 월/일 Total RTY Gap 요약 텍스트 생성
            # ──────────────────────────────────────────────────
            gap_month = None
            gap_daily = None

            if table_df_3010 is not None and not table_df_3010.empty:
                # Total RTY Gap 추출 (첫 번째 행)
                if 'Gap(월)' in table_df_3010.columns and len(table_df_3010) > 0:
                    try:
                        gap_month = float(table_df_3010.iloc[0]['Gap(월)'])
                        gap_daily = float(table_df_3010.iloc[0]['Gap(일)'])
                    except:
                        pass


            # 달성/미달 판정 함수
            def get_achieve_status(gap):
                if gap is None:
                    return "N/A"
                if gap < 0:
                    return "달성"
                elif gap > 0:
                    return "미달"
                else:
                    return "달성"  # 0 은 달성으로 간주

            # 텍스트 생성
            month_text = f"-. 월 : "
            if gap_month is not None:
                month_text += f"Total RTY {gap_month:.2f}% {get_achieve_status(gap_month)}"
            else:
                month_text += "데이터 없음"

            daily_text = f"-. 일 : "
            if gap_daily is not None:
                daily_text += f"Total RTY {gap_daily:.2f}% {get_achieve_status(gap_daily)}"
            else:
                daily_text += "데이터 없음"

            # A2, A3 에 요약 텍스트 출력
            ws['A2'] = month_text
            ws['A2'].font = Font(size=10, bold=False)
            ws['A2'].alignment = Alignment(horizontal='left')

            ws['A3'] = daily_text
            ws['A3'].font = Font(size=10, bold=False)
            ws['A3'].alignment = Alignment(horizontal='left')

            if not chart_path_3010:
                ws['A4'] = "[차트 없음: chart_path 없음]"
                ws['A4'].font = Font(size=10, color="FF0000")
                print("3010: 삽입할 chart_path 없음")
            else:
                chart_path_3010 = Path(chart_path_3010)
                if not chart_path_3010.exists():
                    ws['A4'] = f"[차트 파일 없음: {chart_path_3010.name}]"
                    ws['A4'].font = Font(size=10, color="FF0000")
                    print(f"3010: 차트 파일 없음: {chart_path_3010}")
                else:
                    try:
                        img = ExcelImage(str(chart_path_3010))
                        img.width = 600
                        img.height = 300
                        ws.add_image(img, 'A4')
                    except Exception as e:
                        ws['A4'] = f"[이미지 삽입 실패: {e}]"
                        ws['A4'].font = Font(size=10, color="FF0000")

            # 3010 표 삽입 (H2 \~ K6)

            if table_df_3010 is not None and not table_df_3010.empty:
                start_row = 4
                start_col = 8  # H열

                table_df_3010_fmt = table_df_3010.copy()
                pct_cols = ['월 목표', '월 실적', '일 목표', '일 실적', 'Gap(월)', 'Gap(일)']
                for col in pct_cols:
                    if col in table_df_3010_fmt.columns:
                        table_df_3010_fmt[col] = pd.to_numeric(table_df_3010_fmt[col], errors='coerce') / 100.0

                for r_idx, row in enumerate(dataframe_to_rows(table_df_3010_fmt, index=False, header=True), start_row):
                    for c_idx, value in enumerate(row, start_col):
                        cell = ws.cell(row=r_idx, column=c_idx, value=value)
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')

                        if r_idx == start_row:  # 헤더
                            cell.font = Font(bold=True, size=10)
                            cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        else:
                            if c_idx in [start_col + 5, start_col + 6]:  # Gap 컬럼
                                try:
                                    gap_val = float(value) if pd.notna(value) else 0.0
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=False, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=False, size=9)
                                except:
                                    pass
                            cell.number_format = '0.00%'
            else:
                ws['H2'] = "표 없음"
                ws['H2'].font = Font(size=10, color="FF0000")

            # ──────────────────────────────────────────────────
            # 2. [Prime 불량 목표 比 일실적 변동]
            # ──────────────────────────────────────────────────
            next_start_row = 20
            data_3210_details = report.get('DATA_3210_wafering_300_details', {})
            chart_path = data_3210_details.get('chart_path')

            ws.merge_cells(f'A{next_start_row}:D{next_start_row}')
            ws[f'A{next_start_row}'] = "[ Prime 불량 목표 比 일실적 변동 ]"
            ws[f'A{next_start_row}'].font = Font(size=14, bold=True)
            ws[f'A{next_start_row}'].alignment = Alignment(horizontal='left')

            if not chart_path:
                ws[f'A{next_start_row + 1}'] = "[차트 없음]"
                ws[f'A{next_start_row + 1}'].font = Font(size=10, color="FF0000")
            else:
                chart_path = Path(chart_path)
                if not chart_path.exists():
                    ws[f'A{next_start_row + 1}'] = f"[파일 없음: {chart_path.name}]"
                    ws[f'A{next_start_row + 1}'].font = Font(size=10, color="FF0000")
                else:
                    try:
                        img = ExcelImage(str(chart_path))
                        img.width = 600
                        img.height = 350
                        ws.add_image(img, f'A{next_start_row + 1}')
                    except Exception as e:
                        ws[f'A{next_start_row + 1}'] = f"[삽입 실패: {e}]"
                        ws[f'A{next_start_row + 1}'].font = Font(size=10, color="FF0000")

            # 요약 표 삽입 (G열)
            table_df_for_row_height = None
            if 'summary' in data_3210_details:
                table_df = data_3210_details['summary'][['REJ_GROUP', 'GOAL_RATIO_PCT', 'LOSS_RATIO_PCT', 'GAP_PCT']].copy()
                table_df.columns = ['구분', '목표(%)', '실적(%)', 'GAP(%)']
                for col in ['목표(%)', '실적(%)', 'GAP(%)']:
                    table_df[col] = table_df[col] / 100.0

                table_df_for_row_height = table_df
                start_row = next_start_row + 1
                start_col = 8

                for r_idx, row in enumerate(dataframe_to_rows(table_df, index=False, header=True), start_row):
                    for c_idx, value in enumerate(row, start_col):
                        cell = ws.cell(row=r_idx, column=c_idx, value=value)
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        if r_idx == start_row:
                            cell.font = Font(bold=True, size=9)
                            cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        else:
                            cell.number_format = '0.00%'
                            if c_idx == start_col + 3:  # GAP 열
                                try:
                                    gap_val = float(value)
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=False, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=False, size=9)
                                except:
                                    pass

                for row in range(start_row, start_row + len(table_df) + 1):
                    ws.row_dimensions[row].height = 18


            # ──────────────────────────────────────────────────
            # 3. [Prime 주요 열위 불량 세부코드 분석]
            # ──────────────────────────────────────────────────
            row_start = next_start_row + 20
            ws.merge_cells(f'A{row_start-1}:F{row_start-1}')
            ws[f'A{row_start-1}'] = "[ Prime 주요 열위 불량 세부코드 분석 Ref.(3개월) 比 일실적 변동 (상위 3개) ]"
            ws[f'A{row_start-1}'].font = Font(size=12, bold=True)
            ws[f'A{row_start-1}'].alignment = Alignment(horizontal='left')

            mid_analysis = report.get('DATA_3210_wafering_300_3months', {}).get('top3_midgroup_analysis', {})
            plot_paths = mid_analysis.get('plot_paths', {})
            group_tables = mid_analysis.get('tables', {})
            detailed_analysis = data_3210_details.get('detailed_analysis', [])

            # detailed_analysis 파싱
            groups = []
            current_group = None
            current_code_dict = {}
            for line in detailed_analysis:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("[") and "분석" in stripped:
                    if current_group and current_code_dict:
                        groups.append((current_group, current_code_dict.copy()))
                    current_group = stripped.strip("[]").replace(" 분석", "").strip()
                    current_code_dict = {}
                elif "열위 Lot" in stripped:
                    parts = stripped.split(" 열위 Lot", 1)
                    if len(parts) == 2:
                        code = parts[0].strip()
                        lot_info = parts[1].strip()
                        if code not in current_code_dict:
                            current_code_dict[code] = []
                        current_code_dict[code].append(lot_info)
                elif stripped.startswith("- "):
                    content = stripped[2:].strip()
                    if " " in content:
                        code = content.split(" ", 1)[0].strip()
                        lot_info = content[len(code):].strip()
                        if code not in current_code_dict:
                            current_code_dict[code] = []
                        current_code_dict[code].append(lot_info)
            if current_group and current_code_dict:
                groups.append((current_group, current_code_dict))

            formatted_analysis = []
            for idx, (group_name, code_dict) in enumerate(groups):
                formatted_analysis.append(f"{group_name} 분석")
                for code, lot_list in code_dict.items():
                    lot_str = ", ".join(lot_list)
                    formatted_analysis.append(f"{code} 열위 Lot: {lot_str}")
                if idx < len(groups) - 1:
                    formatted_analysis.extend([""] * 3)

            start_detail_row = row_start + 1
            for i, line in enumerate(formatted_analysis):
                ws.cell(row=start_detail_row + i, column=15, value=line).font = Font(size=10)

            for rej_group, plot_path in plot_paths.items():
                current_row = row_start
                if not Path(plot_path).exists():
                    ws.cell(row=current_row, column=1, value=f"{rej_group} 그래프 없음").font = Font(size=9, color="FF0000")
                    row_start += 4
                    continue

                try:
                    img = ExcelImage(plot_path)
                    img.width = 400
                    img.height = 200
                    ws.add_image(img, f'A{current_row}')
                except Exception as e:
                    ws.cell(row=current_row, column=1, value=f"{rej_group} 삽입 실패: {e}").font = Font(size=9, color="FF0000")

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

                table_df = group_tables.get(rej_group)
                if table_df is not None and not table_df.empty:
                    headers = ['MID_GROUP', '실적(%)', 'Ref(3개월)', 'Gap']
                    for c_idx, header in enumerate(headers, 8):
                        cell = ws.cell(row=current_row, column=c_idx, value=header)
                        cell.font = Font(bold=True, size=10)
                        cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                    table_df_fmt = table_df.copy()
                    for col in ['실적(%)', 'Ref(3개월)', 'Gap']:
                        if col in table_df_fmt.columns:
                            table_df_fmt[col] = pd.to_numeric(table_df_fmt[col], errors='coerce') / 100.0

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
                                    gap_val = float(value)
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=False, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=False, size=9)
                                except:
                                    pass
                    table_height = len(table_df) + 1
                else:
                    ws.cell(row=current_row, column=8, value=f"{rej_group} 표 없음").font = Font(size=9, color="FF0000")
                    table_height = 1

                row_start = current_row + max(len(group_lines), table_height) + 5


            # ──────────────────────────────────────────────────
            # 4. [RC/HG 보상 영향성 분석] 섹션
            # ──────────────────────────────────────────────────
            ws['A70'] = "[ RC/HG 보상 영향성 분석 ]"
            ws['A70'].font = Font(size=12, bold=True)
            ws['A70'].alignment = Alignment(horizontal='left')

            current_date = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
            debug_dir = PROJECT_ROOT / "daily_reports_debug" / current_date

            total_chart_path = debug_dir / "RC_HG_보상_전체.png"
            group_chart_paths = {
                'PARTICLE': debug_dir / "RC_HG_보상_PARTICLE.png",
                'FLATNESS': debug_dir / "RC_HG_보상_FLATNESS.png",
                'WARP&BOW': debug_dir / "RC_HG_보상_WARP&BOW.png",
                'NANO': debug_dir / "RC_HG_보상_NANO.png"
            }

            data_lot_details = report.get('DATA_LOT_3210_wafering_300_details', {})
            loss_rate_table_total = data_lot_details.get('summary')  # 전체 표
            loss_rate_table_by_group = data_lot_details.get('loss_rate_table_by_group', {})  # 그룹별 표

            current_row = 72
            SECTION_HEIGHT = 9

            def safe_pct_to_float(x):
                try:
                    if pd.isna(x) or x == '' or x is None:
                        return 0.0
                    # %만 제거, 부호는 유지
                    cleaned = str(x).strip().replace('%', '')
                    if cleaned == '':
                        return 0.0
                    # float 변환 시 자동으로 +, - 처리
                    return float(cleaned) / 100.0
                except Exception as e:
                    print(f"[ERROR] safe_pct_to_float 변환 실패: {x} → {e}")
                    return 0.0

            # 값 변환 함수 (% 문자열 제거 → float)
            def parse_pct_value(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                # 문자열이면 % 제거
                if isinstance(val, str):
                    val = val.replace('%', '').strip()
                try:
                    return float(val)
                except:
                    return None

            def create_group_comment(ws, row_addr, label, table_row):
                """그룹별 코멘트 생성 (RichText 대신 셀 분리 방식)"""
                try:
                    raw_daily = table_row.iloc[0]['일%']
                    raw_ref = table_row.iloc[0]['Ref.(3개월)%']
                    raw_gap = table_row.iloc[0]['Gap']

                    daily_rate = parse_pct_value(raw_daily)
                    ref_rate = parse_pct_value(raw_ref)
                    gap_val = parse_pct_value(raw_gap)

                    if any(v is None for v in [daily_rate, ref_rate, gap_val]):
                        raise ValueError("값 파싱 실패")

                    prefix = f" - {label} 보상률 {daily_rate:+.2f}%, Ref {ref_rate:+.2f}%, 比 "

                    # gap_val 기준으로 텍스트와 상태 결정
                    if gap_val < 0:
                        status = "열위"
                        gap_color = "FF0000"  # 빨간색
                    elif gap_val > 0:
                        status = "양호"
                        gap_color = "0000FF"  # 파란색
                    else:
                        status = "변화없음"
                        gap_color = "000000"  # 검은색      

                    gap_text = f"{gap_val:+.2f}%p {status}"

                    # A 열: 접두사
                    ws[f'A{row_addr}'] = prefix
                    ws[f'A{row_addr}'].font = Font(size=9, color="000000")

                    # B 열: Gap 값 (색상 적용)
                    ws[f'C{row_addr}'] = gap_text
                    gap_color = "FF0000" if gap_val > 0 else "0000FF" if gap_val < 0 else "000000"
                    ws[f'C{row_addr}'].font = Font(size=9, color=gap_color, bold=True)


                    return True
                except Exception as e:
                    print(f"[ERROR] {label} 코멘트 생성 실패: {e}")
                    ws[f'A{row_addr}'] = f" - [{label}: {str(e)[:20]}]"
                    ws[f'A{row_addr}'].font = Font(size=9, color="FF0000")
                    return False

            # 전체 그래프 + 표
            if total_chart_path.exists():
                # 코멘트 삽입: 그래프 바로 위
                comment_row = current_row - 1  # 그래프는 current_row 에 삽입 → 그 위에 코멘트
                ws[f'A{comment_row}'] = "Total"
                ws[f'A{comment_row}'].font = Font(size=10, bold=True)

                if isinstance(loss_rate_table_total, pd.DataFrame) and not loss_rate_table_total.empty:
                    resc_row = loss_rate_table_total[loss_rate_table_total['구분'] == 'RESC']
                    hg_row = loss_rate_table_total[loss_rate_table_total['구분'] == 'HG']

                    # 코멘트 시작 행
                    comment_row += 1

                    # RESC 처리
                    if not resc_row.empty:
                        create_group_comment(ws, comment_row, 'RC', resc_row)
                        comment_row += 1

                    else:
                        ws[f'A{comment_row}'] = " - [RESC 데이터 없음]"
                        ws[f'A{comment_row}'].font = Font(size=9, color="808080")
                        comment_row += 1

                    # HG 처리
                    if not hg_row.empty:
                        create_group_comment(ws, comment_row, 'HG', hg_row)
                        comment_row += 1

                    else:
                        ws[f'A{comment_row}'] = " - [HG 데이터 없음]"
                        ws[f'A{comment_row}'].font = Font(size=9, color="808080")
                        comment_row += 1

                graph_row = comment_row + 1
                if total_chart_path.exists():
                    try:
                        img = ExcelImage(str(total_chart_path))
                        img.width = 400
                        img.height = 200
                        ws.add_image(img, f'A{current_row+3}')
                    except Exception as e:
                        ws[f'A{current_row+3}'] = f"[RC/HG 전체 그래프 삽입 실패: {e}]"
                        ws[f'A{current_row+3}'].font = Font(size=10, color="FF0000")

                else:
                    ws[f'A{graph_row}'] = "[전체 그래프 파일 없음]"
                    ws[f'A{graph_row}'].font = Font(size=10, color="FF0000")

                if isinstance(loss_rate_table_total, pd.DataFrame) and not loss_rate_table_total.empty:
                    headers = ['구분', 'Ref.(3개월)', '일', 'Ref.(3개월)%', '일%', 'Gap']
                    start_row = graph_row
                    start_col = 8

                    for c_idx, header in enumerate(headers, start_col):
                        cell = ws.cell(row=start_row, column=c_idx, value=header)
                        cell.font = Font(bold=True, size=10)
                        cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                    table_total_fmt = loss_rate_table_total.copy()
                    pct_columns = ['Ref.(3개월)%', '일%', 'Gap']
                    for col in pct_columns:
                        if col in table_total_fmt.columns:
                            table_total_fmt[col] = table_total_fmt[col].apply(safe_pct_to_float)

                    for r_idx, row in enumerate(dataframe_to_rows(table_total_fmt, index=False, header=False), start_row + 1):
                        for c_idx, value in enumerate(row, start_col):
                            cell = ws.cell(row=r_idx, column=c_idx, value=value)
                            cell.font = Font(size=9)
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                                top=Side(style='thin'), bottom=Side(style='thin'))
                            if c_idx in [11, 12, 13]:
                                cell.number_format = '0.00%'
                            if c_idx == 13:
                                try:
                                    gap_val = float(value)
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=False, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=False, size=9)
                                except:
                                    pass

                    table_height = len(loss_rate_table_total) + 1                        

                else:
                    ws.cell(row=graph_row, column=8, value="[RC/HG 전체 표 없음]").font = Font(size=10, color="FF0000")
                    table_height = 1

                current_row = graph_row + 10

            # ───────────────────────────────────────────────
            # 2. 그룹별 그래프 + 코멘트 + 표 (PARTICLE, FLATNESS, WARP&BOW, NANO)
            # ───────────────────────────────────────────────
            for group_idx, group in enumerate(['PARTICLE', 'FLATNESS', 'WARP&BOW', 'NANO']):
                chart_path = group_chart_paths[group]
                table_data = loss_rate_table_by_group.get(group)
                
                # 그룹 제목
                ws[f'A{current_row}'] = f"{group}"
                ws[f'A{current_row}'].font = Font(size=10, bold=True)
                comment_row = current_row + 1

                # 그룹별 코멘트 (RESC, HG)
                if isinstance(table_data, pd.DataFrame) and not table_data.empty:
                    resc_row = table_data[table_data['구분'] == 'RESC']
                    hg_row = table_data[table_data['구분'] == 'HG']

                    if not resc_row.empty:
                        create_group_comment(ws, comment_row, 'RC', resc_row)
                        comment_row += 1
                    else:
                        ws[f'A{comment_row}'] = " - [RESC 데이터 없음]"
                        ws[f'A{comment_row}'].font = Font(size=9, color="808080")
                        comment_row += 1

                    if not hg_row.empty:
                        create_group_comment(ws, comment_row, 'HG', hg_row)
                        comment_row += 1
                    else:
                        ws[f'A{comment_row}'] = " - [HG 데이터 없음]"
                        ws[f'A{comment_row}'].font = Font(size=9, color="808080")
                        comment_row += 1

                # 그래프 삽입 (코멘트 아래)
                graph_row = comment_row + 1
                if chart_path.exists():
                    try:
                        img = ExcelImage(str(chart_path))
                        img.width = 400
                        img.height = 200
                        ws.add_image(img, f'A{graph_row}')
                    except Exception as e:
                        ws[f'A{graph_row}'] = f"[RC/HG {group} 그래프 삽입 실패: {e}]"
                        ws[f'A{graph_row}'].font = Font(size=10, color="FF0000")
                else:
                    ws[f'A{graph_row}'] = f"[{group} 그래프 파일 없음]"
                    ws[f'A{graph_row}'].font = Font(size=10, color="FF0000")

                # 표 삽입 (그래프 오른쪽, H 열부터)
                if isinstance(table_data, pd.DataFrame) and not table_data.empty:
                    headers = ['구분', 'Ref.(3 개월)', '일', 'Ref.(3개월)%', '일%', 'Gap']
                    table_start_row = graph_row
                    start_col = 8

                    for c_idx, header in enumerate(headers, start_col):
                        cell = ws.cell(row=table_start_row, column=c_idx, value=header)
                        cell.font = Font(bold=True, size=10)
                        cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                    table_group_fmt = table_data.copy()
                    for col in ['Ref.(3개월)%', '일%', 'Gap']:
                        if col in table_group_fmt.columns:
                            table_group_fmt[col] = table_group_fmt[col].apply(safe_pct_to_float)

                    for r_idx, row in enumerate(dataframe_to_rows(table_group_fmt, index=False, header=False), table_start_row + 1):
                        for c_idx, value in enumerate(row, start_col):
                            cell = ws.cell(row=r_idx, column=c_idx, value=value)
                            cell.font = Font(size=9)
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                                top=Side(style='thin'), bottom=Side(style='thin'))
                            if c_idx in [11, 12, 13]:
                                cell.number_format = '0.00%'
                            if c_idx == 13:
                                try:
                                    gap_val = float(value)
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=False, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=False, size=9)
                                except:
                                    pass

                    table_height = len(table_data) + 1
                else:
                    ws.cell(row=graph_row, column=8, value=f"[{group} 표 없음]").font = Font(size=10, color="FF0000")
                    table_height = 1

                # 다음 그룹 시작 행 (그래프 높이 + 코멘트 + 표 + 여유)
                current_row = graph_row + 10  # 그래프 높이 (200px ≈ 15 행) + 여유


            # ──────────────────────────────────────────────────
            # 5. [ 제품 영향성 분석 ] 섹션
            # ──────────────────────────────────────────────────
            current_row = current_row + 1
            ws[f'A{current_row}'] = "[ 제품 영향성 분석 ]"
            ws[f'A{current_row}'].font = Font(size=12, bold=True)
            current_row += 1

            product_influence_gap = report.get('product_influence_gap')
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
                    chart1_path = debug_dir / f"{rej_group}_물량비_불량GAP_temp.png"
                    try:
                        fig1, ax1 = plt.subplots(figsize=(6, 4))
                        x = [str(row['PRODUCT_TYPE']) for _, row in df_group.iterrows()]
                        y = [float(pd.to_numeric(row['물량비_불량GAP'], errors='coerce')) for _, row in df_group.iterrows()]
                        bars = ax1.bar(x, y, color='orange')

                        ax1.set_title(f'{rej_group} 제품 Ref. 물량 비 불량 변동', fontsize=12, fontweight='bold')
                        ax1.set_xlabel('제품', fontsize=10)
                        ax1.set_ylabel('물량비_불량GAP', fontsize=10)
                        ax1.tick_params(axis='x', rotation=0)
                        ax1.grid(axis='y', linestyle='--', alpha=0.7)

                        for i, (bar, val) in enumerate(zip(bars, y)):
                            height = bar.get_height()
                            # 음수는 아래, 양수는 위에 표시
                            va = 'top' if val >= 0 else 'bottom'
                            pos_y = height + (0.005 if val >= 0 else -0.005)
                            ax1.text(bar.get_x() + bar.get_width() / 2, pos_y,
                                    f'{val:+.2f}%', ha='center', va=va, fontsize=9, fontweight='bold', color='black')

                        plt.tight_layout()
                        plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
                        plt.close()
                        if chart1_path.exists():
                            img1 = ExcelImage(str(chart1_path))
                            img1.width = 400
                            img1.height = 200
                            ws.add_image(img1, f'A{current_row}')
                    except Exception as e:
                        ws[f'A{current_row}'] = f"[그래프1 생성 실패: {e}]"
                        ws[f'A{current_row}'].font = Font(size=10, color="FF0000")

                    chart2_path = debug_dir / f"{rej_group}_물량비_GAP_temp.png"
                    try:
                        fig2, ax2 = plt.subplots(figsize=(6, 4))
                        x = [str(row['PRODUCT_TYPE']) for _, row in df_group.iterrows()]
                        y = [float(pd.to_numeric(row['물량비_GAP(%)'], errors='coerce')) for _, row in df_group.iterrows()]
                        bars = ax2.bar(x, y, color='orange')

                        ax2.set_title(f'{rej_group} 제품 Ref. 비 물량 변동', fontsize=12, fontweight='bold')
                        ax2.set_xlabel('제품', fontsize=10)
                        ax2.set_ylabel('물량비_GAP(%)', fontsize=10)
                        ax2.tick_params(axis='x', rotation=0)
                        ax2.grid(axis='y', linestyle='--', alpha=0.7)

                        for i, (bar, val) in enumerate(zip(bars, y)):
                            height = bar.get_height()
                            # 음수는 아래, 양수는 위에 표시
                            va = 'top' if val >= 0 else 'bottom'
                            pos_y = height + (0.005 if val >= 0 else -0.005)
                            ax1.text(bar.get_x() + bar.get_width() / 2, pos_y,
                                    f'{val:+.2f}%', ha='center', va=va, fontsize=9, fontweight='bold', color='black')

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

                    table_start_row = 160
                    headers = ['제품', 'Ref. 제품 불량률', '물량비 Gap', 'Ref.(6개월) 물량비', '일 물량비', 'Ref.(6개월) 수량', '일 수량']
                    for c_idx, header in enumerate(headers, 1):
                        cell = ws.cell(row=table_start_row, column=c_idx, value=header)
                        cell.font = Font(bold=True, size=10)
                        cell.fill = PatternFill("solid", fgColor="D3D3D3")
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                    table_data = []
                    for _, row in df_group.iterrows():
                        table_data.append({
                            '제품': row['PRODUCT_TYPE'],
                            'Ref. 제품 불량률': row['Ref_불량률(%)'],
                            '물량비 Gap': row['물량비_GAP(%)'],
                            'Ref.(6개월) 물량비': row['Ref_물량비(%)'],
                            '일 물량비': row['Daily_물량비(%)'],
                            'Ref.(6개월) 수량': row['Ref_Compile_수량'],
                            '일 수량': row['Daily_Compile_수량']
                        })
                    table_df = pd.DataFrame(table_data, columns=headers)

                    table_df_fmt = table_df.copy()
                    pct_columns = ['Ref. 제품 불량률', '물량비 Gap', 'Ref.(6개월) 물량비', '일 물량비']
                    for col in pct_columns:
                        if col in table_df_fmt.columns:
                            table_df_fmt[col] = pd.to_numeric(table_df_fmt[col], errors='coerce') / 100.0

                    for r_idx, row in enumerate(dataframe_to_rows(table_df_fmt, index=False, header=False), table_start_row + 1):
                        for c_idx, value in enumerate(row, 1):
                            cell = ws.cell(row=r_idx, column=c_idx, value=value)
                            cell.font = Font(size=9)
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                                top=Side(style='thin'), bottom=Side(style='thin'))
                            if c_idx in [2,3,4,7,8]:
                                cell.number_format = '0.00%'
                            if c_idx in [3,4]:
                                try:
                                    gap_val = float(value)
                                    if gap_val > 0:
                                        cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                        cell.font = Font(color="FF0000", bold=False, size=9)
                                    elif gap_val < 0:
                                        cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                        cell.font = Font(color="0000FF", bold=False, size=9)
                                except:
                                    pass

                    for row in range(table_start_row, table_start_row + len(table_data) + 1):
                        ws.row_dimensions[row].height = 18

                    current_row = table_start_row + len(table_data) + 3


            # ──────────────────────────────────────────────────
            # 6. [장비별 불량률 GAP 분석]
            # ──────────────────────────────────────────────────
            current_row = current_row + 5
            ws[f'A{current_row}'] = "[장비별 불량률 GAP 분석]"
            ws[f'A{current_row}'].font = Font(size=12, bold=True)
            current_row += 2
            waf_analysis_details = report.get('DATA_WAF_3210_wafering_300_details', {})

            # waf_analysis_ref가 dict가 아니면 빈 dict
            waf_ref_data = waf_analysis_details.get('waf_analysis_ref')
            if not isinstance(waf_ref_data, dict):
                print(f"[ERROR] waf_analysis_ref type 오류: {type(waf_ref_data)} → 빈 dict로 대체")
                waf_ref_data = {}

            waf_daily_data = waf_analysis_details.get('waf_analysis_daily')
            if not isinstance(waf_daily_data, dict):
                print(f"[ERROR] waf_analysis_daily type 오류: {type(waf_daily_data)} → 빈 dict로 대체")
                waf_daily_data = {}

            waf_gap_data = waf_analysis_details.get('waf_analysis_gap', {})
            if not isinstance(waf_gap_data, dict):
                waf_gap_data = {}


            # top3_rej_groups = ['PIT', 'EDGE', 'VISUAL']
            top3_rej_groups = report.get('DATA_3210_wafering_300_details', {}).get('top3_rej_groups', [])
            valid_groups = [g for g in top3_rej_groups if g in ['PIT', 'SCRATCH', 'EDGE', 'BROKEN', 'CHIP', 'VISUAL']]

            if not valid_groups:
                ws[f'A{current_row}'] = "[WAF 분석: 상위 3개 그룹 중 대상 없음]"
                ws[f'A{current_row}'].font = Font(size=10, color="FF0000")
                current_row += 10
            else:

                # ───────────────────────────────────────────────
                # 0단계: 전체 GAP 값 수집 → Y축 통일을 위해
                # ───────────────────────────────────────────────
                all_gap_values = []
                for rej_group in valid_groups:
                    if rej_group not in waf_gap_data:
                        continue
                    gap_data = waf_gap_data[rej_group]
                    if not gap_data:
                        continue

                    if isinstance(gap_data, dict) and isinstance(next(iter(gap_data.values()), {}), dict):
                        sorted_eqps = sorted(
                            gap_data.items(),
                            key=lambda x: abs(sum(v for v in x[1].values())),
                            reverse=True
                        )[:3]

                        for eqp_col, rates in sorted_eqps:
                            if not rates:
                                continue
                            for val in rates.values():
                                all_gap_values.append(val)
                    
                    else:
                      print(f"[WARNING] {rej_group}: gap_data 구조 오류 - {type(gap_data)}")

                # 전체 min/max 계산
                if all_gap_values:
                    global_min = min(all_gap_values)
                    global_max = max(all_gap_values)
                    margin = max(0.0001, abs(global_max - global_min) * 0.2)
                    y_min = global_min - margin
                    y_max = global_max + margin

                    # 0 포함 보장
                    if y_min > 0:
                        y_min = -margin
                    if y_max < 0:
                        y_max = margin
                else:
                    y_min, y_max = -0.0005, 0.0005  # 기본값

                # ───────────────────────────────────────────────
                # 1단계: 그래프 생성 및 가로 배치 (최대 3개/행)
                # ───────────────────────────────────────────────
                graph_start_row = current_row
                current_graph_row = graph_start_row
                graphs_in_row = 0  # 현재 행에 삽입된 그래프 수 (0\~3)
                current_defect_group = None  # 현재 불량 그룹 추적
                graphs_created = 0  # 생성된 그래프 수 추적

                # 열 번호 → 문자 변환 함수 (get_column_letter 대체)
                def col_num_to_letter(n):
                    if n <= 0:
                        return 'A'
                    result = ''
                    while n > 0:
                        n -= 1
                        result = chr(65 + (n % 26)) + result
                        n //= 26
                    return result

                for rej_group in valid_groups:
                    if rej_group not in waf_gap_data:
                        print(f"[WARNING] {rej_group}: waf_gap_data 에 없음")
                        # 데이터 없으면 메시지 표시 (선택사항)
                        ws[f'A{current_graph_row}'] = f"[{rej_group}: 분석 데이터 없음]"
                        ws[f'A{current_graph_row}'].font = Font(size=10, color="FF0000")
                        current_graph_row += 2
                        continue
                    gap_data = waf_gap_data[rej_group]
                    if not gap_data:
                        print(f"[WARNING] {rej_group}: gap_data 가 비어있음")
                        ws[f'A{current_graph_row}'] = f"[{rej_group}: 분석 데이터 없음]"
                        ws[f'A{current_graph_row}'].font = Font(size=10, color="FF0000")
                        current_graph_row += 2
                        continue

                    # 불량 그룹이 바뀌면 무조건 다음 행으로
                    if current_defect_group is not None and rej_group != current_defect_group:
                        if graphs_in_row > 0:
                            current_graph_row += 10
                            graphs_in_row = 0
                    
                    current_defect_group = rej_group

                    # ───────────────────────────────────────────────
                    # 모든 그룹을 중첩 dict 구조로 통일 처리
                    # → VISUAL, PIT, SCRATCH 등 모두 동일하게 처리
                    # ───────────────────────────────────────────────

                    if isinstance(gap_data, dict) and isinstance(next(iter(gap_data.values()), {}), dict):
                        sorted_eqps = sorted(gap_data.items(), key=lambda x: abs(sum(v for v in x[1].values())), reverse=True)[:3]

                        for eqp_col, rates in sorted_eqps:
                            # 공정명 추출 (예: 3670)
                            proc = eqp_col[-4:] if eqp_col[-4:].isdigit() else eqp_col

                            # 빈 데이터 체크
                            if not rates:
                                continue

                            # 그래프 파일 경로
                            safe_rej = "".join(c if c.isalnum() else "_" for c in rej_group)
                            safe_eqp = "".join(c if c.isalnum() else "_" for c in proc)
                            chart_path = debug_dir / f"WAF_{safe_rej}_{safe_eqp}_gap_chart.png"
                            
                            # 기존 파일 있으면 삭제
                            if chart_path.exists():
                                chart_path.unlink()
                                print(f"[INFO] 기존 파일 삭제: {chart_path}")

                            try:
                                fig, ax = plt.subplots(figsize=(6, 4))
                                sorted_rates = sorted(rates.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                                labels = [k for k, v in sorted_rates]
                                values = [v for k, v in sorted_rates]  # 0.0144 소수
                                colors = ['orange' if v > 0 else 'steelblue' if v < 0 else 'gray' for v in values]

                                ax.bar(labels, values, color=colors, width=0.6)
                                ax.set_title(f'{rej_group} - 공정 {proc}', fontsize=12, fontweight='bold')
                                ax.set_ylabel('GAP (%)', fontsize=10)
                                ax.set_xlabel('장비', fontsize=10)
                                ax.tick_params(axis='x', rotation=0)
                                ax.grid(axis='y', linestyle='--', alpha=0.7)

                                # Y축 범위
                                ax.set_ylim(y_min, y_max)
                                ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))

                                # 값 표시
                                for i, (label, val) in enumerate(zip(labels, values)):
                                    ax.text(i, val + (margin * 0.1 if val >= 0 else -margin * 0.1),
                                            f"{val * 100 :+.2f}%", ha='center', va='bottom' if val >= 0 else 'top',
                                            fontsize=9, fontweight='bold', color='black')

                                plt.tight_layout()
                                plt.savefig(chart_path, dpi=150, bbox_inches='tight')
                                plt.close()

                                if chart_path.exists():
                                    img = ExcelImage(str(chart_path))
                                    img.width = 400
                                    img.height = 200

                                    # 3개 초과 시 다음 행으로
                                    if graphs_in_row >= 3:
                                        current_graph_row += 10  # 다음 행 (10행 간격)
                                        graphs_in_row = 0

                                    col_offset = graphs_in_row * 5  # 0→0, 1→5, 2→10
                                    col_letter = col_num_to_letter(1 + col_offset)  # A, G, M
                                    ws.add_image(img, f'{col_letter}{current_graph_row}')
                                    graphs_in_row += 1

                                else:
                                    col_offset = graphs_in_row * 5
                                    col_letter = col_num_to_letter(1 + col_offset)
                                    ws[f'{col_letter}{current_graph_row}'] = f"[{rej_group}-{proc} 파일 없음]"
                                    graphs_in_row += 1

                            except Exception as e:
                                col_offset = graphs_in_row * 5
                                col_letter = col_num_to_letter(1 + col_offset)
                                ws[f'{col_letter}{current_graph_row}'] = f"[{rej_group}-{proc} 실패]"
                                ws[f'{col_letter}{current_graph_row}'].font = Font(size=9, color="FF0000")
                                graphs_in_row += 1
                    else:
                        print(f"[WARNING] {rej_group}: gap_data 구조 오류 - {type(gap_data)}")

                # 그래프가 하나도 없으면 메시지
                if graphs_created == 0:
                    ws[f'A{current_graph_row}'] = "[분석할 그래프 데이터 없음]"
                    ws[f'A{current_graph_row}'].font = Font(size=10, color="FF0000")
                    current_graph_row += 2

                # ───────────────────────────────────────────────
                # 2 단계: 모든 그래프 아래에 통합 표 생성 (A 열부터)
                # ───────────────────────────────────────────────
                table_start_row = current_graph_row  + 10  # 그래프 아래 여유 공간

                # 헤더
                headers = ['불량','구분', '장비', 'Ref.(3 개월)', '일', 'Ref.(3 개월)', '일', 'Gap']
                for c_idx, header in enumerate(headers, 1):  # A 열부터 (1)
                    cell = ws.cell(row=table_start_row, column=c_idx, value=header)
                    cell.font = Font(bold=True, size=9)
                    cell.fill = PatternFill("solid", fgColor="D3D3D3")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))

                # 데이터 수집 (모든 그룹/공정 통합)
                all_table_rows = []
                for rej_group in valid_groups:
                    if rej_group not in waf_gap_data:
                        continue
                    gap_data = waf_gap_data[rej_group]
                    if not gap_data:
                        continue

                    # ref_dict 추출 + 강제 dict 보정
                    ref_dict_raw = waf_ref_data.get(rej_group)
                    if not isinstance(ref_dict_raw, dict):
                        print(f"[WARNING] ref_dict for {rej_group} is not dict: {type(ref_dict_raw)} → 빈 dict")
                        ref_dict_raw = {}

                    daily_dict_raw = waf_daily_data.get(rej_group, {})
                    if not isinstance(daily_dict_raw, dict):
                        print(f"[WARNING] daily_dict for {rej_group} is not dict: {type(daily_dict_raw)} → 빈 dict")
                        daily_dict_raw = {}

                    if isinstance(gap_data, dict) and isinstance(next(iter(gap_data.values()), {}), dict):
                        for eqp_col, rates in gap_data.items():
                            # 공정 코드 추출
                            proc = eqp_col[-4:] if eqp_col[-4:].isdigit() else eqp_col

                            if not rates:
                                continue

                            eqp_rows = []
                            for eqp_name in rates.keys():
                                # ref: ref_dict_raw[eqp_col][eqp_name]
                                level1 = ref_dict_raw.get(eqp_col)
                                if isinstance(level1, dict):
                                    ref_eqp_data = level1.get(eqp_name, {})
                                else:
                                    ref_eqp_data = ref_dict_raw.get(eqp_name, {})  # fallback

                                if isinstance(ref_eqp_data, dict):
                                    ref_count = ref_eqp_data.get('count', 0)
                                    ref_rate_val = ref_eqp_data.get('rate', 0.0)
                                else:
                                    ref_count = 0
                                    ref_rate_val = 0.0

                                # daily
                                level1_daily = daily_dict_raw.get(eqp_col)
                                if isinstance(level1_daily, dict):
                                    daily_eqp_data = level1_daily.get(eqp_name, {})
                                else:
                                    daily_eqp_data = daily_dict_raw.get(eqp_name, {})

                                if isinstance(daily_eqp_data, dict):
                                    daily_count = daily_eqp_data.get('count', 0)
                                    daily_rate_val = daily_eqp_data.get('rate', 0.0)
                                else:
                                    daily_count = 0
                                    daily_rate_val = 0.0

                                gap_val = rates.get(eqp_name, 0.0)

                                eqp_rows.append({
                                    '불량': rej_group,
                                    '구분': proc,
                                    '장비': eqp_name,
                                    'Ref_Count': ref_count,
                                    'Daily_Count': daily_count,
                                    'Ref_rate': ref_rate_val / 100.0,
                                    'Daily_rate': daily_rate_val / 100.0,
                                    'Gap': gap_val
                                })

                            # Gap 절대값 기준 상위 3개
                            eqp_rows_sorted = sorted(eqp_rows, key=lambda x: abs(x['Gap']), reverse=True)[:3]
                            all_table_rows.extend(eqp_rows_sorted)

                # 표 데이터 작성 + 구분 병합
                if all_table_rows:
                    current_defect = None  # 불량코드
                    current_process = None  # 공정코드
                    defect_merge_start = None
                    process_merge_start = None

                    for r_idx, row in enumerate(all_table_rows, table_start_row + 1):
                        if row['불량'] != current_defect:
                            if current_defect is not None and defect_merge_start is not None:
                                merge_end_row = r_idx - 1
                                if merge_end_row > defect_merge_start:
                                    ws.merge_cells(f'A{defect_merge_start}:A{merge_end_row}')
                                    ws[f'A{defect_merge_start}'].alignment = Alignment(horizontal='center', vertical='center')
                            current_defect = row['불량']
                            defect_merge_start = r_idx

                        # 구분 병합 처리
                        if row['구분'] != current_process:
                            if current_process is not None and process_merge_start is not None:
                                merge_end_row = r_idx - 1
                                if merge_end_row > process_merge_start:
                                    ws.merge_cells(f'B{process_merge_start}:B{merge_end_row}')
                                    ws[f'B{process_merge_start}'].alignment = Alignment(horizontal='center', vertical='center')
                            current_process = row['구분']
                            process_merge_start = r_idx

                        # A 열: 불량
                        cell = ws.cell(row=r_idx, column=1, value=row['불량'])
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # B 열: 구분 (공정코드)
                        cell = ws.cell(row=r_idx, column=2, value=row['구분'])
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # C 열: 장비 (column=3)
                        cell = ws.cell(row=r_idx, column=3, value=row['장비'])
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # D 열: Ref.(3 개월) 수량 (column=4)
                        cell = ws.cell(row=r_idx, column=4, value=row['Ref_Count'])
                        cell.number_format = '#,##0'
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # E 열: 일 수량 (column=5)
                        cell = ws.cell(row=r_idx, column=5, value=row['Daily_Count'])
                        cell.number_format = '#,##0'
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # F 열: Ref.(3 개월) 불량률 (column=6)
                        cell = ws.cell(row=r_idx, column=6, value=row['Ref_rate'])
                        cell.number_format = '0.00%'
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # G 열: 일 불량률 (column=7)
                        cell = ws.cell(row=r_idx, column=7, value=row['Daily_rate'])
                        cell.number_format = '0.00%'
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                        # H 열: Gap (column=8)
                        cell = ws.cell(row=r_idx, column=8, value=row['Gap'])
                        cell.number_format = '+0.00%;-0.00%;0.00%'
                        cell.font = Font(size=9)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))
                        try:
                            gap_num = row['Gap']
                            if gap_num > 0:
                                cell.fill = PatternFill("solid", fgColor="FFCCCC")
                                cell.font = Font(color="FF0000", bold=False, size=9)
                            elif gap_num < 0:
                                cell.fill = PatternFill("solid", fgColor="CCE5FF")
                                cell.font = Font(color="0000FF", bold=False, size=9)
                        except:
                            pass

                    # 마지막 병합 (불량)
                    if current_defect is not None and defect_merge_start is not None:
                        merge_end_row = table_start_row + len(all_table_rows)
                        if merge_end_row > defect_merge_start:
                            ws.merge_cells(f'A{defect_merge_start}:A{merge_end_row}')
                            ws[f'A{defect_merge_start}'].alignment = Alignment(horizontal='center', vertical='center')

                    # 마지막 병합 (구분)
                    if current_process is not None and process_merge_start is not None:
                        merge_end_row = table_start_row + len(all_table_rows)
                        if merge_end_row > process_merge_start:
                            ws.merge_cells(f'B{process_merge_start}:B{merge_end_row}')
                            ws[f'B{process_merge_start}'].alignment = Alignment(horizontal='center', vertical='center')

                    # ───────────────────────────────────────────────
                    # 맨 아래에 모수 행 삽입
                    # ───────────────────────────────────────────────
                    last_row = table_start_row + len(all_table_rows) + 1  # 마지막 데이터 아래

                    avg_in_qty = getattr(self, 'avg_in_qty', 0)
                    total_daily_qty = getattr(self, 'total_daily_qty', 0)

                    # 강제 float 변환 (Decimal → float)
                    try:
                        avg_in_qty = float(avg_in_qty)
                        total_daily_qty = float(total_daily_qty)
                    except (TypeError, ValueError) as e:
                        return 

                    # A 열: 구분: "모수" (병합: A-B)
                    cell = ws.cell(row=last_row, column=1, value="모수")
                    cell.font = Font(size=9, bold=True)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))
                    ws.merge_cells(f'A{last_row}:B{last_row}')  # 🔧 A-B 병합

                    # C 열: 장비: 빈칸 (자동)
                    cell = ws.cell(row=last_row, column=3, value="")
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))

                    # D 열: Ref.(3 개월): avg_in_qty (column=4)
                    cell = ws.cell(row=last_row, column=4, value=avg_in_qty)
                    cell.number_format = '#,##0'
                    cell.font = Font(size=9)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))

                    # E 열: 일: total_daily_qty (column=5)
                    cell = ws.cell(row=last_row, column=5, value=total_daily_qty)
                    cell.number_format = '#,##0'
                    cell.font = Font(size=9)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                        top=Side(style='thin'), bottom=Side(style='thin'))

                    # F, G, H 열: 빈칸 (column=6, 7, 8)
                    for col in [6, 7, 8]:
                        cell = ws.cell(row=last_row, column=col, value="")
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'),
                                            top=Side(style='thin'), bottom=Side(style='thin'))

                current_row = last_row + 2

                
            # ──────────────────────────────────────────────────
            # 7. [장비별 불량률 추세 분석] - _plot_rej_group_top3_eqp_trend 결과 삽입
            # ──────────────────────────────────────────────────
            current_row += 5
            ws[f'A{current_row}'] = "[장비별 불량률 추세 분석]"
            ws[f'A{current_row}'].font = Font(size=12, bold=True)
            current_row += 2

            # self.data → report 복사 (중복 방지)
            if hasattr(self, 'data') and 'EQP_TREND_GRAPHS' not in report:
                eqp_graphs = self.data.get('EQP_TREND_GRAPHS')
                if eqp_graphs:
                    report['EQP_TREND_GRAPHS'] = eqp_graphs
                    print(f"[INFO] EQP_TREND_GRAPHS report에 복사됨: {list(eqp_graphs.keys())}")

            # top3_rej_groups 가져오기
            top3_rej_groups = self.data.get('DATA_3210_wafering_300', {}).get('top3_rej_groups', [])
            # top3_rej_groups = ['PIT', 'EDGE', 'VISUAL']
            valid_groups = [g for g in top3_rej_groups if g in ['PIT', 'SCRATCH', 'EDGE', 'BROKEN', 'CHIP', 'VISUAL']]
            eqp_trend_graphs = report.get('EQP_TREND_GRAPHS', {})

            # SCRATCH, BROKEN, CHIP 순서 보장
            display_groups = ['PIT', 'SCRATCH', 'EDGE', 'BROKEN', 'CHIP', 'VISUAL']

            for rej_group in display_groups:
                if rej_group not in valid_groups:
                    current_row += 1
                    continue

                paths = eqp_trend_graphs.get(rej_group, [])
                if not paths:
                    ws.cell(row=current_row, column=1, value=f"{rej_group}: 그래프 없음").font = Font(size=10, color="FF0000")
                    current_row += 1  # 다음 행으로 이동
                    continue

                ws.cell(row=current_row, column=1, value=f"{rej_group} 그룹").font = Font(size=10, bold=True) # 현재 행에 제목 추가 (선택)
                graph_row = current_row + 1

                # 장비별 그래프 삽입 (최대 3개, A, G, M 열)
                for idx, path in enumerate(paths[:3]):
                    path_obj = Path(path)
                    if not path_obj.exists():
                        col_letter = ['A', 'F', 'K'][idx]
                        ws.cell(row=graph_row , column=1 + idx * 5, value=f"{rej_group}: 파일 없음").font = Font(size=9, color="FF0000")
                        continue

                    try:
                        img_path = str(path_obj).replace('\\', '/')
                        img = ExcelImage(img_path)
                        img.width = 400
                        img.height = 200
                        col_letter = ['A', 'F', 'K'][idx]
                        ws.add_image(img, f'{col_letter}{graph_row}')
                        print(f"[OK] 엑셀에 삽입됨: {img_path}")
                    except Exception as e:
                        print(f"[ERROR] 이미지 삽입 실패: {e}")
                        ws.cell(row=graph_row , column=1 + idx * 5, value=f"{rej_group} 실패").font = Font(size=9, color="FF0000")

                # 🔹 한 줄 끝나면 다음 행으로 이동 (그래프 높이 고려)
                current_row += 11

            # 열 너비
            for col, width in zip('ABCDEFGHIJ', [13] + [12]*9):
                ws.column_dimensions[col].width = width

            wb.save(str(excel_path))
            print(f"Excel 저장 성공: {excel_path}")
            return str(excel_path)

        except Exception as e:
            logger.error(f"Excel 생성 실패: {e}")
            raise
