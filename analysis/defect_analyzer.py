import pandas as pd
from config.mappings import REJ_GROUP_TO_MID_MAPPING


def add_mid_group(df, rej_group):
    """
    AFT_BAD_RSN_CD를 기준으로 MID_GROUP 생성
    매핑 정보는 mapping.py 에서 가져옴
    """
    mapping = REJ_GROUP_TO_MID_MAPPING.get(rej_group, {})
    df = df.copy()
    df['MID_GROUP'] = df['AFT_BAD_RSN_CD'].map(mapping)
    # 매핑되지 않은 것은 원래 값 유지
    df['MID_GROUP'] = df['MID_GROUP'].fillna(df['AFT_BAD_RSN_CD'])
    return df

def safe_convert_loss_qty(df, col_name='LOSS_QTY'):
    """
    LOSS_QTY 컬럼을 안전하게 숫자형으로 변환
    - 문자열, 공백, None, '-', '?' 등 처리
    """
    if col_name not in df.columns:
        raise KeyError(f"컬럼 없음: {col_name}")
    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
    return df


def analyze_flatness(df_lot, target_mids=None):
    """
    FLATNESS 불량 분석 (디버그 포함)
    """

    # 1. FLATNESS 데이터 필터링
    df = df_lot[df_lot['REJ_GROUP'] == 'FLATNESS'].copy()
    df = df[df['GRD_CD_NM_CS'] == 'Prime']
    if df.empty:
        return ["[FLATNESS 분석] 데이터 없음"]

    # 숫자형 변환
    df = safe_convert_loss_qty(df, 'LOSS_QTY')
    df = add_mid_group(df, 'FLATNESS')

    #  target_mids 필터링
    if target_mids is not None and len(target_mids) > 0:
        print(f"  - target_mids 적용: {target_mids}")
        before_filter = len(df)
        df = df[df['MID_GROUP'].isin(target_mids)]
        after_filter = len(df)
        print(f"  - 필터 전: {before_filter} → 필터 후: {after_filter}")
        # 순서 유지
        mid_list = [mid for mid in target_mids if mid in df['MID_GROUP'].unique()]
    else:
        mid_list = df.groupby('MID_GROUP')['LOSS_QTY'].sum().sort_values(ascending=False).index.tolist()

    if not mid_list:
        return ["[FLATNESS 분석] 대상 MID_GROUP 없음"]

    result = ["[FLATNESS 분석]"]
    for mid in mid_list:
        df_mid = df[df['MID_GROUP'] == mid].copy()
        grouped = (df_mid
                   .groupby(['AFT_BAD_RSN_CD', 'PRODUCT_TYPE', 'GRD_CD_NM_CS', 'GRD_CD_NM_PS'], dropna=False)['LOSS_QTY']
                   .sum()
                   .reset_index())
        grouped = grouped[grouped['LOSS_QTY'] >= 30].nlargest(3, 'LOSS_QTY')

        if grouped.empty:
            continue

        parts = []
        for _, row in grouped.iterrows():
            cs, ps = row['GRD_CD_NM_CS'], row['GRD_CD_NM_PS']
            grade = ('Prime' if (cs == 'Prime' and ps == 'Prime') else
                     'Normal' if (cs == 'Normal' and ps == 'Normal') else
                     'Premium' if (cs == 'Normal' and ps == 'Prime') else cs)
            cust = row['PRODUCT_TYPE'] if pd.notna(row['PRODUCT_TYPE']) else 'Unknown'
            qty = int(row['LOSS_QTY'])
            parts.append(f" {cust} {grade} {qty}매")

        if parts:
            line = f"- {mid} - " + ", ".join(parts)
            result.append(line)

    return result if len(result) > 1 else result + ["상위 30장 이상 없음"]


def analyze_warp(df_wafer, target_mids=None):
    """
    Nano Warp 불량 분석 통합 함수 (디버그 모드 포함)
    """
    df = df_wafer[df_wafer['REJ_GROUP'] == 'WARP&BOW'].copy()
    df = df[df['GRADE_CS'] == 'Prime']

    df = safe_convert_loss_qty(df, 'LOSS_QTY')

    # 날짜 포맷팅
    def format_date(val):
        s = str(val)
        return f"{int(s[4:6])}/{int(s[6:8])}" if len(s) >= 8 and s[:8].isdigit() else "Unknown"

    df['REG_DTTM_3200_FMT'] = df['REG_DTTM_300_WF_3200'].apply(format_date)

    result = ["[WARP&BOW 분석]"]

    # --- 대량불량 ---
    large = (df.groupby(['AFT_BAD_RSN_CD', 'BLK_ID', 'EQP_NM_300_WF_3200', 'REG_DTTM_3200_FMT'], dropna=False)['LOSS_QTY']
             .sum().reset_index())

    for _, row in large[large['LOSS_QTY'] >= 100].iterrows():
        result.append(f"{row['AFT_BAD_RSN_CD']} {row['BLK_ID']}의 {int(row['LOSS_QTY'])}매 ({row['EQP_NM_300_WF_3200']} {row['REG_DTTM_3200_FMT']})")

    # --- 소량 불량 ---
    df_minor = df[df['LOSS_QTY'] < 100].copy()
    if df_minor.empty:
        print("  - 소량불량 데이터 없음")
    else:
        # add_mid_group 함수 확인 필요
        if 'add_mid_group' not in globals() and 'add_mid_group' not in locals():
            print("❌ add_mid_group 함수가 정의되지 않음")
            return result + ["[오류] add_mid_group 없음"]

        try:
            df_minor = add_mid_group(df_minor, 'WARP&BOW')
        except Exception as e:
            print(f"❌ add_mid_group 실행 오류: {e}")
            return result + ["[오류] MID_GROUP 생성 실패"]

        # target_mids 가 있으면 그 값만 분석 (상위 3 개)
        if target_mids and len(target_mids) > 0:
            analysis_mids = target_mids[:3]  #  최대 3 개만
        else:
            # 전체 MID_GROUP 분석 (기존 로직)
            if 'MID_GROUP' in df_minor.columns:
                mid_summary = df_minor.groupby('MID_GROUP')['LOSS_QTY'].sum().sort_values(ascending=False)
                analysis_mids = mid_summary.index.tolist()[:3]  # 상위 3 개
            else:
                return result + ["소량분석: MID_GROUP 없음"]

        # 각 MID_GROUP별 상위 3개 BLK_ID 분석
        for mid in analysis_mids:
            df_m = df_minor[df_minor['MID_GROUP'] == mid].copy()

            grouped = (df_m.groupby(['AFT_BAD_RSN_CD', 'BLK_ID', 'PRODUCT_TYPE', 'GRADE_CS'], dropna=False)['LOSS_QTY']
                       .sum().reset_index().nlargest(3, 'LOSS_QTY'))

            if grouped.empty:
                continue

            parts = [f"{r['PRODUCT_TYPE']} {r['GRADE_CS']} {r['BLK_ID']} {int(r['LOSS_QTY'])}매" for _, r in grouped.iterrows()]
            result.append(f"- {mid} 열위 Lot - " + ", ".join(parts))

    return result if len(result) > 1 else result + ["대량/소량 없음"]


def analyze_growing(df_lot):
    """
    GROWING 불량 분석 함수:
    - AFT_BAD_RSN_CD 기준 상위 3개 코드 추출
    - 각 코드별 IGOT_ID 기준 LOSS_QTY 합계 상위 3개 추출
    입력: df_lot (DATA_LOT_3210_wafering_300 결과)
    """
    df_lot = df_lot[df_lot['REJ_GROUP'] == 'GROWING'].copy()
    df_lot = df_lot[df_lot['GRD_CD_NM_CS'] == 'Prime']

    if df_lot.empty:
        return ["[GROWING 분석] 데이터 없음"]
    df_lot = safe_convert_loss_qty(df_lot, 'LOSS_QTY')
    result_lines = ["[GROWING 분석]"]

    # AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 → 상위 3개 코드 추출
    code_summary = (
        df_lot.groupby('AFT_BAD_RSN_CD')['LOSS_QTY']
        .sum()
        .sort_values(ascending=False)
        .head(3)
    )

    if code_summary.empty:
        result_lines.append("상위 불량 코드 없음")
        return result_lines

    top3_codes = code_summary.index.tolist()

    # 각 코드별 결과를 모았다가 한 줄로 병합
    for code in top3_codes:
        df_code = df_lot[df_lot['AFT_BAD_RSN_CD'] == code].copy()
        grouped = df_code.groupby('IGOT_ID', dropna=False)['LOSS_QTY'].sum().reset_index()
        grouped = grouped.sort_values(by='LOSS_QTY', ascending=False).head(3)

        if grouped.empty:
            continue

        content_parts = []
        for _, row in grouped.iterrows():
            igot = row['IGOT_ID'] if pd.notna(row['IGOT_ID']) else 'Unknown'
            qty = int(row['LOSS_QTY'])
            content_parts.append(f"{igot} {qty}매 폐기")
        
        if content_parts:
            result_lines.append(f"- {code} - " + ", ".join(content_parts))

    if len(result_lines) == 1:
        result_lines.append("분석 결과 없음")

    return result_lines


def analyze_broken(df_lot):
    """
    BROKEN 불량 분석:
    1) AFT_BAD_RSN_CD 기준 상위 2개 추출
    2) 'LAP', 'EP', 'FP', 'DSP' 포함 여부 → 'main공정' / '이외공정' 분류
    3) 각 불량 코드별 EQP_ID 기준 LOSS_QTY 집계 및 몰림성 판단 (10매 이상 차이 시)
    입력: df_lot (DATA_LOT_3210_wafering_300 결과)
    """
    df_lot = df_lot[df_lot['REJ_GROUP'] == 'BROKEN'].copy()
    df_lot = df_lot[df_lot['GRD_CD_NM_CS'] == 'Prime']

    if df_lot.empty:
        return ["[BROKEN 분석] 데이터 없음"]

    df_lot = safe_convert_loss_qty(df_lot, 'LOSS_QTY')
    result = ["[BROKEN 분석]"]

    # Step 1: 공정구분 - 'LAP', 'EP', 'FP', 'DSP' 포함 여부
    main_keywords = ['LAP', 'EP', 'FP', 'DSP']
    df_lot['공정구분'] = df_lot['AFT_BAD_RSN_CD'].apply(
        lambda x: 'main공정' if any(k in str(x).upper() for k in main_keywords) else '이외공정'
    )

    # Step 2: AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 → 상위 2개 추출
    top2_codes = (
        df_lot.groupby('AFT_BAD_RSN_CD')['LOSS_QTY']
        .sum()
        .sort_values(ascending=False)
        .head(2)
        .index.tolist()
    )

    if not top2_codes:
        result.append("상위 불량 코드 없음")
        return result

    # Step 3: 각 상위 코드별 분석
    for code in top2_codes:
        df_code = df_lot[df_lot['AFT_BAD_RSN_CD'] == code].copy()
        proc_type = df_code['공정구분'].iloc[0] if not df_code.empty else 'Unknown'

        # EQP_ID 기준 집계
        grouped = (
            df_code.groupby('EQP_NM', dropna=False)['LOSS_QTY']
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        if grouped.empty:
            result.append(f" - {code} ({proc_type}): 장비 데이터 없음")
            continue

        sub_reason = f"{code} ({proc_type})"

        equip_list = []
        for _, row in grouped.iterrows():
            eqp = row['EQP_NM'] if pd.notna(row['EQP_NM']) else 'Unknown'
            loss = int(row['LOSS_QTY'])
            equip_list.append(f"{eqp}: {loss}매")
        # 판단 문구
        if len(grouped) == 1:
            eqp = grouped.iloc[0]['EQP_NM']
            loss = int(grouped.iloc[0]['LOSS_QTY'])
            eqp_str = eqp if pd.notna(eqp) else 'Unknown'
            final_line = f"{sub_reason} : {eqp_str}: {loss}매 → {eqp_str} 장비에 완전한 몰림성 (1개 장비)"
            result.append(final_line)
            continue

        # 상위 2개 장비 비교
        top1 = grouped.iloc[0]
        top2 = grouped.iloc[1]
        diff = int(top1['LOSS_QTY'] - top2['LOSS_QTY'])

        top1_eqp = top1['EQP_NM'] if pd.notna(top1['EQP_NM']) else 'Unknown'
        top2_eqp = top2['EQP_NM'] if pd.notna(top2['EQP_NM']) else 'Unknown'

        details_str = f"{top1_eqp}: {int(top1['LOSS_QTY'])}매, {top2_eqp}: {int(top2['LOSS_QTY'])}매"

        if diff >= 10:
            judgment = f" → {top1_eqp} 장비에 GR보증 존재 ({diff}매 차이)"
        else:
            judgment = f" → 몰림성 판단 어려움 ({diff}매 차이)"

        final_line = f"{sub_reason} : {details_str}{judgment}"
        result.append(final_line)

    if len(result) == 1:
        result.append("분석 결과 없음")

    return result


def analyze_nano(df_wafer):
    """
    NANO 불량 분석 통합 함수:
    1) 대량불량 (LOSS_QTY ≥ 100): 설비, 날짜 포함 문장 생성
    2) 소량 반복 불량 (LOSS_QTY < 100): AFT_BAD_RSN_CD 기준 상위 3개 중, 각각 상위 3개 BLK_ID 추출
    입력: df_wafer (DATA_WAF_3210_wafering_300 결과)
    """
    df_wafer = df_wafer[df_wafer['REJ_GROUP'] == 'NANO'].copy()
    df_wafer = df_wafer[df_wafer['GRADE_CS'] == 'Prime']

    if df_wafer.empty:
        return ["[NANO 분석] 데이터 없음"]
    df_wafer = safe_convert_loss_qty(df_wafer, 'LOSS_QTY')

    result_lines = ["[NANO 분석]"]

    # 날짜 포맷 변경: YYYYMMDDHHMMSS → M/D
    def format_date(val):
        try:
            val_str = str(val)
            if len(val_str) >= 8 and val_str[:8].isdigit():
                month = str(int(val_str[4:6]))
                day = str(int(val_str[6:8]))
                return f"{month}/{day}"
            else:
                return "Unknown"
        except:
            return "Unknown"

    df_wafer['REG_DTTM_3200_FMT'] = df_wafer['REG_DTTM_300_WF_3200'].apply(format_date)

    # 공통 그룹 컬럼 정의
    group_cols = ['AFT_BAD_RSN_CD', 'BLK_ID', 'EQP_NM_300_WF_3200', 'REG_DTTM_3200_FMT']
    grouped = df_wafer.groupby(group_cols, dropna=False)['LOSS_QTY'].sum().reset_index()

    # 대량불량: LOSS_QTY ≥ 100
    large_defects = grouped[grouped['LOSS_QTY'] >= 100].copy()
    for _, row in large_defects.iterrows():
        line = f"{row['AFT_BAD_RSN_CD']} {row['BLK_ID']}의 {int(row['LOSS_QTY'])}매 ({row['EQP_NM_300_WF_3200']} {row['REG_DTTM_3200_FMT']})"
        result_lines.append(line)

    # 소량 반복 불량: LOSS_QTY < 100
    minor_group_cols = ['AFT_BAD_RSN_CD', 'BLK_ID', 'PRODUCT_TYPE', 'GRADE_CS']  #PRODUCT_TYPE CUST_SITE_NM
    grouped_minor = df_wafer[df_wafer['LOSS_QTY'] < 100].groupby(minor_group_cols, dropna=False)['LOSS_QTY'].sum().reset_index()

    if grouped_minor.empty:
        if len(result_lines) == 1:
            result_lines.append("대량/소량 불량 없음")
        return result_lines

    # AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 → 상위 3개 코드 추출
    top3_codes = (
        grouped_minor.groupby('AFT_BAD_RSN_CD')['LOSS_QTY']
        .sum()
        .sort_values(ascending=False)
        .head(3)
        .index.tolist()
    )

    if not top3_codes:
        if len(result_lines) == 1:
            result_lines.append("소량 반복 불량 없음")
        return result_lines

    # 각 상위 코드별로 상위 3개 BLK_ID 추출
    for code in top3_codes:
        df_code = grouped_minor[grouped_minor['AFT_BAD_RSN_CD'] == code].copy()
        # BLK_ID 기준으로 PRODUCT_TYPE, GRADE_CS, LOSS_QTY 합계
        grouped_code = (df_code.groupby(['BLK_ID', 'PRODUCT_TYPE', 'GRADE_CS'], dropna=False)['LOSS_QTY']
                        .sum().reset_index())
        top3_blks = grouped_code.nlargest(3, 'LOSS_QTY')

        # 각 항목 포맷팅
        parts = []
        for _, row in top3_blks.iterrows():
            cust = row['PRODUCT_TYPE'] if pd.notna(row['PRODUCT_TYPE']) else 'Unknown'
            grade = row['GRADE_CS'] if pd.notna(row['GRADE_CS']) else 'Unknown'
            qty = int(row['LOSS_QTY'])
            parts.append(f"{cust} {grade} {row['BLK_ID']} {qty}매")

        if parts:
            line = f"- {code} 열위 Lot - " + ", ".join(parts)
            result_lines.append(line)

    if len(result_lines) == 1:
        result_lines.append("분석 결과 없음")

    return result_lines

def analyze_pit(df_wafer):
    """
    PIT 불량 분석 함수:
    1) AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 상위 3개 코드 추출
    2) 각 코드별 EQP_NM_3670 기준 LOSS_QTY 합계 상위 3개 장비 추출
    3) 날짜 형식: REG_DTTM_3670 → M/D 변환 (옵션 포함)
    입력: df_wafer (DATA_WAF_3210_wafering_300 결과)
    """
    df_wafer = df_wafer[df_wafer['REJ_GROUP'] == 'PIT'].copy()
    df_wafer = df_wafer[df_wafer['GRADE_CS'] == 'Prime']

    if df_wafer.empty:
        return ["[PIT 분석] 데이터 없음"]

    df_wafer = safe_convert_loss_qty(df_wafer, 'LOSS_QTY')

    result_lines = ["[PIT 분석]"]

    # 날짜 포맷 변경: YYYYMMDDHHMMSS → M/D
    def format_date(val):
        try:
            val_str = str(val)
            if len(val_str) >= 8 and val_str[:8].isdigit():
                month = str(int(val_str[4:6]))
                day = str(int(val_str[6:8]))
                return f"{month}/{day}"
            else:
                return "Unknown"
        except:
            return "Unknown"

    df_wafer['REG_DTTM_3670_FMT'] = df_wafer['REG_DTTM_300_WF_3670'].apply(format_date)

    # AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 → 상위 3개 코드 추출
    top3_codes = (
        df_wafer.groupby('AFT_BAD_RSN_CD')['LOSS_QTY']
        .sum()
        .sort_values(ascending=False)
        .head(3)
        .index.tolist()
    )

    if not top3_codes:
        result_lines.append("상위 불량 코드 없음")
        return result_lines

    # 각 상위 코드별 분석
    for code in top3_codes:
        df_code = df_wafer[df_wafer['AFT_BAD_RSN_CD'] == code].copy()

        # EQP_NM_3670 기준 집계
        grouped = (
            df_code.groupby(['EQP_NM_300_WF_3670', 'REG_DTTM_3670_FMT'], dropna=False)['LOSS_QTY']
            .sum()
            .reset_index()
            .sort_values('LOSS_QTY', ascending=False)
            .head(3)  # 상위 3개 장비 + 날짜 조합
        )

        if grouped.empty:
            continue

        # 결과 추가
        for _, row in grouped.iterrows():
            eqp = row['EQP_NM_300_WF_3670'] if pd.notna(row['EQP_NM_300_WF_3670']) else 'Unknown'
            qty = int(row['LOSS_QTY'])
            date = row['REG_DTTM_3670_FMT']
            line = f"{code} 열위 장비 - {eqp} {qty}매 ({date})"
            result_lines.append(line)

    if len(result_lines) == 1:
        result_lines.append("분석 결과 없음")

    return result_lines


def analyze_scratch(df_wafer):
    """
    SCRATCH 불량 분석 함수:
    1) AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 상위 3개 코드 추출
    2) 각 코드별로 EQP_NM_6100 / EQP_NM_3670 공정에서의 최다 발생 장비 및 날짜 분석
    3) 동일 수량일 경우 장비 병기 출력 (예: A장비 M/D / B장비 M/D)
    입력: df_wafer (DATA_WAF_3210_wafering_300 결과)
    """
    df_wafer = df_wafer[df_wafer['REJ_GROUP'] == 'SCRATCH'].copy()
    df_wafer = df_wafer[df_wafer['GRADE_CS'] == 'Prime']

    if df_wafer.empty:
        return ["[SCRATCH 분석] 데이터 없음"]

    df_wafer = safe_convert_loss_qty(df_wafer, 'LOSS_QTY')
    df_wafer = add_mid_group(df_wafer, 'SCRATCH')  # MID_GROUP 생성

    result = ["[SCRATCH 분석]"]
    mid_results = {}

    # 분석 대상 공정 정의
    process_list = [
        {'eqp_col': 'EQP_NM_300_WF_6100', 'time_col': 'REG_DTTM_300_WF_6100'},
        {'eqp_col': 'EQP_NM_300_WF_3670', 'time_col': 'REG_DTTM_300_WF_3670'}
    ]

    for mid_name in ['Front Side', 'Back Side']:
        df_mid = df_wafer[df_wafer['MID_GROUP'] == mid_name].copy()
        if df_mid.empty:
            continue

        total_qty = int(df_mid['LOSS_QTY'].sum())
        details = []

        for proc in process_list:
            eqp_col = proc['eqp_col']
            time_col = proc['time_col']

            # 컬럼명 끝 4글자 → 공정 코드
            proc_code = eqp_col[-4:]

            # 장비 데이터 존재 여부 확인
            df_proc = df_mid[df_mid[eqp_col].notna()].copy()
            if df_proc.empty:
                continue

            # 장비별 합계
            grouped = df_proc.groupby(eqp_col)['LOSS_QTY'].sum().reset_index()
            for _, row in grouped.iterrows():
                eqp = row[eqp_col]
                qty = int(row['LOSS_QTY'])
                eqp_str = eqp if pd.notna(eqp) else 'Unknown'
                details.append(f"{proc_code} - {eqp_str} {qty}매")

        # 결과 포맷팅
        if len(details) > 5:
            shown = ", ".join(details[:5])
            mid_results[mid_name] = f"{total_qty}매 ({shown} 등)"
        elif details:
            mid_results[mid_name] = f"{total_qty}매 ({', '.join(details)})"
        else:
            mid_results[mid_name] = f"{total_qty}매 (장비 정보 없음)"

    # 최종 문장 생성
    parts = [f"{k} {v}" for k, v in mid_results.items()]
    if parts:
        result.append(", ".join(parts))
    else:
        result.append("분석 결과 없음")

    return result




def analyze_edge(df_wafer):
    """
    EDGE 불량 분석 함수:
    1) EG1차(3335), EG2차(3696), EBIS측정(7000) 공정별 장비 기준 LOSS_QTY 집계
    2) 각 공정별 상위 2개 장비 비교 → 10매 이상 차이 시 "몰림 발생" 판단
    3) LOSS_QTY 타입 변환, NaN 처리, 인덱스 안전 접근 보장
    입력: df_wafer (DATA_WAF_3210_wafering_300 결과)
    """
    # REJ_GROUP 필터링
    df_wafer = df_wafer[df_wafer['REJ_GROUP'] == 'EDGE'].copy()
    df_wafer = df_wafer[df_wafer['GRADE_CS'] == 'Prime']

    if df_wafer.empty:
        return ["[EDGE 분석] 데이터 없음"]

    df_wafer = safe_convert_loss_qty(df_wafer, 'LOSS_QTY')

    result = ["[EDGE 분석]"]

    # 공정 정보 정의
    process_info = [
        {'eqp_col': 'EQP_NM_300_WF_3335', 'time_col': 'REG_DTTM_300_WF_3335', 'label': 'EG1차'},
        {'eqp_col': 'EQP_NM_300_WF_3696', 'time_col': 'REG_DTTM_300_WF_3696', 'label': 'EG2차'},
        {'eqp_col': 'EQP_NM_300_WF_7000', 'time_col': 'REG_DTTM_300_WF_7000', 'label': 'EBIS측정'},
    ]

    for proc in process_info:
        eqp_col = proc['eqp_col']
        time_col = proc['time_col']
        label = proc['label']

        # 장비 컬럼 존재 여부 체크
        if eqp_col not in df_wafer.columns:
            result.append(f"{label} : 장비 컬럼 없음")
            continue

        # 장비명이 NaN이 아닌 데이터만 필터링
        df_proc = df_wafer[df_wafer[eqp_col].notna()].copy()

        if df_proc.empty:
            result.append(f"{label} : 데이터 없음")
            continue

        # 장비별 LOSS_QTY 합계
        grouped = (
            df_proc.groupby(eqp_col, dropna=False)['LOSS_QTY']
            .sum()
            .reset_index()
            .sort_values('LOSS_QTY', ascending=False)
            .reset_index(drop=True)
        )

        # 장비 1개만 존재할 경우
        if len(grouped) == 1:
            eqp = grouped.iloc[0][eqp_col]
            qty = int(grouped.iloc[0]['LOSS_QTY'])
            eqp_str = eqp if pd.notna(eqp) else "Unknown"
            result.append(f"{label} : 단일 장비 - {eqp_str} {qty}매")
            continue

        # 장비 2개 이상: 상위 2개 비교
        top1 = grouped.iloc[0]
        top2 = grouped.iloc[1]
        diff = int(top1['LOSS_QTY'] - top2['LOSS_QTY'])

        top1_eqp = top1[eqp_col] if pd.notna(top1[eqp_col]) else "Unknown"
        top2_eqp = top2[eqp_col] if pd.notna(top2[eqp_col]) else "Unknown"

        if diff >= 10:  # >= 10: 임계값 포함
            result.append(f"{label} : {top1_eqp} 장비 몰림 발생 - {int(top1['LOSS_QTY'])}매 (2위 대비 +{diff}매)")
        else:
            result.append(f"{label} : 몰림 없음 - {top1_eqp} {int(top1['LOSS_QTY'])}매 / {top2_eqp} {int(top2['LOSS_QTY'])}매")

    if len(result) == 1:
        result.append("분석 결과 없음")

    return result

def analyze_chip(df_wafer):
    """
    CHIP 불량 분석 함수:
    1) AFT_BAD_RSN_CD 기준 LOSS_QTY 합계 상위 1개 추출
    2) 분석 대상 불량 유형인지 확인 (defect_mapping 기준)
    3) 해당 유형별 장비 기준 상위 1, 2위 출력 (몰림성 판단 제외)
    입력: df_wafer (DATA_WAF_3210_wafering_300 결과)
    """
    df_wafer = df_wafer[df_wafer['REJ_GROUP'] == 'CHIP'].copy()
    df_wafer = df_wafer[df_wafer['GRADE_CS'] == 'Prime']

    if df_wafer.empty:
        return ["[CHIP 분석] 데이터 없음"]

    df_wafer = safe_convert_loss_qty(df_wafer, 'LOSS_QTY')

    result = ["[CHIP 분석]"]

    # STEP 1: AFT_BAD_RSN_CD별 LOSS_QTY 합계 → 상위 1개
    defect_sums = (
        df_wafer.groupby('AFT_BAD_RSN_CD', dropna=False)['LOSS_QTY']
        .sum()
        .reset_index()
        .sort_values('LOSS_QTY', ascending=False)
        .reset_index(drop=True)
    )

    if defect_sums.empty:
        result.append("분석 대상 불량 없음")
        return result

    top_defect = defect_sums.iloc[0]['AFT_BAD_RSN_CD']
    result.append(f"최다 CHIP 불량 유형: {top_defect}")

    # STEP 2: 분석 대상 불량 유형 매핑
    defect_mapping = {
        'EDGE_CHIP': ['EQP_NM_300_WF_3335', 'REG_DTTM_300_WF_3335', 'EQP_NM_300_WF_3696', 'REG_DTTM_300_WF_3696'],
        'CHIP-LAP': ['EQP_NM_300_WF_3670', 'REG_DTTM_300_WF_3670'],
        'CHIP-EG1AF': ['EQP_NM_300_WF_3335', 'REG_DTTM_300_WF_3335', 'EQP_NM_300_WF_3696', 'REG_DTTM_300_WF_3696'],
        'CHIP-EG1BF': ['EQP_NM_300_WF_3300', 'REG_DTTM_300_WF_3300'],
    }

    if top_defect not in defect_mapping:
        result.append(f"분석 제외: '{top_defect}'는 분석 대상 불량 유형이 아님")
        return result

    df_sub = df_wafer[df_wafer['AFT_BAD_RSN_CD'] == top_defect].copy()
    eqp_cols = defect_mapping[top_defect]

    # CHIP-EG1AF: 주 장비 없을 시 fallback
    if top_defect == 'CHIP-EG1AF':
        primary_eqp = 'EQP_NM_300_WF_3335'
        if primary_eqp not in df_sub.columns or df_sub[primary_eqp].isna().all():
            eqp_cols = ['EQP_NM_300_WF_3300', 'REG_DTTM_300_WF_3300']
            result.append("→ 주 장비 정보 없어 EQP_NM_3300으로 대체")

    result.append(f"세부불량: {top_defect}")

    # STEP 3: 각 장비 컬럼별 분석 (2개씩 묶음)
    for i in range(0, len(eqp_cols), 2):
        eqp_col = eqp_cols[i]
        time_col = eqp_cols[i + 1] if i + 1 < len(eqp_cols) else None

        # 장비 컬럼 존재 여부 체크
        if eqp_col not in df_sub.columns:
            result.append(f"{eqp_col}: 컬럼 없음")
            continue

        # 장비명이 NaN이 아닌 데이터만
        df_eqp = df_sub[df_sub[eqp_col].notna()].copy()
        if df_eqp.empty:
            result.append(f"{eqp_col}: 데이터 없음")
            continue

        # 장비별 LOSS_QTY 합계
        grouped = (
            df_eqp.groupby(eqp_col, dropna=False)['LOSS_QTY']
            .sum()
            .reset_index()
            .sort_values('LOSS_QTY', ascending=False)
            .reset_index(drop=True)
        )

        result.append(f"{eqp_col} 장비별 불량 상위")

        # 1위
        top1 = grouped.iloc[0]
        top1_eqp = top1[eqp_col] if pd.notna(top1[eqp_col]) else "Unknown"
        result.append(f"1위: {top1_eqp} ({int(top1['LOSS_QTY'])}매)")

        # 2위
        if len(grouped) >= 2:
            top2 = grouped.iloc[1]
            top2_eqp = top2[eqp_col] if pd.notna(top2[eqp_col]) else "Unknown"
            result.append(f"2위: {top2_eqp} ({int(top2['LOSS_QTY'])}매)")

    return result


def analyze_others(df_lot, rej_group):
    """
    기타 불량 그룹 공통 분석 함수
    - REJ_GROUP에 따라 AFT_BAD_RSN_CD별 LOSS_QTY 합계 상위 1개 출력
    입력:
        df_lot: 원본 데이터
        rej_group: REJ_GROUP 값 (예: 'HUMAN_ERR', 'VISUAL', ...)
        group_label_kr: 한글 그룹명 (예: '사람오류', '시각불량', ...)
    """
    df_group = df_lot[df_lot['REJ_GROUP'] == rej_group].copy()
    df_group = df_group[df_group['GRD_CD_NM_CS'] == 'Prime']

    if df_group.empty:
        return [f"[{rej_group} 분석] 데이터 없음"]

    df_group = safe_convert_loss_qty(df_group, 'LOSS_QTY')

    # AFT_BAD_RSN_CD별 합계 → 상위 1개
    defect_summary = (
        df_group.groupby('AFT_BAD_RSN_CD', dropna=False)['LOSS_QTY']
        .sum()
        .reset_index()
        .sort_values('LOSS_QTY', ascending=False)
        .reset_index(drop=True)
    )

    if defect_summary.empty:
        return [f"[{rej_group} 분석] 분석 대상 없음"]

    top_row = defect_summary.iloc[0]
    code = top_row['AFT_BAD_RSN_CD']
    qty = int(top_row['LOSS_QTY'])
    code_str = code if pd.notna(code) else "Unknown"

    return [f"[{rej_group} 분석]", f"{code_str} {qty}장 등 처리"]

def analyze_HUMAN_ERR(df_lot):
    return analyze_others(df_lot, 'HUMAN_ERR')

def analyze_VISUAL(df_lot):
    return analyze_others(df_lot, 'VISUAL')

def analyze_NOSALE(df_lot):
    return analyze_others(df_lot, 'NOSALE')

def analyze_OTHER(df_lot):
    return analyze_others(df_lot, 'OTHER')

def analyze_GR(df_lot):  # 이름 수정: GR → GR_보증
    return analyze_others(df_lot, 'GR_보증')


# 1.Particle 상세분석
# 1) 기본 비율 분석(FS, RESC, HG 비율)
def analyze_particle_ratios(df_lot, ref_value=1.8, threshold=0.5):
    """
    FS/RESC/HG 불량률 및 반영율을 'IN_QTY 전체 합계'를 기준으로 계산하며,
    RESC 영향 여부도 함께 판단하여 결과 문자열로 반환.
    """
    df_lot = df_lot[df_lot['GRD_CD_NM_CS'] == 'Prime']
    denominator_data = df_lot[df_lot['REJ_GROUP'] == '분모']
    denominator_data = safe_convert_loss_qty(denominator_data, 'IN_QTY')
    total_in_qty = denominator_data['IN_QTY'].sum()

    df = df_lot[df_lot['REJ_GROUP'] == 'PARTICLE'].copy()
    df = df[df['GRD_CD_NM_CS'] == 'Prime']
    df = safe_convert_loss_qty(df, 'LOSS_QTY')
    result = []
    base_dt = df['BASE_DT'].iloc[0]

    for cret in ['FS', 'RESC', 'HG']:
        cret_total_loss = 0 #cret별 total loss_qty 저장용
        for grade in ['Prime', 'Normal']:
            # 🔸 분자: 해당 조건의 LOSS_QTY
            loss_qty = df[
                (df['CRET_CD'] == cret) &
                (df['GRD_CD_NM_CS'] == grade) &
                (df['REJ_GROUP'] == 'PARTICLE')
            ]['LOSS_QTY'].sum()

            cret_total_loss += loss_qty

            rate = (loss_qty / total_in_qty * 100) if total_in_qty != 0 else 0.00
            rate_rounded = round(rate, 2)  # 음수도 유지

            result.append({
                'BASE_DT': base_dt,
                'CRET_CD': cret,
                'GRADE_CS': grade,
                'LOSS_QTY': loss_qty,
                'TOTAL_IN_QTY': total_in_qty,
                'RATE(%)': rate_rounded
            })
    
        # cret별 total행 추가
        rate_total = (cret_total_loss / total_in_qty * 100)
        rate_total_rounded = round(rate_total, 2)

        result.append({
            'BASE_DT': base_dt,
            'CRET_CD': cret,
            'GRADE_CS': 'Total',
            'LOSS_QTY': cret_total_loss,
            'TOTAL_IN_QTY': total_in_qty,
            'RATE(%)': rate_total_rounded
        })

    df_result = pd.DataFrame(result)

    # 🔹 RESC Total 영향 판단
    resc_total_row = df_result[
        (df_result['CRET_CD'] == 'RESC') & 
        (df_result['GRADE_CS'] == 'Total')
    ]

    if resc_total_row.empty or resc_total_row['RATE(%)'].values[0] == 0.00:
        rc_judgement = "RESC 반영율 판단 불가"
    else:
        rate = resc_total_row['RATE(%)'].values[0]
        rate_floate = float(rate)
        abs_rate = abs(rate)  # 🔹 절댓값 기준 판단
        lower_bound = ref_value - threshold
        upper_bound = ref_value + threshold

        if abs_rate < lower_bound:
            rc_judgement = f"R/C 양품 감소 → 불량 미달 가능성 (기준 대비 -{round(ref_value - rate_floate, 2)}%)"
        elif abs_rate  > upper_bound:
            rc_judgement = f"R/C 양품 증가 → 불량 과보상 가능성 (기준 대비 +{round(rate_floate - ref_value, 2)}%)"
        else:
            rc_judgement = "R/C 영향 변동 아님 → 다른 요인 탐색 필요"

    return df_result, rc_judgement

# 2) particle 상세분석
def create_particle_table(df_wafer):
    """
    Particle wafer 단위 데이터에서 주요 컬럼 기준으로 LOSS_QTY 합계를 피벗 테이블 형태로 변환
    """
    print(f"df_wafer 컬럼 목록: {list(df_wafer.columns)}")
    index_cols =['BASE_DT','WAF_ID','WAF_SEQ','DIV_CD','FAC_ID','CRET_CD','PROD_ID','IGOT_ID','BLK_ID','SUBLOT_ID','BEF_BAD_RSN_CD','AFT_BAD_RSN_CD','REJ_GROUP','PRODUCT_TYPE','GRADE_CS','GRADE_PS'] #PRODUCT_TYPE CUST_SITE_NM

    #pivot은 잘 안되서, groupby로 해결
    df_wafer = df_wafer[df_wafer['REJ_GROUP'] == 'PARTICLE'].copy()
    df_wafer = df_wafer[df_wafer['GRADE_CS'] == 'Prime']
    df_grouped = df_wafer.groupby(index_cols, dropna=False)['LOSS_QTY'].sum().reset_index()

    #loss_qty별 구분
    df_grouped_plus = df_grouped[df_grouped['LOSS_QTY'] > 0].copy() #df_grouped[df_grouped['LOSS_QTY'] == 1].copy() 로 하면 데이터 일부 사라짐. 원인은 모르겠음.
    df_grouped_minus = df_grouped[df_grouped['LOSS_QTY'] < 0].copy()

    df_grouped_plus['matching'] = (df_grouped_plus['WAF_SEQ'].astype(str) + df_grouped_plus['IGOT_ID'].astype(str) + df_grouped_plus['AFT_BAD_RSN_CD'].astype(str))
    df_grouped_minus['matching'] = (df_grouped_minus['WAF_SEQ'].astype(str) + df_grouped_minus['IGOT_ID'].astype(str) + df_grouped_minus['BEF_BAD_RSN_CD'].astype(str))

    df_grouped_minus['cat'] = 'Good' #loss_qty = -1인경우, cat(구분) 컬럼에 Good으로 입력
    df_grouped_plus['cat'] = 'NAN' #우선 cat(구분)컬럼을 NAN으로 초기화

    #매칭된 값이 존재하는 경우만 'Good'으로 설정
    df_grouped_plus.loc[df_grouped_plus['matching'].isin(df_grouped_minus['matching']), 'cat'] = 'Good'

    # [수정] \\~ 제거 → 문자열 조건 직접 비교 (문제 없이 동작)
    df_nan_particle = df_grouped_plus[df_grouped_plus['cat'] == 'NAN'].copy()  # 필터링

    top3_codes = (df_nan_particle.groupby('AFT_BAD_RSN_CD')['LOSS_QTY'].sum().sort_values(ascending=False).head(3).index.tolist()) # AFT_BAD_RSN_CD별 LOSS_QTY 합계 상위 3개 코드 추출

    #결과 list 작성
    result_descriptions = []

    for code in top3_codes:
        df_code = df_nan_particle[df_nan_particle['AFT_BAD_RSN_CD'] == code]

        #그룹화하여 LOSS_QTY 합계 계산
        grouped = (df_code.groupby(['PRODUCT_TYPE', 'GRADE_CS', 'GRADE_PS'])['LOSS_QTY'].sum().reset_index().sort_values('LOSS_QTY', ascending=False)) #PRODUCT_TYPE CUST_SITE_NM

        #가장 많은 LOSS_QTY를 기록한 항목 선택
        if not grouped.empty:
            top_row = grouped.iloc[0]
            cust = top_row['PRODUCT_TYPE']  #PRODUCT_TYPE CUST_SITE_NM
            grade_cs = top_row['GRADE_CS']
            grade_ps = top_row['GRADE_PS']
            qty = int(top_row['LOSS_QTY']) #수량은 정수로 표현

            #grade 재분류
            if grade_cs == 'Prime' and grade_ps == 'Prime':
                final_grade = 'Prime'
            elif grade_cs == 'Normal' and grade_ps == 'Normal':
                final_grade = 'Normal'
            elif grade_cs == 'Normal' and grade_ps == 'Prime':
                final_grade = 'Premium'
            else:
                final_grade = grade_cs  

            result_descriptions.append(f"{code} {cust} {final_grade} {qty} 매")

    return result_descriptions

# particle완성
def analyze_particle(df_lot, df_wafer):
    result = []
    # 1) RC 판단 비율 분석
    df_result, rc_judgement = analyze_particle_ratios(df_lot)

    # RESC Total 값 추출 (safe_get 없이 직접 처리)
    resc_total_row = df_result[
        (df_result['CRET_CD'] == 'RESC') &
        (df_result['GRADE_CS'] == 'Total')
    ]
    if not resc_total_row.empty:
        total_rate = resc_total_row['RATE(%)'].values[0]
    else:
        total_rate = 0.00

    # Prime, Normal 개별 추출
    prime_row = df_result[
        (df_result['CRET_CD'] == 'RESC') &
        (df_result['GRADE_CS'] == 'Prime')
    ]
    normal_row = df_result[
        (df_result['CRET_CD'] == 'RESC') &
        (df_result['GRADE_CS'] == 'Normal')
    ]

    prime_rate = prime_row['RATE(%)'].values[0] if not prime_row.empty else 0.00
    normal_rate = normal_row['RATE(%)'].values[0] if not normal_row.empty else 0.00

    # 출력 (FS, HG는 분석만, 출력은 RESC만)
    result.append("[PARTICLE 분석]")
    result.append(
        f"- RESC : Prime반영율:{prime_rate:.2f}%, "
        f"Normal반영율:{normal_rate:.2f}%, "
        f"P+N반영율:{total_rate:.2f}%, "
        f"판정결과 : {rc_judgement}"
    )

    # 2) RC 영향 없을 때만 wafer 상세분석
    if rc_judgement == "R/C 영향 변동 아님 → 다른 요인 탐색 필요":
        desc = create_particle_table(df_wafer)
        if desc:
            code_part = "코드별: " + ", ".join([d.split(' ', 1)[0] + " " + d.split(' ', 1)[1].rsplit(' ', 1)[0] + " 매" for d in desc])
            cust_part = "제품별: " + " / ".join([d.replace(' 매', '') for d in desc])
            result.append(f"- {code_part}")
            result.append(f"- {cust_part}")

    return result


def analyze_sample(df_lot):
    """
    SAMPLE 관련 AFT_BAD_RSN_CD 불량 유형 상세 분석
    - MOM_SAMPLE, LOT_SMPL, SMPL, 기타 등 구분하여 로직 처리
    - 상위 2개 코드만 분석 (LOSS_QTY 기준)
    """
    df_lot = df_lot[df_lot['REJ_GROUP'] == 'SAMPLE'].copy()
    result = ["[SAMPLE 분석]"]

    if df_lot.empty:
        result.append("SAMPLE 불량 데이터 없음")
        return result

    # 숫자형 변환
    df_lot = safe_convert_loss_qty(df_lot, 'LOSS_QTY')
    df_lot = add_mid_group(df_lot, 'SAMPLE')

    # ──────────────────────────────────────────────────
    # 1) Eng'r Sample (ENGSFT, ENGSCT, ENGSIS)
    # ──────────────────────────────────────────────────
    eng_df = df_lot[df_lot['MID_GROUP'] == 'Engr Sample']
    if not eng_df.empty:
        code_summary = (eng_df.groupby('AFT_BAD_RSN_CD')['LOSS_QTY']
                        .sum().reset_index().sort_values('LOSS_QTY', ascending=False))
        parts = [f"{row['AFT_BAD_RSN_CD']} {int(row['LOSS_QTY'])}매" for _, row in code_summary.iterrows()]
        result.append(f"- Eng'r Sample 발췌 증가 ({', '.join(parts)})")

    # ──────────────────────────────────────────────────
    # 2) Lot Sample (LOT_SMPL, LOT-SAMPLE)
    # ──────────────────────────────────────────────────
    lot_df = df_lot[df_lot['MID_GROUP'] == 'Lot Sample']
    if not lot_df.empty:
        prod_summary = (lot_df.groupby('PRODUCT_TYPE')['LOSS_QTY']
                        .sum().reset_index().nlargest(2, 'LOSS_QTY'))
        if not prod_summary.empty:
            parts = [f"{row['PRODUCT_TYPE']} {int(row['LOSS_QTY'])}매" for _, row in prod_summary.iterrows()]
            result.append(f"- Lot Sample ({', '.join(parts)})")

    # ──────────────────────────────────────────────────
    # 3) Monitoring Sample (MON_SAMPLE)
    # ──────────────────────────────────────────────────
    mon_df = df_lot[df_lot['MID_GROUP'] == 'Monitoring Sample']
    if not mon_df.empty:
        oper_summary = (mon_df.groupby('OPER_ID')['LOSS_QTY']
                        .sum().reset_index().nlargest(2, 'LOSS_QTY'))
        oper_parts = []
        for _, row in oper_summary.iterrows():
            oper_id = row['OPER_ID']
            qty = int(row['LOSS_QTY'])
            df_oper = mon_df[mon_df['OPER_ID'] == oper_id]
            top_prods = (df_oper.groupby('PRODUCT_TYPE')['LOSS_QTY']
                         .sum().reset_index().nlargest(2, 'LOSS_QTY'))
            prod_str = " - " + ", ".join([f"{r['PRODUCT_TYPE']} {int(r['LOSS_QTY'])}매" for _, r in top_prods.iterrows()])
            oper_parts.append(f"{oper_id} {qty}매{prod_str}")
        result.append(f"- Monitoring Sample ({', '.join(oper_parts)})")

    # ──────────────────────────────────────────────────
    # 4) Growing Engr Sample (SMPL)
    # ──────────────────────────────────────────────────
    smpl_df = df_lot[df_lot['MID_GROUP'] == 'Growing Engr Sample']
    if not smpl_df.empty:
        igot_summary = (smpl_df.groupby('IGOT_ID')['LOSS_QTY']
                        .sum().reset_index().nlargest(2, 'LOSS_QTY'))
        if not igot_summary.empty:
            parts = [f"{row['IGOT_ID']} {int(row['LOSS_QTY'])}매" for _, row in igot_summary.iterrows()]
            result.append(f"- Growing Engr Sample ({', '.join(parts)})")

    # ──────────────────────────────────────────────────
    # 5) 기타 SAMPLE 코드 (MID_GROUP에 포함되지 않은 AFT_BAD_RSN_CD)
    # ──────────────────────────────────────────────────
    # set difference로 known AFT_BAD_RSN_CD 제거
    known_list = ['ENGSFT', 'ENGSCT', 'ENGSIS', 'LOT_SMPL', 'LOT-SAMPLE', 'MON_SAMPLE', 'SMPL']

    # 현재 df_lot의 AFT_BAD_RSN_CD 중, known_list에 없는 것만 추출
    current_codes = df_lot['AFT_BAD_RSN_CD'].dropna().unique()
    other_codes = [code for code in current_codes if code not in known_list]

    if other_codes:
        other_df = df_lot[df_lot['AFT_BAD_RSN_CD'].isin(other_codes)].copy()
        
        other_summary = other_df.groupby('AFT_BAD_RSN_CD')['LOSS_QTY'].sum().reset_index()
        parts = [f"{row['AFT_BAD_RSN_CD']} {int(row['LOSS_QTY'])}매" for _, row in other_summary.iterrows()]
        result.append(f"- 기타 Sample ({', '.join(parts)})")

    return result if len(result) > 1 else result + ["분석 없음"]



# def analyze_sample(df_lot):
#     """
#     SAMPLE 관련 AFT_BAD_RSN_CD 불량 유형 상세 분석
#     - MOM_SAMPLE, LOT_SMPL, SMPL, 기타 등 구분하여 로직 처리
#     - 상위 2개 코드만 분석 (LOSS_QTY 기준)
#     """
#     df_lot = df_lot[df_lot['REJ_GROUP'] == 'SAMPLE'].copy()
#     result = ["[SAMPLE 분석]"]

#     if df_lot.empty:
#         result.append("SAMPLE 불량 데이터 없음")
#         return result

#     # AFT_BAD_RSN_CD별 LOSS_QTY 합계 → 상위 2개
#     defect_sums = (
#         df_lot.groupby('AFT_BAD_RSN_CD')['LOSS_QTY']
#         .sum()
#         .reset_index()
#         .sort_values('LOSS_QTY', ascending=False)
#         .head(2)
#     )

#     if defect_sums.empty:
#         result.append("분석할 불량 데이터 없음")
#         return result

#     for _, row in defect_sums.iterrows():
#         code = row['AFT_BAD_RSN_CD']
#         total_qty = int(row['LOSS_QTY'])
#         df_code = df_lot[df_lot['AFT_BAD_RSN_CD'] == code].copy()

#         # 1) MON_SAMPLE
#         if code == 'MON_SAMPLE':
#             grouped = (
#                 df_code.groupby('OPER_ID')['LOSS_QTY']
#                 .sum()
#                 .reset_index()
#                 .sort_values('LOSS_QTY', ascending=False)
#             )
#             over20 = grouped[grouped['LOSS_QTY'] >= 20]

#             if not over20.empty:
#                 top_oper = over20.iloc[0]
#                 result.append(f"{code} 총 {total_qty}장 ({top_oper['OPER_ID']} 공정 {int(top_oper['LOSS_QTY'])}장 등 발췌)")
#             else:
#                 result.append(f"{code} {total_qty}장 (20장 이상 공정 없음)")

#         # 2) ENGSFT, ENGSCT, ENGSIS → skip
#         elif code in ['ENGSFT', 'ENGSCT', 'ENGSIS']:
#             continue  # 보고서에서 제외 (의도된 스킵)

#         # 3) LOT_SMPL
#         elif code == 'LOT_SMPL':
#             grouped = (
#                 df_code.groupby('PRODUCT_TYPE')['LOSS_QTY'] # PRODUCT_TYPE CUST_SITE_NM
#                 .sum()
#                 .reset_index()
#                 .sort_values('LOSS_QTY', ascending=False)
#             )
#             if not grouped.empty:
#                 top_site = grouped.iloc[0]
#                 result.append(f"{code} 총 {total_qty}장 ({top_site['PRODUCT_TYPE']} {int(top_site['LOSS_QTY'])}장 등 발췌)")
#             else:
#                 result.append(f"{code} {total_qty}장")

#         # 4) SMPL
#         elif code == 'SMPL':
#             grouped = (
#                 df_code.groupby('IGOT_ID')['LOSS_QTY']
#                 .sum()
#                 .reset_index()
#                 .sort_values('LOSS_QTY', ascending=False)
#             )
#             over20 = grouped[grouped['LOSS_QTY'] >= 20]
#             if not over20.empty:
#                 top_igot = over20.iloc[0]
#                 result.append(f"SMPL (Growing Eng’r Sample) 총 {total_qty}장, {top_igot['IGOT_ID']} {int(top_igot['LOSS_QTY'])}장 등 발췌")
#             else:
#                 result.append(f"SMPL (Growing Eng’r Sample) 총 {total_qty}장 (20장 이상 IGOT_ID 없음)")

#         # 5) 그 외 코드 (20장 이상인 경우만 출력)
#         else:
#             if total_qty >= 20:
#                 result.append(f"{code} 총 {total_qty}장")

#     return result
