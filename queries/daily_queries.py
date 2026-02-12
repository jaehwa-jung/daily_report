from datetime import datetime, timedelta

_RUNTIME_CONFIG = {}

def set_runtime_config(config):
    """런타임 시점에 config 설정"""
    _RUNTIME_CONFIG.update(config)

def get_runtime_config():
    """런타임 config 조회"""
    return _RUNTIME_CONFIG.copy()


def DATA_3010_wafering_300(target_date, config):
    """
    3010 보고서

    """
    # 날짜 포맷 변환
    target_date_nm = datetime.strptime(target_date, '%Y%m%d').strftime('%y-%m-%d')
    target_ym = target_date[:6]  # '202601'
    return f"""
select *
from Oracle.pmdw_mgr.DW_DMS_WFYLD a
where 1=1
and a.base_dt = '{target_date}'
and a.waf_size = '{config['waf_size']}'
and a.prod_type = '{config['oper_div_l']}'
"""


def DATA_WAF_3210_wafering_300(target_date, config):
    """
    3210_DATA_WAF_wafering_300 조회
    
    Args:
        target_date: YYYYMMDD 형식 날짜
        config: QUERY_CONFIG 딕셔너리
    """
    fac_ids_str = "','".join(config['fac_ids'])
    grade_filter = config['grade_filter']

    # 등급 조건 설정: GRD_CD_NM과 GRD_CD_NM_PS 모두 적용
    if grade_filter == 'PN':
        grade_condition = ""
    else:
        grade_condition = f"AND mp.GRD_CD_NM = '{grade_filter}' AND mp.GRD_CD_NM_PS = '{grade_filter}'"

    # Trino에서 30일 전 날짜 계산
    target_date_obj = datetime.strptime(target_date, '%Y%m%d')
    date_range_start = (target_date_obj - timedelta(days=30)).strftime('%Y%m%d')

    return f"""
WITH 
-- 1. 불량 데이터 추출
step1_base AS (
    SELECT 
        A.WAF_ID, A.WAF_SEQ, A.WAF_SIZE, A.BASE_DT, A.DIV_CD, A.REJ_DIV_CD,
        A.FAC_ID, A.OPER_ID, A.OWNR_CD, A.CRET_CD, A.PROD_ID, A.IGOT_ID,
        A.BLK_ID, A.SUBLOT_ID, A.USER_LOT_ID, A.EQP_ID, A.BEF_BAD_RSN_CD,
        A.AFT_BAD_RSN_CD, A.REJ_GROUP, A.OPER1_GROUP, A.OPER2_GROUP,
        A.RESPON, A.ALLO_GROUP, A.RESPON_RATIO, A.IN_QTY, A.OUT_QTY,
        A.LOSS_QTY, A.REAL_DPT_GROUP, A.HST_REG_DTTM, 'ORI' AS DATA_TYPE, A.DATA_CHG_DTTM
    FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLWAFSTD_S A
    WHERE A.WAF_SIZE = '{config['waf_size']}'
      AND A.BASE_DT = '{target_date}'
      AND A.FAC_ID IN ('{fac_ids_str}')
      AND A.OPER_ID != '3200'

    UNION ALL

    SELECT 
        A.WAF_ID, A.WAF_SEQ, A.WAF_SIZE, A.BASE_DT, A.DIV_CD, A.REJ_DIV_CD,
        A.FAC_ID, A.OPER_ID, A.OWNR_CD, A.CRET_CD, A.PROD_ID, A.IGOT_ID,
        A.BLK_ID, A.SUBLOT_ID, A.USER_LOT_ID, A.EQP_ID, A.BEF_BAD_RSN_CD,
        A.AFT_BAD_RSN_CD, A.REJ_GROUP, A.OPER1_GROUP, A.OPER2_GROUP,
        A.RESPON, A.ALLO_GROUP, A.RESPON_RATIO, A.IN_QTY, A.OUT_QTY,
        A.LOSS_QTY, A.REAL_DPT_GROUP, NULL AS HST_REG_DTTM, 'MNL' AS DATA_TYPE, A.DATA_CHG_DTTM
    FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S A
    WHERE A.WAF_SIZE = '{config['waf_size']}'
      AND A.BASE_DT = '{target_date}'
      AND A.FAC_ID IN ('{fac_ids_str}')
      AND A.OPER_ID != '3200'
),
-- 2. 불량 데이터에 장비명 추가
step2_joined AS (
    SELECT 
        b.*,
        eqp.EQP_NM
    FROM step1_base b
    LEFT JOIN oracle.PMDW_MGR.DW_BA_CM_STDPEQP_M eqp 
        ON eqp.FAC_ID = b.FAC_ID AND eqp.EQP_ID = b.EQP_ID
),
step2_with_prod AS (
    SELECT 
        j.*,
        p.CUST_SITE_NM,
        codes.CD_NM AS GRADE_CS
    FROM step2_joined j
    LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M p 
        ON p.PROD_ID = j.PROD_ID 
       AND p.SPEC_DIV_CD = 'PS'
    LEFT JOIN oracle.DMS_MGR.TB_FX_CODES codes 
        ON codes.UP_CD = 'DMS010' 
       AND codes.SYS_CD = 'DMS' 
       AND codes.CD_VAL = p.GRD_CD_NM
),

-- 3. IGOT_ID 기준 WAF_SEQ ↔ BLK_ID 매핑 테이블 생성
waf_blk_mapping AS (
    SELECT 
        IGOT_ID,
        WAF_SEQ,
        BLK_ID
    FROM oracle.PMDW_MGR.DW_QM_PW_WAFOPER_H
    WHERE HST_DIV_CD = 'OC'
      AND FAC_ID IN ('{fac_ids_str}')
      AND OPER_ID IN ('3300','3335','3670','3696','6100','7000')
      AND BASE_DT BETWEEN '{date_range_start}' AND '{target_date}'
      AND IGOT_ID IS NOT NULL
      AND WAF_SEQ IS NOT NULL
      AND BLK_ID IS NOT NULL
    GROUP BY IGOT_ID, WAF_SEQ, BLK_ID
),
-- 4. 모든 공정 이력 추출 (3200 포함)
ope_history_all AS (
    SELECT 
        B.IGOT_ID,
        B.WAF_SEQ,
        B.BLK_ID,
        B.OPER_ID,
        B.EQP_ID,
        B.REG_DTTM,
        B.SLOT_NO,
        eqp.EQP_NM
    FROM oracle.PMDW_MGR.DW_QM_PW_WAFOPER_H B
    LEFT JOIN oracle.PMDW_MGR.DW_BA_CM_STDPEQP_M eqp 
        ON eqp.EQP_ID = B.EQP_ID
    WHERE B.HST_DIV_CD = 'OC'
      AND B.FAC_ID IN ('{fac_ids_str}')
      AND B.OPER_ID IN ('3200','3300','3335','3670','3696','6100','7000')
      AND B.BASE_DT BETWEEN '{date_range_start}' AND '{target_date}'
      AND B.IGOT_ID IS NOT NULL
      AND (B.WAF_SEQ IS NOT NULL OR B.BLK_ID IS NOT NULL)
),
-- 5. 중복 제거
latest_ope AS (
    SELECT 
        IGOT_ID,
        WAF_SEQ,
        BLK_ID,
        OPER_ID,
        EQP_ID,
        REG_DTTM,
        SLOT_NO,
        EQP_NM,
        ROW_NUMBER() OVER (
            PARTITION BY IGOT_ID, 
                         COALESCE(CAST(WAF_SEQ AS VARCHAR), BLK_ID), 
                         OPER_ID 
            ORDER BY REG_DTTM DESC, SLOT_NO DESC
        ) AS RN
    FROM ope_history_all
),
-- 6. 피벗: 공정별 장비 정보
pivot_ope AS (
    SELECT 
        IGOT_ID,
        WAF_SEQ,
        MAX(CASE WHEN OPER_ID = '3200' THEN EQP_NM END) AS EQP_NM_300_WF_3200,
        MAX(CASE WHEN OPER_ID = '3200' THEN REG_DTTM END) AS REG_DTTM_300_WF_3200,
        MAX(CASE WHEN OPER_ID = '3200' THEN SLOT_NO END) AS SLOT_NO_300_WF_3200,
        MAX(CASE WHEN OPER_ID = '3300' THEN EQP_NM END) AS EQP_NM_300_WF_3300,
        MAX(CASE WHEN OPER_ID = '3300' THEN REG_DTTM END) AS REG_DTTM_300_WF_3300,
        MAX(CASE WHEN OPER_ID = '3300' THEN SLOT_NO END) AS SLOT_NO_300_WF_3300,
        MAX(CASE WHEN OPER_ID = '3335' THEN EQP_NM END) AS EQP_NM_300_WF_3335,
        MAX(CASE WHEN OPER_ID = '3335' THEN REG_DTTM END) AS REG_DTTM_300_WF_3335,
        MAX(CASE WHEN OPER_ID = '3335' THEN SLOT_NO END) AS SLOT_NO_300_WF_3335,
        MAX(CASE WHEN OPER_ID = '3670' THEN EQP_NM END) AS EQP_NM_300_WF_3670,
        MAX(CASE WHEN OPER_ID = '3670' THEN REG_DTTM END) AS REG_DTTM_300_WF_3670,
        MAX(CASE WHEN OPER_ID = '3670' THEN SLOT_NO END) AS SLOT_NO_300_WF_3670,
        MAX(CASE WHEN OPER_ID = '3696' THEN EQP_NM END) AS EQP_NM_300_WF_3696,
        MAX(CASE WHEN OPER_ID = '3696' THEN REG_DTTM END) AS REG_DTTM_300_WF_3696,
        MAX(CASE WHEN OPER_ID = '3696' THEN SLOT_NO END) AS SLOT_NO_300_WF_3696,
        MAX(CASE WHEN OPER_ID = '6100' THEN EQP_NM END) AS EQP_NM_300_WF_6100,
        MAX(CASE WHEN OPER_ID = '6100' THEN REG_DTTM END) AS REG_DTTM_300_WF_6100,
        MAX(CASE WHEN OPER_ID = '6100' THEN SLOT_NO END) AS SLOT_NO_300_WF_6100,
        MAX(CASE WHEN OPER_ID = '7000' THEN EQP_NM END) AS EQP_NM_300_WF_7000,
        MAX(CASE WHEN OPER_ID = '7000' THEN REG_DTTM END) AS REG_DTTM_300_WF_7000,
        MAX(CASE WHEN OPER_ID = '7000' THEN SLOT_NO END) AS SLOT_NO_300_WF_7000
    FROM latest_ope
    WHERE RN = 1
    GROUP BY IGOT_ID, WAF_SEQ
),
-- 7. 최종 매핑
final_with_all_history AS (
    SELECT 
        b.*,
        p.EQP_NM_300_WF_3200,
        p.REG_DTTM_300_WF_3200,
        p.SLOT_NO_300_WF_3200,
        p.EQP_NM_300_WF_3300,
        p.REG_DTTM_300_WF_3300,
        p.SLOT_NO_300_WF_3300,
        p.EQP_NM_300_WF_3335,
        p.REG_DTTM_300_WF_3335,
        p.SLOT_NO_300_WF_3335,
        p.EQP_NM_300_WF_3670,
        p.REG_DTTM_300_WF_3670,
        p.SLOT_NO_300_WF_3670,
        p.EQP_NM_300_WF_3696,
        p.REG_DTTM_300_WF_3696,
        p.SLOT_NO_300_WF_3696,
        p.EQP_NM_300_WF_6100,
        p.REG_DTTM_300_WF_6100,
        p.SLOT_NO_300_WF_6100,
        p.EQP_NM_300_WF_7000,
        p.REG_DTTM_300_WF_7000,
        p.SLOT_NO_300_WF_7000
    FROM step2_with_prod b
    LEFT JOIN waf_blk_mapping m 
        ON m.IGOT_ID = b.IGOT_ID 
       AND m.WAF_SEQ = b.WAF_SEQ
    LEFT JOIN pivot_ope p 
        ON p.IGOT_ID = b.IGOT_ID 
       AND p.WAF_SEQ = b.WAF_SEQ
)

--  최종 출력
SELECT *
FROM final_with_all_history
ORDER BY HST_REG_DTTM DESC
"""

def DATA_3210_wafering_300(target_date, config):
    """
    팀별 Loss Rate 조회 쿼리 (300mm 웨이퍼링 공정 기준)
    
    Args:
        target_date (str): 조회 일자 ('YYYYMMDD' 형식, 예: '20260127')
        config (dict): 쿼리 설정값
            - waf_size: '300'
            - oper_div_l: 'WF'
            - grade_filter: 'PN'
            - fac_ids: ['WF7','WF8','WFA','FPC7','FPC8']

    Returns:
        str: Trino SQL 쿼리 문자열
    """
    # 날짜 포맷 변환
    target_date_nm = datetime.strptime(target_date, '%Y%m%d').strftime('%y-%m-%d')
    target_ym = target_date[:6]  # '202601'
    fac_ids_str = "','".join(config['fac_ids'])
    
    return f"""
-- =============================================
-- [Trino] LossYieldService.SELECT_TEAM_LOSS_RATE (config 기반)
-- =============================================
WITH TBL_DTP_GRP AS (
    SELECT *
    FROM (
        SELECT 
            S2.TEAMGRP_NM, 
            S2.SORT_SEQ, 
            S1.DPT_CD, 
            ROW_NUMBER() OVER (PARTITION BY S1.DPT_CD ORDER BY ST_DT DESC) AS M
        FROM oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRPDTL_M S1
        JOIN oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRP_M S2
            ON S1.TEAMGRP_CD = S2.TEAMGRP_CD
            AND S1.WAF_SIZE = S2.WAF_SIZE
            AND S1.OPER_DIV_L = S2.OPER_DIV_L
            AND S1.TARGET_DIV_CD IN ('A','L')
            AND S1.WAF_SIZE = '{config['waf_size']}'
            AND S1.OPER_DIV_L = '{config['oper_div_l']}'
            AND S1.ED_DT >= '{target_date}'
            AND S1.ST_DT <= '{target_date}'
    ) A
    WHERE M = 1
),
-- 일자 목록 (단일 일자)
DATE_LIST AS (
    SELECT 
        '{target_date}' AS BASE_DT,
        '{target_date_nm}' AS BASE_DT_NM
),
-- 일별 목표(GOAL) 조회
DAILY_GOAL AS (
    SELECT 
        Z.BASE_DT_NM,
        A.YLD_DIV3_CD AS REJ_GROUP,
        'D' AS CATEGORY,
        SUM(A.GOAL_VAL) AS GOAL_RATIO
    FROM DATE_LIST Z
    INNER JOIN (
        SELECT DISTINCT
            BASE_YM,
            WAF_SIZE,
            YLD_DIV1_CD,
            YLD_DIV3_CD,
            GOAL_DIV_CD,
            YLD_PLAN_TYPE,
            REF_DIV2,
            GOAL_VAL
        FROM oracle.PMDW_MGR.DW_BA_CM_YLDPLAN_M
        WHERE 
            WAF_SIZE = '{config['waf_size']}'
            AND YLD_DIV1_CD = '{config['oper_div_l']}'
            AND GOAL_DIV_CD = 'BAD-RATE'
            AND YLD_PLAN_TYPE = 'BP'
            AND REF_DIV2 = '{config['grade_filter']}'
            AND BASE_YM = '{target_ym}'
    ) A
        ON A.BASE_YM = SUBSTR(Z.BASE_DT, 1, 6)
    GROUP BY 
        Z.BASE_DT_NM,
        A.YLD_DIV3_CD
),
-- REJ_GROUP 목록 추출
REJ_GROUP_LIST AS (
    SELECT DISTINCT REJ_GROUP
    FROM (
        SELECT REJ_GROUP FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
        WHERE WAF_SIZE = '{config['waf_size']}' 
          AND BASE_DT = '{target_date}'
          AND DIV_CD <> 'COM_QTY'

        UNION

        SELECT REJ_GROUP FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
        WHERE WAF_SIZE = '{config['waf_size']}' 
          AND BASE_DT = '{target_date}'
          AND DIV_CD <> 'COM_QTY'
    ) A
),
-- Loss 및 ComQty 통합
LOSS_INFO AS (
    -- ---------------------------------------------------------- 분자: 불량량
    SELECT
        A.WAF_SIZE,
        B.OPER_DIV_L,
        A.BASE_DT,
        '' AS DIV_CD,
        A.REJ_GROUP,
        COALESCE(CASE WHEN A.DIV_CD = 'RESC_HG_QTY' THEN A.BEF_BAD_RSN_CD ELSE A.AFT_BAD_RSN_CD END, 'N/A') AS AFT_BAD_RSN_CD,
        COALESCE(E.TEAMGRP_NM, A.REAL_DPT_GROUP) AS REAL_DPT_GROUP,
        A.BEF_BAD_RSN_CD,
        SUM(A.LOSS_QTY) AS LOSS_QTY,
        0 AS LOSS_QTY_TOT,
        0 AS MGR_QTY,
        NULL AS MS_ID,
        NULL AS EQP_ID
    FROM (
        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
        WHERE WAF_SIZE = '{config['waf_size']}'
          AND BASE_DT = '{target_date}'

        UNION ALL

        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
        WHERE WAF_SIZE = '{config['waf_size']}'
          AND BASE_DT = '{target_date}'
    ) A
    INNER JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M B
        ON B.FAC_ID = A.FAC_ID 
       AND B.OPER_ID = A.OPER_ID
       AND B.OPER_DIV_L = '{config['oper_div_l']}'
       AND B.FAC_ID IN ('{fac_ids_str}')
    LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M D
        ON D.PROD_ID = A.PROD_ID 
       AND D.SPEC_DIV_CD = 'PS'
       AND (D.GRD_CD_NM = '{config['grade_filter']}' OR D.GRD_CD_NM_PS = '{config['grade_filter']}')
    LEFT JOIN (
        SELECT DPT_CD, TEAMGRP_NM, ROW_NUMBER() OVER (PARTITION BY DPT_CD ORDER BY SORT_SEQ) AS RN
        FROM TBL_DTP_GRP
    ) E ON E.DPT_CD = A.REAL_DPT_GROUP AND E.RN = 1
    WHERE
        A.DIV_CD <> 'COM_QTY'
        AND A.WAF_SIZE = '{config['waf_size']}'
        AND A.BASE_DT = '{target_date}'
        AND CONCAT(A.WAF_SIZE, B.OPER_DIV_L) NOT IN ('200WF', '300EPI')
    GROUP BY 
        A.WAF_SIZE, B.OPER_DIV_L, A.BASE_DT, A.REJ_GROUP,
        COALESCE(CASE WHEN A.DIV_CD = 'RESC_HG_QTY' THEN A.BEF_BAD_RSN_CD ELSE A.AFT_BAD_RSN_CD END, 'N/A'),
        COALESCE(E.TEAMGRP_NM, A.REAL_DPT_GROUP), A.BEF_BAD_RSN_CD

    UNION ALL

    -- ---------------------------------------------------------- 분모: 합계량
    SELECT
        A.WAF_SIZE,
        B.OPER_DIV_L,
        A.BASE_DT,
        'COM_QTY' AS DIV_CD,
        'TOTAL' AS REJ_GROUP,
        COALESCE(A.AFT_BAD_RSN_CD, 'N/A') AS AFT_BAD_RSN_CD,
        A.REAL_DPT_GROUP,
        NULL AS BEF_BAD_RSN_CD,
        0 AS LOSS_QTY,
        0 AS LOSS_QTY_TOT,
        SUM(A.IN_QTY) AS MGR_QTY,
        NULL AS MS_ID,
        NULL AS EQP_ID
    FROM (
        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
        WHERE WAF_SIZE = '{config['waf_size']}'
          AND BASE_DT = '{target_date}'

        UNION ALL

        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
        WHERE WAF_SIZE = '{config['waf_size']}'
          AND BASE_DT = '{target_date}'
    ) A
    INNER JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M B
        ON B.FAC_ID = A.FAC_ID 
       AND B.OPER_ID = A.OPER_ID
       AND B.OPER_DIV_L = '{config['oper_div_l']}'
       AND B.FAC_ID IN ('{fac_ids_str}')
    WHERE
        A.DIV_CD = 'COM_QTY'
        AND A.WAF_SIZE = '{config['waf_size']}'
        AND A.BASE_DT = '{target_date}'
        AND CONCAT(A.WAF_SIZE, B.OPER_DIV_L) NOT IN ('200WF', '300EPI')
    GROUP BY 
        A.WAF_SIZE, B.OPER_DIV_L, A.BASE_DT, 
        COALESCE(A.AFT_BAD_RSN_CD, 'N/A'), A.REAL_DPT_GROUP
),
-- 일별 Loss 정보
MGR_LOSS_INFO AS (
    SELECT 
        Z.WAF_SIZE, 
        Z.OPER_DIV_L,
        date_format(date_parse(Z.BASE_DT, '%Y%m%d'), '%y-%m-%d') AS BASE_DT_NM,
        Z.REJ_GROUP, 
        Z.AFT_BAD_RSN_CD, 
        Z.BEF_BAD_RSN_CD,
        SUM(Z.LOSS_QTY) AS LOSS_QTY, 
        SUM(Z.LOSS_QTY_TOT) AS LOSS_QTY_TOT,
        'D' AS CATEGORY
    FROM LOSS_INFO Z
    WHERE Z.DIV_CD = ''
    GROUP BY 
        Z.WAF_SIZE, Z.OPER_DIV_L, Z.BASE_DT, Z.REJ_GROUP, Z.AFT_BAD_RSN_CD, Z.BEF_BAD_RSN_CD
),
-- MGR_COMQTY_INFO: REJ_GROUP_LIST 기반 MGR_QTY 복제
MGR_COMQTY_INFO AS (
    SELECT 
        C.WAF_SIZE, 
        C.OPER_DIV_L,
        C.BASE_DT_NM,
        R.REJ_GROUP,
        C.COM_QTY,
        C.MGR_QTY,
        C.CATEGORY
    FROM (
        SELECT 
            Z.WAF_SIZE, 
            Z.OPER_DIV_L,
            date_format(date_parse(Z.BASE_DT, '%Y%m%d'), '%y-%m-%d') AS BASE_DT_NM,
            SUM(Z.LOSS_QTY) AS COM_QTY,
            SUM(Z.MGR_QTY) AS MGR_QTY,
            'D' AS CATEGORY
        FROM LOSS_INFO Z
        WHERE Z.DIV_CD = 'COM_QTY'
        GROUP BY 
            Z.WAF_SIZE, Z.OPER_DIV_L, Z.BASE_DT
    ) C
    CROSS JOIN REJ_GROUP_LIST R
),
-- 최종 데이터 조합
FINAL_DATA AS (
    SELECT 
        L.CATEGORY,
        L.BASE_DT_NM,
        L.REJ_GROUP,
        L.AFT_BAD_RSN_CD,
        CAST(CASE WHEN C.MGR_QTY > 0 THEN CAST(L.LOSS_QTY AS DOUBLE) / NULLIF(C.MGR_QTY, 0) ELSE 0.0 END AS DECIMAL(24,16)) AS LOSS_RATIO,
        CAST(COALESCE(G.GOAL_RATIO, 0.0) AS DECIMAL(24,16)) AS GOAL_RATIO,
        CAST(COALESCE(G.GOAL_RATIO, 0.0) AS DECIMAL(24,16)) AS GOAL_RATIO_SUM,
        CAST(CASE WHEN C.MGR_QTY > 0 THEN (CAST(L.LOSS_QTY AS DOUBLE) / NULLIF(C.MGR_QTY, 0)) - COALESCE(G.GOAL_RATIO, 0.0) ELSE -COALESCE(G.GOAL_RATIO, 0.0) END AS DECIMAL(24,16)) AS GAP_RATIO,
        L.LOSS_QTY,
        C.MGR_QTY,
        CAST(NULL AS DECIMAL(24,16)) AS COM_QTY,
        99999 AS SORT_CD,
        'N/A' AS PROD_GRP,
        'N/A' AS EQP_NM,
        'N/A' AS EQP_MODEL_NM,
        '일' AS CATEGORY_NAME
    FROM MGR_LOSS_INFO L
    LEFT JOIN MGR_COMQTY_INFO C
        ON C.BASE_DT_NM = L.BASE_DT_NM
       AND C.REJ_GROUP = L.REJ_GROUP
    LEFT JOIN DAILY_GOAL G
        ON G.BASE_DT_NM = L.BASE_DT_NM
       AND G.REJ_GROUP = L.REJ_GROUP
)
-- 최종 출력
SELECT * FROM FINAL_DATA
ORDER BY BASE_DT_NM, REJ_GROUP, LOSS_QTY DESC
"""



def DATA_3210_wafering_300_3months(target_date, config):
    """
    3개월 평균 목표값 계산용 쿼리
    기간: 2025-11-01 \~ 2026-01-31 (예시)
    
    Args:
        start_date: 조회 시작일 ('YYYYMMDD')
        end_date: 조회 종료일 ('YYYYMMDD')
        config: QUERY_CONFIG (waf_size, fac_ids 등)
    """
    # 여기서는 전역 _RUNTIME_CONFIG 사용
    if 'start_3m' not in _RUNTIME_CONFIG or 'end_3m' not in _RUNTIME_CONFIG:
        raise ValueError("config에 'start_3m' 또는 'end_3m'이 없습니다.")
    
    # 날짜 포맷 변환
    start_3m = _RUNTIME_CONFIG['start_3m']
    end_3m = _RUNTIME_CONFIG['end_3m']
    fac_ids_str = "','".join(config['fac_ids'])
    
    return f"""
-- =============================================
-- [Trino] LossYieldService.SELECT_TEAM_LOSS_RATE (config 기반)
-- =============================================
WITH TBL_DTP_GRP AS (
    SELECT *
    FROM (
        SELECT 
            S2.TEAMGRP_NM, 
            S2.SORT_SEQ, 
            S1.DPT_CD, 
            ROW_NUMBER() OVER (PARTITION BY S1.DPT_CD ORDER BY ST_DT DESC) AS M
        FROM oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRPDTL_M S1
        JOIN oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRP_M S2
            ON S1.TEAMGRP_CD = S2.TEAMGRP_CD
            AND S1.WAF_SIZE = S2.WAF_SIZE
            AND S1.OPER_DIV_L = S2.OPER_DIV_L
            AND S1.TARGET_DIV_CD IN ('A','L')
            AND S1.WAF_SIZE = '{config['waf_size']}'
            AND S1.OPER_DIV_L = '{config['oper_div_l']}'
            AND S1.ED_DT >= '{start_3m}'
            AND S1.ST_DT <= '{end_3m}'
    ) A
    WHERE M = 1
),

-- 일자 목록 (단일 일자)
DATE_LIST AS (
    SELECT 
        '{target_date}' AS BASE_DT
),
-- 일별 목표(GOAL) 조회
DAILY_GOAL AS (
    SELECT 
        A.YLD_DIV3_CD AS REJ_GROUP,
        'D' AS CATEGORY,
        SUM(A.GOAL_VAL) AS GOAL_RATIO
    FROM DATE_LIST Z
    INNER JOIN (
        SELECT DISTINCT
            BASE_YM,
            WAF_SIZE,
            YLD_DIV1_CD,
            YLD_DIV3_CD,
            GOAL_DIV_CD,
            YLD_PLAN_TYPE,
            REF_DIV2,
            GOAL_VAL
        FROM oracle.PMDW_MGR.DW_BA_CM_YLDPLAN_M
        WHERE 
            WAF_SIZE = '{config['waf_size']}'
            AND YLD_DIV1_CD = '{config['oper_div_l']}'
            AND GOAL_DIV_CD = 'BAD-RATE'
            AND YLD_PLAN_TYPE = 'BP'
            AND REF_DIV2 = '{config['grade_filter']}'
    ) A
        ON A.BASE_YM = SUBSTR(Z.BASE_DT, 1, 6)
    GROUP BY 
        A.YLD_DIV3_CD
),
-- REJ_GROUP 목록 추출
REJ_GROUP_LIST AS (
    SELECT DISTINCT REJ_GROUP
    FROM (
        SELECT REJ_GROUP FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
        WHERE WAF_SIZE = '{config['waf_size']}' 
          AND BASE_DT >= '{start_3m}'
          AND BASE_DT <= '{end_3m}'
          AND DIV_CD <> 'COM_QTY'

        UNION

        SELECT REJ_GROUP FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
        WHERE WAF_SIZE = '{config['waf_size']}' 
          AND BASE_DT >= '{start_3m}'
          AND BASE_DT <= '{end_3m}'
          AND DIV_CD <> 'COM_QTY'
    ) A
),
-- Loss 및 ComQty 통합
LOSS_INFO AS (
    -- ---------------------------------------------------------- 분자: 불량량
    SELECT
        A.WAF_SIZE,
        B.OPER_DIV_L,
        A.BASE_DT,
        '' AS DIV_CD,
        A.REJ_GROUP,
        COALESCE(CASE WHEN A.DIV_CD = 'RESC_HG_QTY' THEN A.BEF_BAD_RSN_CD ELSE A.AFT_BAD_RSN_CD END, 'N/A') AS AFT_BAD_RSN_CD,
        COALESCE(E.TEAMGRP_NM, A.REAL_DPT_GROUP) AS REAL_DPT_GROUP,
        A.BEF_BAD_RSN_CD,
        SUM(A.LOSS_QTY) AS LOSS_QTY,
        0 AS LOSS_QTY_TOT,
        0 AS MGR_QTY,
        NULL AS MS_ID,
        NULL AS EQP_ID
    FROM (
        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
        WHERE WAF_SIZE = '{config['waf_size']}'
        AND BASE_DT >= '{start_3m}'
        AND BASE_DT <= '{end_3m}'

        UNION ALL

        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
        WHERE WAF_SIZE = '{config['waf_size']}'
        AND BASE_DT >= '{start_3m}'
        AND BASE_DT <= '{end_3m}'
    ) A
    INNER JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M B
        ON B.FAC_ID = A.FAC_ID 
       AND B.OPER_ID = A.OPER_ID
       AND B.OPER_DIV_L = '{config['oper_div_l']}'
       AND B.FAC_ID IN ('{fac_ids_str}')
    LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M D
        ON D.PROD_ID = A.PROD_ID 
       AND D.SPEC_DIV_CD = 'PS'
       AND (D.GRD_CD_NM = '{config['grade_filter']}' OR D.GRD_CD_NM_PS = '{config['grade_filter']}')
    LEFT JOIN (
        SELECT DPT_CD, TEAMGRP_NM, ROW_NUMBER() OVER (PARTITION BY DPT_CD ORDER BY SORT_SEQ) AS RN
        FROM TBL_DTP_GRP
    ) E ON E.DPT_CD = A.REAL_DPT_GROUP AND E.RN = 1
    WHERE
        A.DIV_CD <> 'COM_QTY'
        AND A.WAF_SIZE = '{config['waf_size']}'
        AND BASE_DT >= '{start_3m}'
        AND BASE_DT <= '{end_3m}'
        AND CONCAT(A.WAF_SIZE, B.OPER_DIV_L) NOT IN ('200WF', '300EPI')
    GROUP BY 
        A.WAF_SIZE, B.OPER_DIV_L, A.BASE_DT, A.REJ_GROUP,
        COALESCE(CASE WHEN A.DIV_CD = 'RESC_HG_QTY' THEN A.BEF_BAD_RSN_CD ELSE A.AFT_BAD_RSN_CD END, 'N/A'),
        COALESCE(E.TEAMGRP_NM, A.REAL_DPT_GROUP), A.BEF_BAD_RSN_CD

    UNION ALL

    -- ---------------------------------------------------------- 분모: 합계량
    SELECT
        A.WAF_SIZE,
        B.OPER_DIV_L,
        A.BASE_DT,
        'COM_QTY' AS DIV_CD,
        'TOTAL' AS REJ_GROUP,
        COALESCE(A.AFT_BAD_RSN_CD, 'N/A') AS AFT_BAD_RSN_CD,
        A.REAL_DPT_GROUP,
        NULL AS BEF_BAD_RSN_CD,
        0 AS LOSS_QTY,
        0 AS LOSS_QTY_TOT,
        SUM(A.IN_QTY) AS MGR_QTY,
        NULL AS MS_ID,
        NULL AS EQP_ID
    FROM (
        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
        WHERE WAF_SIZE = '{config['waf_size']}'
        AND BASE_DT >= '{start_3m}'
        AND BASE_DT <= '{end_3m}'

        UNION ALL

        SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
        FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
        WHERE WAF_SIZE = '{config['waf_size']}'
        AND BASE_DT >= '{start_3m}'
        AND BASE_DT <= '{end_3m}'
    ) A
    INNER JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M B
        ON B.FAC_ID = A.FAC_ID 
       AND B.OPER_ID = A.OPER_ID
       AND B.OPER_DIV_L = '{config['oper_div_l']}'
       AND B.FAC_ID IN ('{fac_ids_str}')
    WHERE
        A.DIV_CD = 'COM_QTY'
        AND A.WAF_SIZE = '{config['waf_size']}'
        AND BASE_DT >= '{start_3m}'
        AND BASE_DT <= '{end_3m}'
        AND CONCAT(A.WAF_SIZE, B.OPER_DIV_L) NOT IN ('200WF', '300EPI')
    GROUP BY 
        A.WAF_SIZE, B.OPER_DIV_L, A.BASE_DT, 
        COALESCE(A.AFT_BAD_RSN_CD, 'N/A'), A.REAL_DPT_GROUP
),
-- 일별 Loss 정보
MGR_LOSS_INFO AS (
    SELECT 
        Z.WAF_SIZE, 
        Z.OPER_DIV_L,
        Z.REJ_GROUP, 
        Z.AFT_BAD_RSN_CD, 
        Z.BEF_BAD_RSN_CD,
        SUM(Z.LOSS_QTY) AS LOSS_QTY, 
        SUM(Z.LOSS_QTY_TOT) AS LOSS_QTY_TOT,
        'D' AS CATEGORY
    FROM LOSS_INFO Z
    WHERE Z.DIV_CD = ''
    GROUP BY 
        Z.WAF_SIZE, Z.OPER_DIV_L, Z.REJ_GROUP, Z.AFT_BAD_RSN_CD, Z.BEF_BAD_RSN_CD
),
-- MGR_COMQTY_INFO: REJ_GROUP_LIST 기반 MGR_QTY 복제
MGR_COMQTY_INFO AS (
    SELECT 
        C.WAF_SIZE, 
        C.OPER_DIV_L,
        R.REJ_GROUP,
        C.COM_QTY,
        C.MGR_QTY,
        C.CATEGORY
    FROM (
        SELECT 
            Z.WAF_SIZE, 
            Z.OPER_DIV_L,
            SUM(Z.LOSS_QTY) AS COM_QTY,
            SUM(Z.MGR_QTY) AS MGR_QTY,
            'D' AS CATEGORY
        FROM LOSS_INFO Z
        WHERE Z.DIV_CD = 'COM_QTY'
        GROUP BY 
            Z.WAF_SIZE, Z.OPER_DIV_L, Z.BASE_DT
    ) C
    CROSS JOIN REJ_GROUP_LIST R
),
-- 최종 데이터 조합
FINAL_DATA AS (
    SELECT 
        L.CATEGORY,
        L.REJ_GROUP,
        L.AFT_BAD_RSN_CD,
        CAST(CASE WHEN C.MGR_QTY > 0 THEN CAST(L.LOSS_QTY AS DOUBLE) / NULLIF(C.MGR_QTY, 0) ELSE 0.0 END AS DECIMAL(24,16)) AS LOSS_RATIO,
        CAST(COALESCE(G.GOAL_RATIO, 0.0) AS DECIMAL(24,16)) AS GOAL_RATIO,
        CAST(COALESCE(G.GOAL_RATIO, 0.0) AS DECIMAL(24,16)) AS GOAL_RATIO_SUM,
        CAST(CASE WHEN C.MGR_QTY > 0 THEN (CAST(L.LOSS_QTY AS DOUBLE) / NULLIF(C.MGR_QTY, 0)) - COALESCE(G.GOAL_RATIO, 0.0) ELSE -COALESCE(G.GOAL_RATIO, 0.0) END AS DECIMAL(24,16)) AS GAP_RATIO,
        L.LOSS_QTY,
        C.MGR_QTY,
        CAST(NULL AS DECIMAL(24,16)) AS COM_QTY,
        99999 AS SORT_CD,
        'N/A' AS PROD_GRP,
        'N/A' AS EQP_NM,
        'N/A' AS EQP_MODEL_NM,
        '일' AS CATEGORY_NAME
    FROM MGR_LOSS_INFO L
    LEFT JOIN MGR_COMQTY_INFO C
       ON C.REJ_GROUP = L.REJ_GROUP
    LEFT JOIN DAILY_GOAL G
       ON G.REJ_GROUP = L.REJ_GROUP
)
-- 최종 출력
SELECT * FROM FINAL_DATA
ORDER BY REJ_GROUP, LOSS_QTY DESC
"""



def DATA_LOT_3210_wafering_300(target_date, config):
    """
    → OPER_ID 조건 없음
    → PART_NO 추출 로직 포함
    → config 주입만 정확히 수행
    """
    waf_size = config['waf_size']
    fac_ids_str = "','".join(config['fac_ids'])
    oper_div_l = config['oper_div_l']
    grade_filter = config['grade_filter']

    if grade_filter == 'PN':
        grade_cs_condition = "TRUE"
        grade_ps_condition = "TRUE"
    else:
        grade_cs_condition = f"C.GRD_CD_NM = '{grade_filter}'"
        grade_ps_condition = f"C.GRD_CD_NM_PS = '{grade_filter}'"

    return f"""
WITH
-- (1) Z: 원본 + 보정 데이터 통합
Z AS (
-- (1-1) 원본 데이터
SELECT
    A.WAF_SIZE,
    A.BASE_DT,
    A.DIV_CD,
    A.REJ_DIV_CD,
    A.FAC_ID,
    A.OPER_ID,
    A.OWNR_CD,
    A.CRET_CD,
    A.PROD_ID,
    A.IGOT_ID,
    A.BLK_ID,
    A.SUBLOT_ID,
    A.USER_LOT_ID,
    A.EQP_ID,
    COALESCE(TRIM(BEF_ALIAS.ALIAS_RSN_CD), A.BEF_BAD_RSN_CD) AS BEF_BAD_RSN_CD,
    COALESCE(TRIM(AFT_ALIAS.ALIAS_RSN_CD), A.AFT_BAD_RSN_CD) AS AFT_BAD_RSN_CD,
    A.REJ_GROUP,
    A.OPER1_GROUP,
    A.OPER2_GROUP,
    A.RESPON,
    A.ALLO_GROUP,
    A.RESPON_RATIO,
    A.IN_QTY,
    A.OUT_QTY,
    A.LOSS_QTY,
    A.REAL_DPT_GROUP,
    D.CD_NM AS GRD_CD_NM_CS,
    E.CD_NM AS GRD_CD_NM_PS,
    C.CUST_SITE_NM,
    SUBSTR(B.BESOF_BASE_YW_NM, 3) AS WEEK_DAY_NM,
    A.DATA_CHG_DTTM
FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S A

-- BEF 매핑
LEFT JOIN (
    SELECT
        XX.REJ_RSN_GRP,
        XX.REJ_RSN_CD,
        XX.ALIAS_RSN_CD,
        ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
    FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
    WHERE XX.WAF_SIZE = '{waf_size}'
      AND XX.PROD_DIV_CD = CASE WHEN '{oper_div_l}' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS BEF_ALIAS
ON BEF_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND BEF_ALIAS.REJ_RSN_CD = A.BEF_BAD_RSN_CD
AND BEF_ALIAS.RN = 1

-- AFT 매핑
LEFT JOIN (
    SELECT
        XX.REJ_RSN_GRP,
        XX.REJ_RSN_CD,
        XX.ALIAS_RSN_CD,
        ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
    FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
    WHERE XX.WAF_SIZE = '{waf_size}'
      AND XX.PROD_DIV_CD = CASE WHEN '{oper_div_l}' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS AFT_ALIAS
ON AFT_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND AFT_ALIAS.REJ_RSN_CD = A.AFT_BAD_RSN_CD
AND AFT_ALIAS.RN = 1

JOIN oracle.PMDW_MGR.DW_BA_CM_BASEDATE_M B
ON B.BASE_DT = A.BASE_DT

LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M C
ON C.PROD_ID = A.PROD_ID
AND C.SPEC_DIV_CD = 'PS'

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES D
ON D.UP_CD = 'DMS010'
AND D.SYS_CD = 'DMS'
AND D.CD_VAL = C.GRD_CD_NM

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES E
ON E.UP_CD = 'DMS010'
AND E.SYS_CD = 'DMS'
AND E.CD_VAL = C.GRD_CD_NM_PS

JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M Z1
ON Z1.FAC_ID = A.FAC_ID
AND Z1.OPER_ID = A.OPER_ID

WHERE
    1 = 1
    AND Z1.OPER_DIV_L = '{oper_div_l}'
    AND (CASE WHEN '{grade_filter}' = 'PN' THEN TRUE ELSE {grade_cs_condition} END)
    AND (CASE WHEN '{grade_filter}' = 'PN' THEN TRUE ELSE {grade_ps_condition} END)
    AND Z1.FAC_ID IN ('{fac_ids_str}')
    AND 'N' = 'N'
    AND A.WAF_SIZE = '{waf_size}'
    AND A.BASE_DT = '{target_date}'

UNION ALL

-- (1-2) 보정 데이터
SELECT
    A.WAF_SIZE,
    A.BASE_DT,
    A.DIV_CD,
    A.REJ_DIV_CD,
    A.FAC_ID,
    A.OPER_ID,
    A.OWNR_CD,
    A.CRET_CD,
    A.PROD_ID,
    A.IGOT_ID,
    A.BLK_ID,
    A.SUBLOT_ID,
    A.USER_LOT_ID,
    A.EQP_ID,
    COALESCE(TRIM(BEF_ALIAS.ALIAS_RSN_CD), A.BEF_BAD_RSN_CD) AS BEF_BAD_RSN_CD,
    COALESCE(TRIM(AFT_ALIAS.ALIAS_RSN_CD), A.AFT_BAD_RSN_CD) AS AFT_BAD_RSN_CD,
    A.REJ_GROUP,
    A.OPER1_GROUP,
    A.OPER2_GROUP,
    A.RESPON,
    A.ALLO_GROUP,
    A.RESPON_RATIO,
    SUM(A.IN_QTY) AS IN_QTY,
    SUM(A.OUT_QTY) AS OUT_QTY,
    SUM(A.LOSS_QTY) AS LOSS_QTY,
    A.REAL_DPT_GROUP,
    D.CD_NM AS GRD_CD_NM_CS,
    E.CD_NM AS GRD_CD_NM_PS,
    C.CUST_SITE_NM,
    SUBSTR(B.BESOF_BASE_YW_NM, 3) AS WEEK_DAY_NM,
    MAX(A.DATA_CHG_DTTM) AS DATA_CHG_DTTM
FROM (
    SELECT *
    FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
) A

-- BEF 매핑
LEFT JOIN (
    SELECT
        XX.REJ_RSN_GRP,
        XX.REJ_RSN_CD,
        XX.ALIAS_RSN_CD,
        ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
    FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
    WHERE XX.WAF_SIZE = '{waf_size}'
      AND XX.PROD_DIV_CD = CASE WHEN '{oper_div_l}' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS BEF_ALIAS
ON BEF_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND BEF_ALIAS.REJ_RSN_CD = A.BEF_BAD_RSN_CD
AND BEF_ALIAS.RN = 1

-- AFT 매핑
LEFT JOIN (
    SELECT
        XX.REJ_RSN_GRP,
        XX.REJ_RSN_CD,
        XX.ALIAS_RSN_CD,
        ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
    FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
    WHERE XX.WAF_SIZE = '{waf_size}'
      AND XX.PROD_DIV_CD = CASE WHEN '{oper_div_l}' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS AFT_ALIAS
ON AFT_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND AFT_ALIAS.REJ_RSN_CD = A.AFT_BAD_RSN_CD
AND AFT_ALIAS.RN = 1

JOIN oracle.PMDW_MGR.DW_BA_CM_BASEDATE_M B
ON B.BASE_DT = A.BASE_DT

LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M C
ON C.PROD_ID = A.PROD_ID
AND C.SPEC_DIV_CD = 'PS'

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES D
ON D.UP_CD = 'DMS010'
AND D.SYS_CD = 'DMS'
AND D.CD_VAL = C.GRD_CD_NM

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES E
ON E.UP_CD = 'DMS010'
AND E.SYS_CD = 'DMS'
AND E.CD_VAL = C.GRD_CD_NM_PS

JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M Z1
ON Z1.FAC_ID = A.FAC_ID
AND Z1.OPER_ID = A.OPER_ID

WHERE
    1 = 1
    AND Z1.OPER_DIV_L = '{oper_div_l}'
    AND (CASE WHEN '{grade_filter}' = 'PN' THEN TRUE ELSE {grade_cs_condition} END)
    AND (CASE WHEN '{grade_filter}' = 'PN' THEN TRUE ELSE {grade_ps_condition} END)
    AND Z1.FAC_ID IN ('{fac_ids_str}')
    AND 'N' = 'N'
    AND A.WAF_SIZE = '{waf_size}'
    AND A.BASE_DT = '{target_date}'

GROUP BY
    A.WAF_SIZE, A.BASE_DT, A.DIV_CD, A.REJ_DIV_CD, A.FAC_ID, A.OPER_ID,
    A.OWNR_CD, A.CRET_CD, A.PROD_ID, A.IGOT_ID, A.BLK_ID, A.SUBLOT_ID,
    A.USER_LOT_ID, A.EQP_ID, A.REJ_GROUP, A.OPER1_GROUP, A.OPER2_GROUP,
    A.RESPON, A.ALLO_GROUP, A.RESPON_RATIO, A.REAL_DPT_GROUP,
    D.CD_NM, E.CD_NM, C.CUST_SITE_NM, SUBSTR(B.BESOF_BASE_YW_NM, 3),
    BEF_ALIAS.ALIAS_RSN_CD, AFT_ALIAS.ALIAS_RSN_CD,
    A.BEF_BAD_RSN_CD, A.AFT_BAD_RSN_CD
),
-- (2) Z_WITH_PIMS: Z + PIMS_PROD 조인
Z_WITH_PIMS AS (
    SELECT
        Z.*,
        P.CREQ_T1, P.CREQ_T2, P.CREQ_T3,
        P.CREQ_V1, P.CREQ_V2, P.CREQ_V3
    FROM Z
    LEFT JOIN iceberg.ibg_lake.PIMS_PROD P
    ON P.MS_CODE = Z.PROD_ID
    AND P.SPEC_TYPE = 'CS'
)
-- 최종 SELECT: REJ_GROUP별 집계 + PART_NO 포함
SELECT
    Z.*,
    X1.EQP_NM,
    X.TEAMGRP_NM,
    X.SORT_CD,
    COALESCE(X.TEAMGRP_NM, Z.REAL_DPT_GROUP) AS N_DPT_GROUP,
    --  PART_NO: 일반 CASE 문 (상관 없음)
    CASE
        WHEN STRPOS(UPPER(Z.CREQ_T1), 'PART') > 0 THEN TRIM(SUBSTR(Z.CREQ_V1, STRPOS(Z.CREQ_V1, ':') + 1, 100))
        WHEN STRPOS(UPPER(Z.CREQ_T2), 'PART') > 0 THEN TRIM(SUBSTR(Z.CREQ_V2, STRPOS(Z.CREQ_V2, ':') + 1, 100))
        WHEN STRPOS(UPPER(Z.CREQ_T3), 'PART') > 0 THEN TRIM(SUBSTR(Z.CREQ_V3, STRPOS(Z.CREQ_V3, ':') + 1, 100))
        ELSE ' '
    END AS PART_NO
FROM Z_WITH_PIMS Z
--  Step 5: 팀부서그룹 매핑 (LATERAL)
LEFT JOIN LATERAL (
    SELECT
        S2.TEAMGRP_NM,
        S2.SORT_SEQ AS SORT_CD
    FROM oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRPDTL_M S1
    INNER JOIN oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRP_M S2
    ON S1.TEAMGRP_CD = S2.TEAMGRP_CD
    AND S1.WAF_SIZE = S2.WAF_SIZE
    AND S1.OPER_DIV_L = S2.OPER_DIV_L
    WHERE
        S1.TARGET_DIV_CD IN ('A', 'L')
        AND S1.WAF_SIZE = '{waf_size}'
        AND S1.OPER_DIV_L = '{oper_div_l}'
        AND S1.ED_DT >= '{target_date}'
        AND S1.ST_DT <= '{target_date}'
        AND S1.DPT_CD = Z.REAL_DPT_GROUP
    ORDER BY S1.ST_DT DESC
    LIMIT 1
) X ON TRUE
--  Step 4: EQP_NM 매핑
LEFT JOIN oracle.PMDW_MGR.DW_BA_CM_STDPEQP_H X1
ON X1.FAC_ID = Z.FAC_ID
AND X1.EQP_ID = Z.EQP_ID
AND X1.ST_DT <= Z.BASE_DT
AND (X1.ED_DT >= Z.BASE_DT OR X1.ED_DT IS NULL OR X1.ED_DT = '99991231')
LEFT JOIN oracle.PMDW_MGR.DW_BA_CM_STDPEQP_M X2
ON X2.FAC_ID = X1.FAC_ID
AND X2.EQP_ID = X1.EQP_ID
AND X2.APPLY_YN = 'Y'
"""


# 쿼리 그룹화
QUERIES_BY_CATALOG = {
    'oracle': {
        'DATA_3010_wafering_300' : DATA_3010_wafering_300,
        'DATA_WAF_3210_wafering_300': DATA_WAF_3210_wafering_300,
        'DATA_3210_wafering_300': DATA_3210_wafering_300,
        'DATA_3210_wafering_300_3months' : DATA_3210_wafering_300_3months, #3개월 쿼리 추가
        'DATA_LOT_3210_wafering_300' : DATA_LOT_3210_wafering_300
    }
}
