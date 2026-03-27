
# 중분류(MID_GROUP) 매핑 정의 — DMS 기준 정합성 유지
REJ_GROUP_TO_MID_MAPPING = {
    'BROKEN' : {
        'BK_LAP': 'LAP', 'BK_LAP_LD': 'LAP', 'BK_LAP_UDD': 'LAP', 'BK_LAP_UDN': 'LAP',
        'BK_LAP_ULD': 'LAP', 'BK_LAP_WIP': 'LAP', 'BK_LEC': 'LAP', 'BK_LEC_LD': 'LAP', 'BK_LEC_ULD': 'LAP',
        'BK_EP': 'EP', 'EQP_EP': 'EP', 'MAINT_EP': 'EP',
        'BK_DSP_ATT': 'DSP', 'BK_DSP_ROT': 'DSP', 'BK_DSP_SSS': 'DSP', 'BK_DSP_WIP': 'DSP',
        'BK_DSPATL': 'DSP', 'EQP_DSPATL': 'DSP',
        'BK_FP': 'FP', 'BK_FP_WIPE': 'FP', 'EQP_FP': 'FP', 'EQP_FP_LD': 'FP',
        'EQP_FP_POL': 'FP', 'EQP_FP_ULD': 'FP'
    },

    #  Flatness 
    'FLATNESS' : {
        'TTV' : 'GBIR', 'STIR' : 'SFQR'
    },

    'GR_보증' : {
        'ZZZZ' : 'Lot보증'
    },

    'SCRATCH' : {
        'F_SCRATCH' : 'Front Side', 'F_DSCRATCH' : 'Front Side', 'FMSCR' : 'Front Side',
        'B_SCRATCH' : 'Back Side', 'B_DSCRATCH' : 'Back Side', 'BMSCR' : 'Back Side', 'BNG' : 'Back Side'
    },

    'SAMPLE' : {
        'ENGSFT' : 'Engr Sample', 'ENGSCT' : 'Engr Sample', 'ENGSIS' : 'Engr Sample',
        'LOT_SMPL' : 'Lot Sample', 'LOT-SAMPLE' : 'Lot Sample',  'MON_SAMPLE' : 'Monitoring Sample',
        'SMPL' : 'Growing Engr Sample'
    }

}

#    - 분석 그룹이 장비 기반 분석 가능한 경우에만 정의
NAME_TO_EQP = {
    'EQP_NM_300_WF_3300' : 'ASC',
    'EQP_NM_300_WF_3335' : 'EG1차',
    'EQP_NM_300_WF_3670' : 'LAP',
    'EQP_NM_300_WF_3696' : 'EG2차',
    'EQP_NM_300_WF_6100' : 'DSP',
    'EQP_NM_300_WF_6210' : 'EP',
    'EQP_NM_300_WF_6500' : 'FP',
    'EQP_NM_300_WF_7000' : 'EBIS'
}


MID_TO_EQP = {
    'ASC' : 'EQP_NM_300_WF_3300',
    'EG1차' : 'EQP_NM_300_WF_3335',
    'LAP' : 'EQP_NM_300_WF_3670',
    'EG2차' : 'EQP_NM_300_WF_3696',
    'DSP' : 'EQP_NM_300_WF_6100',
    'EP' : 'EQP_NM_300_WF_6210',
    'FP' : 'EQP_NM_300_WF_6500',
    'EBIS' : 'EQP_NM_300_WF_7000' 
}
