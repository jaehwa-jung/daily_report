
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
