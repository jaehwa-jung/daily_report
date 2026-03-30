"""Backward-compatible mapping exports.

기존 코드에서 `config.mappings`를 import하고 있어도 동작하도록
`config.mapping`의 상수를 그대로 재노출한다.
"""

from .mapping import MID_TO_EQP, NAME_TO_EQP, REJ_GROUP_TO_MID_MAPPING

__all__ = [
    "REJ_GROUP_TO_MID_MAPPING",
    "NAME_TO_EQP",
    "MID_TO_EQP",
]
