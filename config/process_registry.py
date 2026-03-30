"""공정별 확장 설정.

현재는 wafering만 실제 쿼리 함수가 구현되어 있으며,
추후 epi/growing을 동일 포맷으로 쉽게 추가할 수 있도록
등록 구조를 분리했다.
"""

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class ProcessSpec:
    code: str
    display_name: str
    data_prefix: str


PROCESS_SPECS: Dict[str, ProcessSpec] = {
    "wafering": ProcessSpec(code="wafering", display_name="Wafering", data_prefix="wafering"),
    # 아래 2개는 추후 쿼리 구현 시 활성화
    "epi": ProcessSpec(code="epi", display_name="EPI", data_prefix="epi"),
    "growing": ProcessSpec(code="growing", display_name="Growing", data_prefix="growing"),
}


def get_enabled_processes(config: dict) -> List[ProcessSpec]:
    """QUERY_CONFIG 기반으로 활성 공정 리스트 반환."""
    requested = config.get("enabled_processes", ["wafering"])
    return [PROCESS_SPECS[p] for p in requested if p in PROCESS_SPECS]
