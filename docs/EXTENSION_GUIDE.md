# Daily Report 확장/수정 가이드

## 1) 절대 레이아웃 고정 구역 (기존 엑셀)
아래 파일/함수의 기존 시트(`Prime 분석`) 내 좌표/이미지 크기는 고정 구역입니다.

- `modules/report_generator.py`
  - `_export_to_excel()` 안의 기존 `ws.add_image(..., 'A4')`, `img.width/height`, `current_row` 계산 로직

> 신규 요구사항은 **새 시트 추가** 방식으로 처리하세요.

## 2) 수정 구역 (기능별)

### A. 공정 추가(wafering/epi/growing)
- 공정 등록: `config/process_registry.py`
- 쿼리 구현: `queries/daily_queries.py`
- 실행 공정 선택: `config/database.py`의 `QUERY_CONFIG['enabled_processes']`

### B. 이메일 발송
- SMTP 설정: `config/email.py` (환경변수 기반)
- 발송 로직: `modules/email_sender.py`
- 실행 진입점: `main.py` (`--send-email`)

### C. 전체 불량 시트
- 신규 시트 생성 함수: `modules/report_generator.py::_append_all_defect_sheet()`
- 기존 시트는 유지, 신규 시트(`전체 불량 상세`)만 추가

## 3) 권장 폴더 구조 (공정 확장)

```text
config/
  process_registry.py
queries/
  daily_queries.py                 # 공통 + 공정별 query builder
modules/
  report_generator.py              # 기존 wafering 전용 레이아웃 유지
  email_sender.py
docs/
  EXTENSION_GUIDE.md
```

추후 공정별로 분리하려면:

```text
queries/processes/
  wafering_queries.py
  epi_queries.py
  growing_queries.py
```

이후 `daily_queries.py`에서 통합 export만 담당하도록 권장합니다.

## 4) 메일 본문 전략
- 비권장: 엑셀 전체 캡처 이미지 본문 삽입 (가독성/용량/모바일 대응 불리)
- 권장: 본문은 KPI 요약(HTML 표), 원본 엑셀은 첨부파일

현재 구현은 권장 방식(요약 + 첨부) 기준입니다.
