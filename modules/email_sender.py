import os
import smtplib
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Optional


class EmailSender:
    def __init__(self, config: Dict):
        self.config = config

    def _build_summary_html(self, report: Dict, target_date: str) -> str:
        """엑셀 원본을 그대로 붙이지 않고, 핵심 수치만 본문 요약."""
        overall = report.get("overall_stats", {}) or {}
        total_gap = overall.get("total_volume_defect_change", 0.0)
        ref_rate = overall.get("overall_ref_loss_rate", 0.0)
        daily_rate = overall.get("overall_daily_loss_rate", 0.0)

        top_mix = report.get("total_loss_gap")
        rows_html = ""
        if top_mix is not None and not getattr(top_mix, "empty", True):
            for _, row in top_mix.head(5).iterrows():
                rows_html += (
                    f"<tr><td>{row.get('PRODUCT_TYPE','-')}</td>"
                    f"<td>{row.get('Ref_전체_불량률(%)',0):.2f}%</td>"
                    f"<td>{row.get('Daily_전체_불량률(%)',0):.2f}%</td>"
                    f"<td>{row.get('제품 Mix비 변동',0):.4f}</td></tr>"
                )

        return f"""
        <h3>[Daily Report] {target_date}</h3>
        <p>전체 불량률(Ref): <b>{ref_rate:.2f}%</b><br>
           전체 불량률(Daily): <b>{daily_rate:.2f}%</b><br>
           제품 Mix비 변동 합계: <b>{total_gap:.4f}</b></p>
        <p>※ 권장: 엑셀 전체 캡처 이미지 대신, 아래 요약 + 원본 엑셀 첨부 조합을 사용하세요.</p>
        <table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;">
          <tr><th>제품</th><th>Ref 불량률</th><th>Daily 불량률</th><th>Mix비 변동</th></tr>
          {rows_html or '<tr><td colspan="4">요약 데이터 없음</td></tr>'}
        </table>
        """

    def send_daily_report(self, report: Dict, target_date: str, excel_path: Optional[str] = None):
        if not self.config.get("smtp_host"):
            raise ValueError("EMAIL_CONFIG.smtp_host가 비어 있습니다.")
        if not self.config.get("sender"):
            raise ValueError("EMAIL_CONFIG.sender가 비어 있습니다.")
        recipients = self.config.get("recipients") or []
        if not recipients:
            raise ValueError("EMAIL_CONFIG.recipients가 비어 있습니다.")

        msg = MIMEMultipart()
        msg["Subject"] = f"[Daily Report] {target_date}"
        msg["From"] = self.config["sender"]
        msg["To"] = ", ".join(recipients)

        html_body = self._build_summary_html(report, target_date)
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        if excel_path and os.path.exists(excel_path):
            with open(excel_path, "rb") as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(excel_path))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(excel_path)}"'
            msg.attach(part)

        with smtplib.SMTP(self.config["smtp_host"], self.config.get("smtp_port", 587)) as smtp:
            if self.config.get("use_tls", True):
                smtp.starttls()
            if self.config.get("username"):
                smtp.login(self.config["username"], self.config.get("password", ""))
            smtp.sendmail(self.config["sender"], recipients, msg.as_string())


def should_send_now(now: Optional[datetime] = None, hhmm: str = "08:30") -> bool:
    now = now or datetime.now()
    target_h, target_m = [int(x) for x in hhmm.split(":")]
    return now.hour == target_h and now.minute == target_m
