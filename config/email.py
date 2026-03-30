import os

EMAIL_CONFIG = {
    "smtp_host": os.getenv("DAILY_SMTP_HOST", ""),
    "smtp_port": int(os.getenv("DAILY_SMTP_PORT", "587")),
    "use_tls": os.getenv("DAILY_SMTP_TLS", "true").lower() == "true",
    "username": os.getenv("DAILY_SMTP_USER", ""),
    "password": os.getenv("DAILY_SMTP_PASS", ""),
    "sender": os.getenv("DAILY_MAIL_SENDER", ""),
    "recipients": [x.strip() for x in os.getenv("DAILY_MAIL_RECIPIENTS", "").split(",") if x.strip()],
}
