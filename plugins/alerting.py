"""
Alertas por email vía SMTP (ej. Gmail con contraseña de aplicación).

Variables de entorno:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO (coma-separado)

Si no están configuradas, el email no se envía y se registra en el log
("modo dry-run"), así el pipeline corre igual en desarrollo.
"""
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

log = logging.getLogger(__name__)


def smtp_configured() -> bool:
    return all(os.getenv(k) for k in ("SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "ALERT_EMAIL_TO"))


def send_email(subject: str, html: str) -> bool:
    if not smtp_configured():
        log.warning("SMTP no configurado: dry-run. Asunto: %s", subject)
        log.info(html)
        return False

    to = [a.strip() for a in os.environ["ALERT_EMAIL_TO"].split(",") if a.strip()]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = ", ".join(to)
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.getenv("SMTP_PORT", "587")), timeout=30) as s:
        s.starttls()
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_PASSWORD"])
        s.sendmail(msg["From"], to, msg.as_string())
    log.info("Email enviado a %s: %s", to, subject)
    return True


def _card(title: str, color: str, rows: dict) -> str:
    trs = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:#666'>{k}</td><td><b>{v}</b></td></tr>"
        for k, v in rows.items()
    )
    return (
        f"<div style='font-family:Arial,sans-serif;max-width:560px'>"
        f"<h2 style='color:{color};margin-bottom:8px'>{title}</h2>"
        f"<table style='font-size:14px'>{trs}</table></div>"
    )


def on_failure_alert(context):
    """Callback de Airflow: se dispara cuando una tarea falla (después de los reintentos)."""
    ti = context["task_instance"]
    exc = context.get("exception")
    html = _card("❌ Falló el pipeline de SpaceX", "#c0392b", {
        "DAG": ti.dag_id,
        "Tarea": ti.task_id,
        "Fecha lógica": context.get("ds"),
        "Intento": ti.try_number,
        "Error": str(exc)[:500] if exc else "-",
        "Log": ti.log_url,
    })
    send_email(f"[ALERTA] {ti.dag_id}.{ti.task_id} falló ({context.get('ds')})", html)


def on_retry_alert(context):
    ti = context["task_instance"]
    log.warning("Reintentando %s.%s (intento %s)", ti.dag_id, ti.task_id, ti.try_number)
