"""Operador personalizado: envía un reporte por email al terminar el pipeline."""
from airflow.models.baseoperator import BaseOperator

from alerting import _card, send_email


class EmailReportOperator(BaseOperator):
    """
    Envía un resumen de la corrida. `subject` y `summary` aceptan Jinja,
    por ejemplo {{ ds }} o {{ ti.xcom_pull(task_ids='load') }}.
    """

    template_fields = ("subject", "summary")
    ui_color = "#ffe9a8"

    def __init__(self, subject: str, summary: dict, **kwargs):
        super().__init__(**kwargs)
        self.subject = subject
        self.summary = summary

    def execute(self, context):
        html = _card("✅ Pipeline de SpaceX completado", "#1e8449", self.summary)
        sent = send_email(self.subject, html)
        return {"sent": sent, **self.summary}
