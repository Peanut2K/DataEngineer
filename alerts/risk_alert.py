"""
alerts/risk_alert.py
─────────────────────
Read the latest Gold fact records from MongoDB and send an email alert
when any province's risk_score exceeds RISK_THRESHOLD (default: 50).

Called as an Airflow task at the end of both pipeline DAGs.
"""

import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from pymongo import MongoClient, DESCENDING

from config.settings import GOLD_COLLECTIONS, MONGODB_DATABASE, MONGODB_URI
from utils.logger import get_logger

logger = get_logger("risk_alert")

# ── Configuration (overridable via environment variables) ─────────────────────
RISK_THRESHOLD = float(os.getenv("ALERT_RISK_THRESHOLD", "50"))
SMTP_HOST      = os.getenv("ALERT_SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("ALERT_SMTP_PORT", "587"))
SMTP_USER      = os.getenv("ALERT_EMAIL_USER",     "")
SMTP_PASSWORD  = os.getenv("ALERT_EMAIL_PASSWORD", "")
ALERT_TO       = os.getenv("ALERT_EMAIL_TO",       "")


# ─────────────────────────────────────────────────────────────────────────────
# Email builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_email(high_risk: list) -> MIMEMultipart:
    """Compose a structured HTML alert email."""
    subject = (
        f"⚠️ Environmental Risk Alert — "
        f"{len(high_risk)} province(s) above threshold ({int(RISK_THRESHOLD)})"
    )

    # Build HTML rows
    rows = ""
    for rec in sorted(high_risk, key=lambda r: r["risk_score"], reverse=True):
        level   = rec.get("risk_level", "—")
        score   = rec.get("risk_score", 0)
        pm25    = rec.get("pm25",        "N/A")
        temp    = rec.get("temperature", "N/A")
        flood   = rec.get("flood_risk",  "N/A")
        date    = rec.get("date",        "—")

        # Color by level
        color = {
            "Critical": "#d32f2f",
            "High":     "#f57c00",
            "Moderate": "#fbc02d",
            "Low":      "#388e3c",
        }.get(level, "#000")

        rows += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;">{rec['province']}</td>
          <td style="padding:8px;border:1px solid #ddd;">{date}</td>
          <td style="padding:8px;border:1px solid #ddd;font-weight:bold;color:{color};">{score}</td>
          <td style="padding:8px;border:1px solid #ddd;color:{color};">{level}</td>
          <td style="padding:8px;border:1px solid #ddd;">{pm25 if pm25 == 'N/A' else f'{pm25:.1f} µg/m³'}</td>
          <td style="padding:8px;border:1px solid #ddd;">{temp if temp == 'N/A' else f'{temp:.1f} °C'}</td>
          <td style="padding:8px;border:1px solid #ddd;">{flood}</td>
        </tr>"""

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""
    <html><body style="font-family:sans-serif;color:#333;">
      <h2 style="color:#c62828;">⚠️ Environmental Risk Alert</h2>
      <p>The following province(s) have a <strong>Risk Score &gt; {int(RISK_THRESHOLD)}</strong>
         as of <em>{now_str}</em>:</p>
      <table style="border-collapse:collapse;width:100%;">
        <thead style="background:#f5f5f5;">
          <tr>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Province</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Date</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Risk Score</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Level</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">PM2.5</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Temperature</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Flood Risk</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <p style="margin-top:16px;font-size:12px;color:#888;">
        Risk Score formula: PM2.5 × 0.5 + Temp × 0.2 + Flood × 0.3 (scale 0–100)<br>
        Sent by Environmental Data Pipeline · Airflow
      </p>
    </body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_TO
    msg.attach(MIMEText(html, "html", "utf-8"))
    return msg


def _send_email(msg: MIMEMultipart) -> None:
    """Send email via Gmail SMTP with STARTTLS."""
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, ALERT_TO, msg.as_string())
    logger.info(f"Alert email sent to {ALERT_TO}")


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point (called by Airflow task)
# ─────────────────────────────────────────────────────────────────────────────

def check_and_alert() -> None:
    """
    1. Connect to MongoDB Gold layer.
    2. Get the latest fact_environmental record per province.
    3. If any province has risk_score > RISK_THRESHOLD → send email alert.
    4. If all provinces are below threshold → log OK, no email sent.
    """
    if not SMTP_USER or not SMTP_PASSWORD or not ALERT_TO:
        logger.warning("Email credentials not configured — skipping alert check.")
        return

    client = MongoClient(MONGODB_URI)
    try:
        db   = client[MONGODB_DATABASE]
        coll = db[GOLD_COLLECTIONS["fact"]]

        # Get latest record per province using aggregation
        pipeline = [
            {"$sort": {"_ingested_at": DESCENDING}},
            {
                "$group": {
                    "_id":          "$province",
                    "province":     {"$first": "$province"},
                    "date":         {"$first": "$date"},
                    "risk_score":   {"$first": "$risk_score"},
                    "risk_level":   {"$first": "$risk_level"},
                    "pm25":         {"$first": "$pm25"},
                    "temperature":  {"$first": "$temperature"},
                    "flood_risk":   {"$first": "$flood_risk"},
                }
            },
        ]
        latest = list(coll.aggregate(pipeline))

        if not latest:
            logger.info("No Gold records found — skipping alert check.")
            return

        high_risk = [r for r in latest if (r.get("risk_score") or 0) > RISK_THRESHOLD]

        # Log summary for every province
        for rec in latest:
            score = rec.get("risk_score", 0)
            flag  = " ← ALERT" if score > RISK_THRESHOLD else ""
            logger.info(
                f"{rec['province']}: risk_score={score} "
                f"(level={rec.get('risk_level','?')}){flag}"
            )

        if not high_risk:
            logger.info(
                f"All provinces within safe range "
                f"(threshold={int(RISK_THRESHOLD)}). No alert sent."
            )
            return

        logger.warning(
            f"{len(high_risk)} province(s) exceed threshold {int(RISK_THRESHOLD)}: "
            f"{[r['province'] for r in high_risk]}"
        )

        msg = _build_email(high_risk)
        _send_email(msg)

    finally:
        client.close()
