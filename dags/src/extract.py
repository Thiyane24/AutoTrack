"""
Extracttion part
Flow:
    1. Connect to Gmail via IMAP
    2. Search for internship-related emails by keywords
    3. Parse each email into clean text
    4. Classify as rejected / accepted / unknown
    5. Save raw records to Bronze Parquet layer
"""

import imaplib
import email
from email import policy
import os
import re
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

load_dotenv()  

EMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS")
APP_PASSWORD   = os.getenv("GMAIL_APP_PASSWORD")  

IMAP_SERVER = "imap.gmail.com"
MAILBOX     = "inbox"

# Keywords to search in subject line — widens the net
SEARCH_SUBJECT_KEYWORDS = ["application", "internship", "intern", "position", "opportunity"]

# Keywords to classify the email status
REJECTION_KEYWORDS  = [
    "unfortunately", "regret", "not moving forward",
    "not selected", "other candidates", "not successful",
    "not been shortlisted", "unable to offer", "decided not to proceed"
]

ACCEPTANCE_KEYWORDS = [
    "congratulations", "pleased to inform", "happy to inform",
    "offer", "next steps", "welcome aboard",
    "selected", "moving forward", "interview invitation"
]

# Bronze layer output path
BRONZE_DIR  = Path("bronze")
OUTPUT_FILE = BRONZE_DIR / "internship_emails.parquet"

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def connect_to_gmail() -> imaplib.IMAP4_SSL:
    """Open a secure IMAP connection to Gmail and authenticate."""
    log.info("Connecting to Gmail IMAP server")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ADDRESS, APP_PASSWORD)
    mail.select(MAILBOX)
    log.info("Connected and authenticated.")
    return mail

def get_last_processed_uid() -> int:
    """
    Lê a camada bronze e devolve o UID mais alto já processado.
    Se o ficheiro não existir, devolve 0 (processa tudo).
    """
    if OUTPUT_FILE.exists():
        try:
            # Le apenas a coluna do ID para ser rápido
            df_existing = pd.read_parquet(OUTPUT_FILE, columns=["email_id"])
            # Garante que é um número inteiro para podermos comparar
            max_uid = df_existing["email_id"].astype(int).max()
            log.info(f"Último UID encontrado na camada Bronze: {max_uid}")
            return max_uid
        except Exception as e:
            log.warning(f"Erro ao ler o ficheiro Parquet: {e}. A iniciar do zero.")
            return 0
    return 0

def search_emails(mail: imaplib.IMAP4_SSL, last_uid: int) -> list[bytes]:
    """
    Procura e-mails, mas devolve apenas os UIDs maiores que o last_uid.
    """
    new_uids = set()

    for keyword in SEARCH_SUBJECT_KEYWORDS:
        search_query = f'(SUBJECT "{keyword}")'
        # Usa mail.uid('SEARCH') em vez de mail.search()
        status, messages = mail.uid("SEARCH", None, search_query)

        if status != "OK":
            log.warning(f"Pesquisa falhou para a palavra-chave: {keyword}")
            continue

        # A resposta vem como uma lista de IDs separados por espaço
        uids = messages[0].split()
        
        # Filtro: só UIDs maiores do que o que já temos na nossa base
        for uid_bytes in uids:
            uid_int = int(uid_bytes.decode())
            if uid_int > last_uid:
                new_uids.add(uid_bytes)

        log.info(f"Palavra '{keyword}' -> Encontrados {len(new_uids)} novos e-mails (após filtro).")

    log.info(f"Total de e-mails ÚNICOS e NOVOS a processar: {len(new_uids)}")
    return list(new_uids)


def extract_body(msg: email.message.Message) -> str:
    """
    Extract clean plain text from an email message.
    Handles both plain text and HTML emails.
    Returns an empty string if nothing is found.
    """
    body = ""

    if msg.is_multipart():
        # Walk through all parts of the email
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition   = str(part.get("Content-Disposition", ""))

            # Skip attachments
            if "attachment" in disposition:
                continue

            if content_type == "text/plain":
                # Plain text is ideal — take it and stop
                body = part.get_content()
                break

            elif content_type == "text/html":
                # Fall back to HTML — strip the tags with BeautifulSoup
                html_content = part.get_content()
                soup = BeautifulSoup(html_content, "html.parser")
                body = soup.get_text(separator=" ", strip=True)
                break
    else:
        # Single-part email
        content_type = msg.get_content_type()
        if content_type == "text/plain":
            body = msg.get_content()
        elif content_type == "text/html":
            soup = BeautifulSoup(msg.get_content(), "html.parser")
            body = soup.get_text(separator=" ", strip=True)

    # Remove excessive whitespace
    body = re.sub(r"\s+", " ", body).strip()
    return body


def classify_status(subject: str, body: str) -> str:
    """
    Classify an email as rejected, accepted, or unknown
    based on keywords found in the subject or body.
    """
    text = (subject + " " + body).lower()

    if any(kw in text for kw in REJECTION_KEYWORDS):
        return "rejected"
    elif any(kw in text for kw in ACCEPTANCE_KEYWORDS):
        return "accepted"
    else:
        return "unknown"


def fetch_and_parse_emails(mail: imaplib.IMAP4_SSL, email_ids: list[bytes]) -> list[dict]:
    records = []

    for email_uid in email_ids:
        try:
            
            status, msg_data = mail.uid("FETCH", email_uid, "(RFC822)")

            if status != "OK":
                log.warning(f"Falhou ao extrair o e-mail UID: {email_uid}")
                continue

            # O formato de retorno do UID FETCH é um pouco diferente,
            # a mensagem em bruto está normalmente no primeiro tuplo
            raw_email = msg_data[0][1]

            msg = email.message_from_bytes(raw_email, policy=policy.default)
            
            subject = msg.get("Subject", "").strip()
            sender  = msg.get("From", "").strip()
            date    = msg.get("Date", "").strip()
            body    = extract_body(msg)
            application_status = classify_status(subject, body)

            record = {
                # Guarda o UID num formato numérico (string) para consistência
                "email_id"  : email_uid.decode(), 
                "sender"    : sender,
                "subject"   : subject,
                "date"      : date,
                "body"      : body,
                "status"    : application_status,
                "scraped_at": datetime.utcnow().isoformat()
            }

            records.append(record)
            log.info(f"Extraído: [{application_status.upper()}] {subject[:60]}")

        except Exception as e:
            log.error(f"Erro a processar o e-mail UID {email_uid}: {e}")
            continue

    return records

# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def extract():
    log.info("A iniciar a Pipeline do Gmail (Modo Incremental)")

    # 1. Descobrir onde ficámos da última vez
    last_uid = get_last_processed_uid()

    # 2. Ligar ao Gmail
    mail = connect_to_gmail()

    # 3. Procurar apenas e-mails novos
    new_email_ids = search_emails(mail, last_uid)

    if not new_email_ids:
        log.info("Não há e-mails novos. A pipeline vai terminar.")
        mail.logout()
        return

    # 4. Extrair e classificar os novos e-mails
    new_records = fetch_and_parse_emails(mail, new_email_ids)
    mail.logout()
    log.info("Ligação ao Gmail encerrada.")

    if not new_records:
        return

    # 5. Converter os novos dados para DataFrame
    df_new = pd.DataFrame(new_records)
    log.info(f"\nResumo das novas candidaturas:\n{df_new['status'].value_counts().to_string()}")

    # 6. Juntar com os dados antigos e gravar
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    
    if OUTPUT_FILE.exists():
        # Lemos os antigos, juntamos os novos, e voltamos a gravar
        df_old = pd.read_parquet(OUTPUT_FILE)
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new

    df_final.to_parquet(OUTPUT_FILE, index=False)
    log.info(f"Bronze layer is saver at: {OUTPUT_FILE}")
    log.info(f"Total number of emails: {len(df_final)}")


if __name__ == "__main__":
    extract()