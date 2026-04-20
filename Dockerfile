FROM python:3.12-slim AS base

# Tesseract + German lang pack for OCR. apt cache is cleaned in the same RUN
# layer so it doesn't bloat the image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        tesseract-ocr-deu \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Non-root runtime user (uid matches the HelmRelease securityContext).
RUN groupadd -g 1000 app && useradd -u 1000 -g 1000 -m -s /usr/sbin/nologin app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./

USER 1000:1000

# /tmp must be writable for Tesseract scratch — provided by an emptyDir mount
# in production; the in-container path is the default tmpfs in dev.

EXPOSE 8080

CMD ["python", "main.py", "run"]
