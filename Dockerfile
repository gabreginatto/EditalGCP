# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile for Tender-Tracker (no Node.js)
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

# Force unbuffered stdout/stderr (good for logging)
ENV PYTHONUNBUFFERED=1

# Install only the system libraries required by Playwright browsers
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      fonts-liberation \
      libappindicator3-1 \
      libasound2 \
      libatk-bridge2.0-0 \
      libatk1.0-0 \
      libcairo2 \
      libcups2 \
      libdbus-1-3 \
      libdrm2 \
      libgbm1 \
      libglib2.0-0 \
      libgtk-3-0 \
      libnspr4 \
      libnss3 \
      libx11-6 \
      libx11-xcb1 \
      libxcb1 \
      libxcomposite1 \
      libxdamage1 \
      libxext6 \
      libxfixes3 \
      libxrandr2 \
      libxrender1 \
      libxss1 \
      libxtst6 \
      lsb-release \
      wget \
      xdg-utils && \
    rm -rf /var/lib/apt/lists/*

# Set your working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Download Playwright browser binaries (Chromium, Firefox, WebKit)
RUN playwright install --with-deps

# Expose the port your Flask/Gunicorn app listens on
EXPOSE 8080

# Launch the Flask app via Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app", "--timeout", "900"]
