FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    wget \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxss1 \
    libasound2 \
    libx11-xcb1 \
    libgbm1 \
    libxrandr2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 scraper
USER scraper

# Install Playwright browsers as non-root user
RUN python -m playwright install chromium

CMD ["python", "scrape.py"]
