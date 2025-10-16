FROM node:18-alpine

# Install Playwright dependencies and fonts
RUN apk add --no-cache \
    chromium \
    nss \
    freetype \
    freetype-dev \
    harfbuzz \
    ca-certificates \
    ttf-freefont \
    font-noto-emoji \
    python3 \
    make \
    g++ \
    && rm -rf /var/cache/apk/*

# Set environment variables for Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies and Playwright browsers
RUN npm ci --only=production \
    && npx playwright install firefox \
    && npx playwright install chromium

# Copy source code
COPY . .

# Create necessary directories
RUN mkdir -p exports data

# Create non-root user and set permissions
RUN addgroup -g 1001 -S nodejs \
    && adduser -S scraper -u 1001 \
    && chown -R scraper:nodejs /app

USER scraper

# Health check (optional)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD node -e "require('fs').access('/app/package.json', (err) => process.exit(err ? 1 : 0))"

CMD ["node", "scrape_detail.js"]
