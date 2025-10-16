start-db:
	docker compose up -d mongodb

run-therapyfinder-scraper:
	docker compose run --rm therapyfinder_scraper python scrape.py

run-therapyfinder-exporter:
	docker compose run --rm therapyfinder_scraper python export_to_excel.py

run-headway-scraper:
	docker compose run --rm headway_scraper python scrape.py

run-headway-exporter:
	docker compose run --rm headway_scraper python export_to_excel.py

run-headway-installer:
	docker compose run --rm headway_scraper playwright install --with-deps chromium

run-rula-basic-scraper:
	docker compose run --rm rula_scraper node scrape_basic.js

run-rula-detail-scraper:
	docker compose run --rm rula_scraper node scrape_detail.js

run-rula-exporter:
	docker compose run --rm rula_scraper node export_to_excel.js

run-rula-installer:
	docker compose run --rm rula_scraper npx playwright install firefox

stop:
	docker compose down
