.PHONY: up down restart etl test logs help

help:
	@echo "StreamCart Pipeline - Komutlar:"
	@echo "  make up       - Tüm servisleri başlat"
	@echo "  make down     - Tüm servisleri durdur"
	@echo "  make restart  - Tüm servisleri yeniden başlat"
	@echo "  make etl      - Pipeline'ı manuel çalıştır (Silver > Quality > Gold)"
	@echo "  make test     - Veri kalitesi kontrollerini çalıştır"
	@echo "  make logs     - Tüm servis loglarını göster"
	@echo "  make status   - Servis durumlarını göster"

up:
	docker compose up -d
	@echo "Servisler başlatıldı. Airflow UI: http://localhost:8081"

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d

status:
	docker compose ps

logs:
	docker compose logs --tail=50

etl:
	@echo "Silver transformation çalışıyor..."
	python3 processing/silver/orders_silver.py
	@echo "Gold transformation çalışıyor..."
	python3 processing/gold/orders_gold.py
	@echo "Pipeline tamamlandı."

test:
	@echo "Veri kalitesi kontrolleri çalışıyor..."
	python3 quality/expectations/orders_expectations.py
	@echo "Kontroller tamamlandı."

producer:
	@echo "Kafka producer başlatılıyor..."
	python3 ingestion/producers/orders_producer.py

bronze:
	@echo "Bronze streaming job başlatılıyor..."
	python3 processing/bronze/orders_bronze.py
