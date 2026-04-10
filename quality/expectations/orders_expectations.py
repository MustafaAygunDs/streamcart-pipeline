import os
import sys
import json
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import great_expectations as ge

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=5433,
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def run_expectations():
    conn = get_connection()
    results = {}

    # fact_orders kontrolü
    logger.info("fact_orders kontrol ediliyor...")
    df_orders = pd.read_sql("SELECT * FROM fact_orders", conn)
    ge_orders = ge.from_pandas(df_orders)

    checks = [
        ge_orders.expect_column_values_to_not_be_null('order_id'),
        ge_orders.expect_column_values_to_not_be_null('customer_id'),
        ge_orders.expect_column_values_to_be_unique('order_id'),
        ge_orders.expect_column_values_to_be_in_set('order_status', [
            'delivered', 'shipped', 'canceled', 'processing',
            'unavailable', 'invoiced', 'approved', 'created'
        ]),
        ge_orders.expect_table_row_count_to_be_between(90000, 110000),
        ge_orders.expect_column_values_to_not_be_null('order_purchase_timestamp'),
    ]

    results['fact_orders'] = {
        'total_checks': len(checks),
        'passed': sum(1 for c in checks if c['success']),
        'failed': sum(1 for c in checks if not c['success']),
        'details': [
            {
                'expectation': c['expectation_config']['expectation_type'],
                'column': c['expectation_config']['kwargs'].get('column', 'table'),
                'success': c['success']
            }
            for c in checks
        ]
    }

    # dim_customer kontrolü
    logger.info("dim_customer kontrol ediliyor...")
    df_customers = pd.read_sql("SELECT * FROM dim_customer", conn)
    ge_customers = ge.from_pandas(df_customers)

    cust_checks = [
        ge_customers.expect_column_values_to_not_be_null('customer_id'),
        ge_customers.expect_column_values_to_be_unique('customer_id'),
        ge_customers.expect_column_values_to_not_be_null('customer_state'),
        ge_customers.expect_table_row_count_to_be_between(90000, 110000),
    ]

    results['dim_customer'] = {
        'total_checks': len(cust_checks),
        'passed': sum(1 for c in cust_checks if c['success']),
        'failed': sum(1 for c in cust_checks if not c['success']),
    }

    conn.close()

    # Rapor
    print("\n" + "="*60)
    print("VERİ KALİTESİ RAPORU")
    print("="*60)
    total_passed = 0
    total_failed = 0
    for table, result in results.items():
        status = "✅ PASSED" if result['failed'] == 0 else "❌ FAILED"
        print(f"\n{table}: {status}")
        print(f"  Toplam kontrol: {result['total_checks']}")
        print(f"  Geçen: {result['passed']}")
        print(f"  Başarısız: {result['failed']}")
        if 'details' in result:
            for d in result['details']:
                icon = "✅" if d['success'] else "❌"
                print(f"    {icon} {d['expectation']} → {d['column']}")
        total_passed += result['passed']
        total_failed += result['failed']

    print(f"\n{'='*60}")
    print(f"TOPLAM: {total_passed} geçti, {total_failed} başarısız")
    overall = "✅ TÜM KONTROLLER BAŞARILI" if total_failed == 0 else "❌ BAZI KONTROLLER BAŞARISIZ"
    print(overall)
    print("="*60)

    return total_failed == 0

if __name__ == '__main__':
    success = run_expectations()
    sys.exit(0 if success else 1)
