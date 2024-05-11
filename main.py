from faker import Faker
import psycopg2
from datetime import datetime
import random
import uuid

fake = Faker()


def generate_transaction():
    user = fake.simple_profile()

    return {
        "transactionid": fake.uuid4(),
        "userid": user['username'],
        "timestamp": datetime.utcnow().timestamp(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['usd', 'GBP']),
        "city": fake.city(),
        "country": fake.country(),
        "merchantname": fake.company(),
        "paymentmethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "ipaddress": fake.ipv4(),
        "voucherCode": random.choice(['', 'DISCOUNT10', '']),
        "affiliateid": fake.uuid4()
    }


def create_table(conn):
    cursor = conn.cursor()

    cursor.execute("""
        create table if not exists transaction(
            transaction_id varchar(255) PRIMARY KEY,
            user_id varchar(255),
            timestamp TIMESTAMP,
            amount decimal,
            currency varchar(255),
            city varchar(255),
            country varchar(255),
            merchant_name varchar(255),
            payment_method varchar(255),
            ip_address varchar(255),
            voucher_code varchar(255),
            affiliateid varchar(255)
        )
    """)

    cursor.close()
    conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5433
    )

    create_table(conn)
    transaction = generate_transaction()
    cur = conn.cursor()
    print(transaction)



# Convert timestamp before insertion
  
timestamp_str = datetime.fromtimestamp(transaction["timestamp"]).strftime("%Y-%m-%d")
cur.execute("""
    INSERT INTO transaction(transaction_id,
                            user_id, timestamp, amount, currency, city, country, merchant_name,
                            payment_method, ip_address, affiliateid,voucher_code)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
  """, 
  (transaction["transactionid"], transaction["userid"], timestamp_str,
        transaction["amount"], transaction["currency"], transaction["city"],
        transaction["country"],transaction["merchantname"], transaction["paymentmethod"], 
        transaction["ipaddress"], transaction["affiliateid"],transaction["voucherCode"])
        )

cur.close()
conn.commit()