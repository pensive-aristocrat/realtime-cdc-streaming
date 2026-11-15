"""Pretend to be the shop's application: insert, update and delete rows in PostgreSQL.

It knows nothing about Kafka. Debezium picks up every change from the database log.
Run:  python app/shop_simulator.py            (30 changes, one a second)
      python app/shop_simulator.py --n 0     (until Ctrl+C)
"""
import argparse
import random
import time

import psycopg

DSN = "host=localhost port=5434 dbname=shop user=shop password=shop"
NEXT_STATUS = {"created": "paid", "paid": "shipped", "shipped": "delivered"}
CITIES = ["Dhaka", "Chattogram", "Sylhet", "Khulna", "Rajshahi", "Barishal"]


def new_order(cur) -> str:
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    customer_id = cur.fetchone()[0]
    amount = round(random.uniform(100, 5000), 2)
    cur.execute("INSERT INTO orders (customer_id, amount) VALUES (%s, %s) RETURNING order_id",
                (customer_id, amount))
    return f"INSERT order {cur.fetchone()[0]} for customer {customer_id}: {amount}"


def advance_order(cur) -> str:
    cur.execute("SELECT order_id, status FROM orders WHERE status IN ('created', 'paid', 'shipped') "
                "ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        return new_order(cur)
    order_id, status = row
    cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
                (NEXT_STATUS[status], order_id))
    return f"UPDATE order {order_id}: {status} -> {NEXT_STATUS[status]}"


def cancel_and_delete(cur) -> str:
    cur.execute("SELECT order_id FROM orders WHERE status = 'created' ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        return new_order(cur)
    cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))
    return f"DELETE order {row[0]} (cancelled before payment)"


def customer_moves(cur) -> str:
    cur.execute("SELECT customer_id, city FROM customers ORDER BY random() LIMIT 1")
    customer_id, old_city = cur.fetchone()
    new_city = random.choice([c for c in CITIES if c != old_city])
    cur.execute("UPDATE customers SET city = %s WHERE customer_id = %s", (new_city, customer_id))
    return f"UPDATE customer {customer_id}: {old_city} -> {new_city}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=30, help="number of changes (0 = never stop)")
    args = parser.parse_args()

    actions = [new_order, advance_order, cancel_and_delete, customer_moves]
    weights = [45, 40, 8, 7]
    done = 0
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        try:
            while args.n == 0 or done < args.n:
                action = random.choices(actions, weights)[0]
                print(action(cur))
                done += 1
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    print(f"done: {done} changes")


if __name__ == "__main__":
    main()