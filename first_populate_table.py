import pymongo
import os

SOURCE_DB = "reconciliation_test"
SOURCE_COLL = "card_transaction"
DESTINATION_DB = "reconciliation_test"
DESTINATION_COLL = "reconciliation_transactions"
TIMESTAMP_FROM = 1712102400000  # 2024-01-03 00:00:00
TIMESTAMP_TO = 1712188800000  # 2024-01-04 00:00:00
PROCESSOR_NAME = "Kushki Acquirer Processor"

client = pymongo.MongoClient(os.getenv("MONGO_URI"))

def divide_timestamps_into_intervals(n):
    interval = (TIMESTAMP_TO - TIMESTAMP_FROM) / n
    intervals = []
    start = TIMESTAMP_FROM
    for i in range(n):
        end = start + interval
        intervals.append((start, end))
        start = end
    return intervals

def load_new_data():
    card_trx_coll = client[SOURCE_DB][SOURCE_COLL]
    card_trx_coll.aggregate(
        [
            {
                "$match": {
                    "processor_name": PROCESSOR_NAME,
                    "transaction_status_type": {"$in": ["APPROVED"]},
                    "create_timestamp": {
                        "$gte": TIMESTAMP_FROM,
                        "$lt": TIMESTAMP_TO,
                    }
                },
            },
            {
                "$project": {
                    "_id": 1,
                    "conciliation_status": "PENDING",
                    "conciliation_process": "BATCH_LOAD",
                    "transaction_create_timestamp": "$create_timestamp",
                    "processor_name": 1,
                    "transaction_status_type": 1
                },
            },
            {
                "$merge": {
                    "into": {"db": DESTINATION_DB, "coll": DESTINATION_COLL},
                    "on": "_id",
                    "whenMatched": "keepExisting",
                    "whenNotMatched": "insert",
                }
            },
        ]
    )

if __name__ == "__main__":
    load_new_data()