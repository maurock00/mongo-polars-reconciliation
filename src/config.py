import uuid
import datetime
import json
import time

# Constants
CONCILIATION_STATUS = "CONCILIATED"
NON_CONCILIATION_STATUS = "REMANENT"
EXTERNAL_SOURCE_NAME = "odl_2024-05-01_30_mins.csv"
RC_KEYS_JSON = [
    # {"ext_col": "codigo_aprobacion", "int_col": "approval_code"},
    # {"ext_col": "tipo_de_transaccion", "int_col": "transaction_type"},
    # {"ext_col": "estado_transaccion", "int_col": "transaction_status_type"},
    {"ext_col": "codigo_ksh", "int_col": "transaction_code"},
    {"ext_col": "importe", "int_col": "approved_transaction_amount"},
    {"ext_col": "fecha", "int_col": "create_timestamp"},
    {"ext_col": "digitos_bin", "int_col": "bin_code"},
    {"ext_col": "kind_card", "int_col": "card_type"},
    {"ext_col": "ultimos4", "int_col": "last_four_digit_code"},
]
RC_KEYS = json.dumps(RC_KEYS_JSON)
DEFAULT_INTERNAL_FIELDS = [
    "_id",
    "reference_transaction_code",
    "approval_code",
    "processor_type",
    "merchant_name",
    "processor_name",
    "transaction_code",
    "transaction_status_type",
    "transaction_type",
    "ticket_code",
    "sale_ticket_code",
]
INTEGER_FIELDS = ["create_timestamp"]
DOUBLE_FIELDS = ["approved_transaction_amount"]

# Filters for query internal data
TIMESTAMP_FROM = 1714521600000  # 2024-05-01 00:00:00
TIMESTAMP_TO = 1714607999000  # 2024-05-01 23:59:59
PROCESSOR_NAME = "Kushki Acquirer Processor"
COUNTRY_NAME = "Mexico"
TRANSACTION_STATUSES = ["APPROVED"]

# Calculated constants
CONCILIATION_TIMESTAMP = 1712188800000
CONCILIATION_DATE = "2024-01-02"
EXECUTION_TYPE = "AUTOMATIC"
EXECUTION_ID = str(uuid.uuid4())
EXECUTION_DATE = str(datetime.date.today())
EXECUTION_TIMESTAMP = int(time.mktime(datetime.datetime.now().timetuple()) * 1000)

# DB and collections
ODL_DB = "reconciliation_test"
ODL_COLL = "card_transaction"
EXTERNAL_COLL = "external_transaction"
RESULT_COLL = "reconciliation_transactions"
AGG_RESULT_COLL = "reconciliation_summary"
EXT_REMANENT_RECORDS = "external_remanent_records"
TMP_COLL = "tmp_rc_transactions"
