import pymongo
from pymongoarrow.monkey import patch_all
import polars as pl
from pymongoarrow.api import write, Schema
from pyarrow import float64, int64
import datetime
import time
import json
import uuid
import concurrent.futures
import os

patch_all()
pl.Config.set_fmt_float("full")

# Constants
CONCILIATION_STATUS = "CONCILIATED"
NON_CONCILIATION_STATUS = "REMANENT"
EXTERNAL_SOURCE_NAME = "EXTERNAL_ARCHIVE.csv"
RC_KEYS_JSON = [
    # {"ext_col": "codigo_aprobacion", "int_col": "approval_code"},
    # {"ext_col": "tipo_de_transaccion", "int_col": "transaction_type"},
    # {"ext_col": "estado_transaccion", "int_col": "transaction_status_type"},
    {"ext_col": "codigo_ksh", "int_col": "transaction_code"},
    {"ext_col": "importe", "int_col": "approved_transaction_amount"},
    {"ext_col": "fecha", "int_col": "create_timestamp"},
    {"ext_col": "digitos_bin", "int_col": "tmp1_bin_code"},
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
]
INTEGER_FIELDS = ["create_timestamp"]
DOUBLE_FIELDS = ["approved_transaction_amount"]

# Calculated constants
CONCILIATION_TIMESTAMP = 1712188800000
CONCILIATION_DATE = "2024-01-02"
EXECUTION_TYPE = "AUTOMATIC"
EXECUTION_ID = str(uuid.uuid4())
EXECUTION_DATE = str(datetime.date.today())
EXECUTION_TIMESTAMP = int(time.mktime(datetime.datetime.now().timetuple()) * 1000)

# Filters for query internal data
TIMESTAMP_FROM = 1711929600000  # 2024-04-01 00:00:00
TIMESTAMP_TO = 1712188800000  # 2024-04-04 00:00:00
PROCESSOR_NAME = "Kushki Acquirer Processor"

# DB and collections
ODL_DB = "reconciliation_test"
ODL_COLL = "card_transaction"
EXTERNAL_COLL = "external_transaction"
RESULT_COLL = "reconciliation_transactions"
AGG_RESULT_COLL = "reconciliation_summary"
EXT_REMANENT_RECORDS = "external_remanent_records"
TMP_COLL = "tmp_rc_transactions"


client = pymongo.MongoClient(os.getenv("MONGO_URI"))


def get_expressions():
    # Receive an array with reconciliation keys
    json_rc_keys = json.loads(RC_KEYS)
    reconciliation_columns_pairs = []
    internal_fields = DEFAULT_INTERNAL_FIELDS

    for key in json_rc_keys:
        reconciliation_columns_pairs.append(
            {"internal_key_name": key["int_col"], "external_key_name": key["ext_col"]}
        )
        internal_fields.append(key["int_col"])

    # Mapp the external keys to match the internal key column names
    rename_exp = {}
    join_exp = []

    for key in reconciliation_columns_pairs:
        rename_exp[f"{key['external_key_name']}"] = key["internal_key_name"]
        join_exp.append(key["internal_key_name"])

    # rename_exp[date_field] = "transaction_create_timestamp" no me acuerdo por que metí esta línea xd
    return rename_exp, join_exp, internal_fields


def get_internal_schemas(odl_fields):
    project_exp = {}
    schema = {}
    remanent_project_exp = {}
    for field in odl_fields:
        project_exp[field] = {"$toString": f"${field}"}
        remanent_project_exp[field] = {"$toString": f"$transaction.{field}"}
        schema[field] = str

        if field in INTEGER_FIELDS:
            project_exp[field] = 1
            remanent_project_exp[field] = f"$transaction.{field}"
            schema[field] = int64()

        if field in DOUBLE_FIELDS:
            project_exp[field] = 1
            remanent_project_exp[field] = f"$transaction.{field}"
            schema[field] = float64()

    return project_exp, remanent_project_exp, schema


def get_internal_df(project_exp, schema):
    card_trx_coll = client[ODL_DB][ODL_COLL]
    internal_df = card_trx_coll.aggregate_polars_all(
        [
            {
                "$match": {
                    "processor_name": PROCESSOR_NAME,
                    "transaction_status_type": {"$in": ["APPROVED"]},
                    "create_timestamp": {
                        "$gte": TIMESTAMP_FROM,
                        "$lt": TIMESTAMP_TO,
                    },
                },
            },
            {"$project": project_exp},
        ],
        schema=Schema(schema),
    ).lazy()

    return internal_df


def get_remanent_internal_df(project_exp, schema):
    card_trx_coll = client[ODL_DB][RESULT_COLL]
    internal_df = card_trx_coll.aggregate_polars_all(
        [
            {
                "$match": {
                    "processor_name": PROCESSOR_NAME,
                    "transaction_status_type": {"$in": ["APPROVED"]},
                    "conciliation_status": "REMANENT",
                }
            },
            {
                "$lookup": {
                    "from": "card_transaction",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "transaction",
                }
            },
            {"$unwind": {"path": "$transaction"}},
            {
                "$project": project_exp,
            },
        ],
        schema=Schema(schema),
    ).lazy()

    return internal_df


def get_external_df(rename_exp):
    ext_trx_coll = client[ODL_DB][EXTERNAL_COLL]
    rename_exp["index"] = "external_row_number"
    renamed_columns = [*rename_exp.values()]

    # Cast to have the same data type for amounts or double values
    with_column_exp = [pl.col("external_row_number").cast(str)]
    for field in DOUBLE_FIELDS:
        if field in renamed_columns:
            with_column_exp.append(pl.col(field).cast(pl.Float64))

    for field in INTEGER_FIELDS:
        if field in renamed_columns:
            with_column_exp.append(pl.col(field).cast(pl.Int64))

    external_df = (
        ext_trx_coll.aggregate_polars_all(
            [
                {
                    "$match": {
                        "transaction_status_type": {"$in": ["APPROVED"]},
                        "create_timestamp": {
                            "$gte": TIMESTAMP_FROM,
                            "$lt": TIMESTAMP_TO,
                        },
                    },
                },
                {
                    "$project": {
                        "_id": 1,
                        "referencia": {"$toString": "$reference_transaction_code"},
                        "monto": {"$toString": "$approved_transaction_amount"},
                        "tipo_de_transaccion": {"$toString": "$transaction_type"},
                        "codigo_aprobacion": {"$toString": "$approval_code"},
                        "estado_transaccion": {"$toString": "$transaction_status_type"},
                        "fecha_transaccion": {"$toString": "$create_timestamp"},
                        "bin": {"$toString": "$tmp1_bin_code"},
                        "tipo_tarjeta": {"$toString": "$card_type"},
                        "ultimos_digitos": {"$toString": "$last_four_digit_code"},
                        "codigo_ksh": {"$toString": "$transaction_code"},
                        "processor_name": {"$toString": "$processor_name"},
                        "country_name": {"$toString": "$country_name"},
                        "processor_type": {"$toString": "$processor_type"},
                        "ticket_code": {"$toString": "$ticket_code"},
                    },
                },
            ]
        )
        .with_row_index(offset=1)
        .rename(rename_exp)
        .with_columns(with_column_exp)
        .lazy()
    )

    return external_df


def get_external_df_from_file(rename_exp, file_path):
    rename_exp["index"] = "external_row_number"
    renamed_columns = [*rename_exp.values()]

    # Cast to have the same data type for amounts or double values
    with_column_exp = [pl.col("external_row_number").cast(str)]
    for field in DOUBLE_FIELDS:
        if field in renamed_columns:
            with_column_exp.append(pl.col(field).cast(pl.Float64))

    for field in INTEGER_FIELDS:
        if field in renamed_columns:
            with_column_exp.append(pl.col(field).cast(pl.Int64))

    external_df = (
        pl.read_csv(file_path, infer_schema_length=0)
        .with_row_index(offset=1)
        .rename(rename_exp)
        .with_columns(with_column_exp)
        .lazy()
    )

    return external_df


def get_external_df_from_s3(rename_exp):
    rename_exp["index"] = "external_row_number"
    renamed_columns = [*rename_exp.values()]

    # Cast to have the same data type for amounts or double values
    with_column_exp = [pl.col("external_row_number").cast(str)]
    for field in DOUBLE_FIELDS:
        if field in renamed_columns:
            with_column_exp.append(pl.col(field).cast(pl.Float64))

    for field in INTEGER_FIELDS:
        if field in renamed_columns:
            with_column_exp.append(pl.col(field).cast(pl.Int64))

    external_df = (
        pl.read_csv(
            "s3://finops-interchange-plus/external_one_day.csv",
            infer_schema_length=0,
        )
        .with_row_index(offset=1)
        .rename(rename_exp)
        .with_columns(with_column_exp)
        .lazy()
    )

    return external_df


def get_matching_records(internal_df, external_df, join_exp):
    print(join_exp)
    join_df = (
        internal_df.join(external_df, on=join_exp, how="inner")
        .with_columns(
            pl.lit(CONCILIATION_STATUS).alias("conciliation_status"),
            pl.lit(CONCILIATION_TIMESTAMP).alias("conciliation_timestamp"),
            pl.lit(CONCILIATION_DATE).alias("conciliation_date"),
            pl.lit(EXECUTION_ID).alias("execution_id"),
            pl.lit(EXECUTION_TYPE).alias("execution_type"),
            pl.lit(EXECUTION_DATE).alias("execution_date"),
            pl.lit(EXECUTION_TIMESTAMP).alias("execution_timestamp"),
            pl.lit(",".join(join_exp)).alias("conciliation_key_code"),
            pl.lit("Descripción").alias("conciliation_key_description"),
            pl.lit(EXTERNAL_SOURCE_NAME).alias("external_source_name"),
        )
        .select(
            [
                "_id",
                "external_row_number",
                "conciliation_status",
                "conciliation_timestamp",
                "conciliation_date",
                "execution_id",
                "execution_type",
                "execution_date",
                "execution_timestamp",
                "conciliation_key_code",
                "conciliation_key_description",
                "external_source_name",
                "create_timestamp",
                "processor_name",
                "transaction_status_type",
                "approved_transaction_amount",
            ]
        )
        .rename({"create_timestamp": "transaction_create_timestamp"})
        .lazy()
    )

    return join_df


def get_internal_not_matching_records(left_df, right_df, join_exp):
    # Find transactions which doesn´t match
    nc_df = (
        left_df.join(right_df, on=join_exp, how="anti")
        .with_columns(
            pl.lit(NON_CONCILIATION_STATUS).alias("conciliation_status"),
            pl.lit(CONCILIATION_TIMESTAMP).alias("conciliation_timestamp"),
            pl.lit(CONCILIATION_DATE).alias("conciliation_date"),
            pl.lit(EXECUTION_ID).alias("execution_id"),
            pl.lit(EXECUTION_TYPE).alias("execution_type"),
            pl.lit(EXECUTION_DATE).alias("execution_date"),
            pl.lit(EXECUTION_TIMESTAMP).alias("execution_timestamp"),
        )
        .select(
            [
                "_id",
                "conciliation_status",
                "conciliation_timestamp",
                "conciliation_date",
                "execution_id",
                "execution_type",
                "execution_date",
                "execution_timestamp",
                "create_timestamp",
                "processor_name",
                "transaction_status_type",
                "approved_transaction_amount",
            ]
        )
        .rename({"create_timestamp": "transaction_create_timestamp"})
        .lazy()
    )

    return nc_df


def get_external_not_matching_records(left_df, right_df, join_exp):
    # Find transactions which doesn´t match
    nc_df = (
        left_df.join(right_df, on=join_exp, how="anti")
        .with_columns(
            pl.lit(NON_CONCILIATION_STATUS).alias("conciliation_status"),
            pl.lit(CONCILIATION_TIMESTAMP).alias("conciliation_timestamp"),
            pl.lit(CONCILIATION_DATE).alias("conciliation_date"),
            pl.lit(EXECUTION_ID).alias("execution_id"),
            pl.lit(EXECUTION_TYPE).alias("execution_type"),
            pl.lit(EXECUTION_DATE).alias("execution_date"),
            pl.lit(EXECUTION_TIMESTAMP).alias("execution_timestamp"),
            pl.lit(EXTERNAL_SOURCE_NAME).alias("external_source_name"),
            pl.lit(PROCESSOR_NAME).alias("processor_name"),
        )
        .select(
            [
                "_id",
                "external_row_number",
                "conciliation_status",
                "conciliation_timestamp",
                "conciliation_date",
                "execution_id",
                "execution_type",
                "execution_date",
                "execution_timestamp",
                "external_source_name",
                "processor_name",
                "create_timestamp",
            ]
        )
        .rename({"create_timestamp": "transaction_create_timestamp"})
        .lazy()
    )

    return nc_df


def write_to_mongo(dataframe, dest_collection):
    results_coll = client[ODL_DB][dest_collection]
    # print(dataframe.head(5))
    write(results_coll, dataframe.collect())


def clean_tmp_collections():
    client[ODL_DB][TMP_COLL].drop()


def divide_timestamps_into_intervals(n, source_coll, dest_coll):
    interval = (TIMESTAMP_TO - TIMESTAMP_FROM) / n
    intervals = []
    start = TIMESTAMP_FROM
    for i in range(n):
        end = start + interval
        intervals.append((int(start), int(end), source_coll, dest_coll))
        start = end
    return intervals


def move_tmp_data(interval):
    client[ODL_DB][interval[2]].aggregate(
        [
            {
                "$match": {
                    "transaction_create_timestamp": {
                        "$gte": interval[0],
                        "$lt": interval[1],
                    }
                }
            },
            {
                "$merge": {
                    "into": {"db": ODL_DB, "coll": interval[3]},
                    "on": "_id",
                    "whenMatched": "merge",
                    "whenNotMatched": "insert",
                }
            },
        ]
    )


def move_tmp_data_to_final(source_coll, dest_coll):
    ivs = divide_timestamps_into_intervals(5, source_coll, dest_coll)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(move_tmp_data, ivs)


def save_aggregated_results(match_df: pl.LazyFrame, unmatch_df: pl.LazyFrame):
    # Get the aggregated results as DF and transform them to dictionaries
    agg_match_dic = (
        match_df.select(pl.sum("approved_transaction_amount"), pl.count("_id"))
        .collect()
        .iter_rows(named=True)
        .__next__()
    )
    agg_unmatch_dic = (
        unmatch_df.select(pl.sum("approved_transaction_amount"), pl.count("_id"))
        .collect()
        .iter_rows(named=True)
        .__next__()
    )

    mongo_data = {
        "_id": EXECUTION_ID,
        "execution_type": EXECUTION_TYPE,
        "execution_timestamp": EXECUTION_TIMESTAMP,
        "execution_date": EXECUTION_DATE,
        "conciliation_timestamp": CONCILIATION_TIMESTAMP,
        "conciliation_date": CONCILIATION_DATE,
        "processor_name": PROCESSOR_NAME,
        "conciliated_transactions_number": agg_match_dic["_id"],
        "remanent_transactions_number": agg_unmatch_dic["_id"],
        "conciliated_amount": agg_match_dic["approved_transaction_amount"],
        "remanent_amount": agg_unmatch_dic["approved_transaction_amount"],
        "conciliation_currency": "MXN",
    }

    print(mongo_data)

    agg_results_coll = client[ODL_DB][AGG_RESULT_COLL]
    agg_results_coll.insert_one(mongo_data)


def persist_results(
    matching_df: pl.LazyFrame,
    unmatching_df: pl.LazyFrame,
    ext_unmatching_df: pl.LazyFrame,
):
    write_to_mongo(matching_df, TMP_COLL)
    move_tmp_data_to_final(TMP_COLL, RESULT_COLL)
    clean_tmp_collections()

    write_to_mongo(unmatching_df, TMP_COLL)
    move_tmp_data_to_final(TMP_COLL, RESULT_COLL)
    clean_tmp_collections()

    write_to_mongo(ext_unmatching_df, TMP_COLL)
    move_tmp_data_to_final(TMP_COLL, EXT_REMANENT_RECORDS)
    clean_tmp_collections()

    save_aggregated_results(matching_df, unmatching_df)


if __name__ == "__main__":
    # Generate base experssions to operate
    rename_exp, join_exp, odl_fields = get_expressions()
    int_prjt_exp, rmnt_int_prjt_exp, schema = get_internal_schemas(odl_fields)

    # Get the time window data and the remanant data to conciliate
    ksh_df = get_internal_df(int_prjt_exp, schema)
    print(ksh_df.collect())

    ksh_remanent_df = get_remanent_internal_df(rmnt_int_prjt_exp, schema)
    ksh_complete_df = pl.concat([ksh_df, ksh_remanent_df]).unique(
        keep="first", maintain_order=True
    )
    print(ksh_complete_df.collect())

    # Get the external data to conciliate
    external_df = get_external_df_from_file(rename_exp, "test_2024-04-03.csv")
    print(external_df.collect())

    # Get the matching records
    matching_df = get_matching_records(ksh_complete_df, external_df, join_exp)
    print(matching_df.collect())

    # Get duplicated records in the matching dataframe
    duplicate_df = matching_df.filter(pl.col("_id").is_duplicated())
    print(duplicate_df.collect())

    # Get records that are in Kushki data but not in external data
    unmatching_df = get_internal_not_matching_records(
        ksh_complete_df, external_df, join_exp
    )
    print(unmatching_df.collect())

    # Get records that are in external data but not in Kushki data
    unmatching_df_2 = get_external_not_matching_records(
        external_df, ksh_complete_df, join_exp
    )
    print(unmatching_df_2.collect())

    persist_results(matching_df, unmatching_df, unmatching_df_2)
