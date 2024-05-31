import polars as pl
from pymongoarrow.monkey import patch_all
from config import (
    RC_KEYS,
    DEFAULT_INTERNAL_FIELDS,
    INTEGER_FIELDS,
    DOUBLE_FIELDS,
    PROCESSOR_NAME,
    COUNTRY_NAME,
    TRANSACTION_STATUSES,
    TIMESTAMP_FROM,
    TIMESTAMP_TO,
    ODL_DB,
    ODL_COLL,
    EXTERNAL_SOURCE_NAME,
)
from mongo_gtw import MongoGtw
from data_extractor import DataExtractor
from expression_builder import ExpressionBuilder
from reconciliatior import Reconciliator
import os


patch_all()
pl.Config.set_fmt_float("full")
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(300)

if __name__ == "__main__":
    mongo_client = MongoGtw(os.getenv("MONGO_URI")).get_client()
    exp_builder = ExpressionBuilder(
        RC_KEYS, DEFAULT_INTERNAL_FIELDS, INTEGER_FIELDS, DOUBLE_FIELDS
    )
    exp_builder.build_expressions()

    data_extractor = DataExtractor(
        {
            "database": ODL_DB,
            "collection": ODL_COLL,
            "processor_name": PROCESSOR_NAME,
            "country_name": COUNTRY_NAME,
            "transaction_statuses": TRANSACTION_STATUSES,
            "timestamp_from": TIMESTAMP_FROM,
            "timestamp_to": TIMESTAMP_TO,
        },
        mongo_client,
        exp_builder.odl_prj_exp,
        exp_builder.odl_rmnt_prj_exp,
        exp_builder.odl_schema,
        exp_builder.cast_ext_exp,
        exp_builder.repeat_key_exp,
    )

    odl_df = data_extractor.get_complete_odl_df()
    print(odl_df)

    file_df = data_extractor.get_df_from_file(f"./files/{EXTERNAL_SOURCE_NAME}")
    print(file_df)

    rc = Reconciliator(exp_builder.join_exp, odl_df, file_df)

    rc.apply_zero_effect()
    rc.match_records()
    rc.not_match_records()
    rc.save_to_file()
    # rc.new_rc_step()
    # rc.apply_tolerance()
    # rc.save_to_file()

    # rc.new_rc_step()
    # rc.match_records()
    # rc.not_match_records()
    # rc.save_to_file()

    
