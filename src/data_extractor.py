from pymongoarrow.api import Schema
import polars as pl


class DataExtractor:
    def __init__(
        self,
        mongo_cfg,
        mongo_client,
        odl_prj_exp,
        odl_rmnt_prj_exp,
        odl_schema,
        cast_ext_exp,
        repeat_key_exp,
    ):
        self.mongo_cfg = mongo_cfg
        self.mongo_client = mongo_client
        self.odl_prj_exp = odl_prj_exp
        self.odl_rmnt_prj_exp = odl_rmnt_prj_exp
        self.odl_schema = odl_schema
        self.cast_ext_exp = cast_ext_exp
        self.rename_ext_exp = odl_schema
        self.cast_ext_exp = cast_ext_exp
        self.repeat_key_exp = repeat_key_exp

    def get_odl_df(self):
        card_trx_coll = self.mongo_client[self.mongo_cfg["database"]][
            self.mongo_cfg["collection"]
        ]
        internal_df = card_trx_coll.aggregate_polars_all(
            [
                {
                    "$match": {
                        "processor_name": self.mongo_cfg["processor_name"],
                        "country_name": self.mongo_cfg["country_name"],
                        "transaction_status_type": {
                            "$in": self.mongo_cfg["transaction_statuses"]
                        },
                        "create_timestamp": {
                            "$gte": self.mongo_cfg["timestamp_from"],
                            "$lt": self.mongo_cfg["timestamp_to"],
                        },
                    },
                },
                {"$project": self.odl_prj_exp},
            ],
            schema=Schema(self.odl_schema),
        )

        return internal_df

    def get_remanent_odl_df(self):
        card_trx_coll = self.mongo_client[self.mongo_cfg["database"]][
            self.mongo_cfg["collection"]
        ]
        internal_df = card_trx_coll.aggregate_polars_all(
            [
                {
                    "$match": {
                        "processor_name": self.mongo_cfg["processor_name"],
                        "transaction_status_type": {
                            "$in": self.mongo_cfg["transaction_statuses"]
                        },
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
                    "$project": self.odl_rmnt_prj_exp,
                },
            ],
            schema=Schema(self.odl_schema),
        )

        return internal_df

    def get_complete_odl_df(self):
        odl_df = self.get_odl_df()
        remanent_odl_df = self.get_remanent_odl_df()
        complete_odl_df = (
            pl.concat([odl_df, remanent_odl_df])
            .unique(keep="first", maintain_order=True)
        )

        return complete_odl_df

    def get_df_from_file(self, file_path):
        external_df = (
            pl.read_csv(file_path, infer_schema_length=0)
            .with_row_index(offset=1)
            .rename({"index": "file_row_number"})
        )

        df_columns = external_df.columns

        ext_rename_exp = {}
        for col in df_columns:
            ext_rename_exp[col] = "ext_" + col

        external_df = (
            external_df.with_columns(self.cast_ext_exp)
            .rename(ext_rename_exp)
            .with_columns(self.repeat_key_exp)
        )

        # external_df.write_csv("./results/external.csv")

        return external_df
