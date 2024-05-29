import json
import polars as pl
from pyarrow import float64, int64


class ExpressionBuilder:
    def __init__(
        self, rc_keys, default_internal_keys, int_fields, double_fields
    ) -> None:
        self.rc_keys = json.loads(rc_keys)
        self.defaut_internal_keys = default_internal_keys
        self.int_fields = int_fields
        self.double_fields = double_fields
        self.join_exp = {"a_columns": [], "b_columns": []}
        self.internal_fields = self.defaut_internal_keys
        self.odl_prj_exp = {}
        self.odl_rmnt_prj_exp = {}
        self.odl_schema = {}
        self.cast_ext_exp = []
        self.repeat_key_exp = []

    def _set_expressions(self):
        # Build the join expression
        for key in self.rc_keys:
            self.internal_fields.append(key["int_col"])
            self.join_exp["a_columns"].append(key["int_col"])
            self.join_exp["b_columns"].append("ext_" + key["ext_col"])

    def _set_internal_schemas(self):
        for field in self.internal_fields:
            self.odl_prj_exp[field] = {"$toString": f"${field}"}
            self.odl_rmnt_prj_exp[field] = {"$toString": f"$transaction.{field}"}
            self.odl_schema[field] = str

            if field in self.int_fields:
                self.odl_prj_exp[field] = 1
                self.odl_rmnt_prj_exp[field] = f"$transaction.{field}"
                self.odl_schema[field] = int64()

            if field in self.double_fields:
                self.odl_prj_exp[field] = 1
                self.odl_rmnt_prj_exp[field] = f"$transaction.{field}"
                self.odl_schema[field] = float64()

    def _set_external_expressions(self):
        internal_columns = list(map(lambda x: x["int_col"], self.rc_keys))
        external_columns = list(map(lambda x: x["ext_col"], self.rc_keys))

        # Cast to have the same data type for amounts or double values
        for field in self.double_fields:
            if field in internal_columns:
                self.cast_ext_exp.append(
                    pl.col(external_columns[internal_columns.index(field)]).cast(
                        pl.Float64
                    )
                )

        for field in self.int_fields:
            if field in internal_columns:
                self.cast_ext_exp.append(
                    pl.col(external_columns[internal_columns.index(field)]).cast(
                        pl.Int64
                    )
                )

        for rc_key in self.rc_keys:
            self.repeat_key_exp.append(
                pl.col("ext_" + rc_key["ext_col"]).alias("exd_" + rc_key["ext_col"])
            )

    def build_expressions(self):
        self._set_expressions()
        self._set_internal_schemas()
        self._set_external_expressions()
