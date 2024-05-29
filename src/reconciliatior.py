from typing import Dict
import polars as pl
import polars.selectors as cs


class Reconciliator:
    def __init__(
        self, join_exp: Dict, odl_df: pl.DataFrame, file_df: pl.DataFrame
    ) -> None:
        self.iterations = 1
        self.odl_df = odl_df
        self.file_df = file_df
        self.a_to_b_mt = pl.DataFrame()
        self.a_to_b_nmt = pl.DataFrame()
        self.b_to_a_nmt = pl.DataFrame()
        self.join_exp = join_exp

    def _process_duplicates(self, df: pl.DataFrame, col_name: str):
        duplicates_df = df.filter(pl.col(col_name).is_duplicated())

        if duplicates_df.select(pl.len()).item():
            nc_clean_dup_df = duplicates_df.unique(
                subset=[col_name], keep="last", maintain_order=True
            )

            # df_columns = df.columns
            # dup_rename_exp = {}

            # for col in df_columns:
            #     if "ext_" in col:
            #         dup_rename_exp[col] = col.replace("ext_", "")

            #     if "exd_" in col:
            #         dup_rename_exp[col] = col.replace("exd_", "")

            # no_conciliated_dup_df = clean_dup_df.select(
            #     [cs.starts_with("ext_"), cs.starts_with("exd_")]
            # ).rename(dup_rename_exp)

            # To recreate original external dataframe imported repeating rc_keys columns to allow duplicate processing
            add_cols_exp = []
            print(nc_clean_dup_df.columns)
            print(self.join_exp["b_columns"])
            for ext_col in self.join_exp["b_columns"]:
                add_cols_exp.append(
                    pl.col(ext_col.replace("ext_", "exd_")).alias(ext_col)
                )

            no_conciliated_dup_df = nc_clean_dup_df.select(
                [cs.starts_with("ext_"), cs.starts_with("exd_")]
            ).with_columns(add_cols_exp)
            self.b_to_a_nmt = pl.concat(
                [self.b_to_a_nmt, no_conciliated_dup_df], how="diagonal"
            )

            clean_df = df.unique(subset=[col_name], keep="first", maintain_order=True)
            return clean_df
        else:
            return df

    def save_to_file(self):
        self.a_to_b_mt.write_csv(f"./results/a_to_b_mt({self.iterations}).csv")
        self.a_to_b_nmt.write_csv(f"./results/a_to_b_nmt({self.iterations}).csv")
        self.b_to_a_nmt.write_csv(f"./results/b_to_a_nmt({self.iterations}).csv")

    def match_records(self):
        print("ODL DF")
        print(self.odl_df)
        print(self.odl_df.columns)
        join_df = self.odl_df.join(
            self.file_df,
            left_on=self.join_exp["a_columns"],
            right_on=self.join_exp["b_columns"],
            how="inner",
        )
        print("JOIN DF")
        print(join_df)
        clean_join_df = self._process_duplicates(join_df, "_id")
        self.a_to_b_mt = pl.concat([self.a_to_b_mt, clean_join_df], how="diagonal")
        print(self.a_to_b_mt)

    def not_match_records(self):
        join_df = self.odl_df.join(
            self.file_df,
            left_on=self.join_exp["a_columns"],
            right_on=self.join_exp["b_columns"],
            how="anti",
        )
        self.a_to_b_nmt = pl.concat([self.a_to_b_nmt, join_df], how="diagonal")
        print(self.a_to_b_nmt)

        join_df = self.file_df.join(
            self.odl_df,
            left_on=self.join_exp["b_columns"],
            right_on=self.join_exp["a_columns"],
            how="anti",
        )
        self.b_to_a_nmt = pl.concat([self.b_to_a_nmt, join_df], how="diagonal")
        print(self.b_to_a_nmt)

    def new_conciliation_cycle(self):
        self.odl_df = self.a_to_b_nmt
        self.file_df = self.b_to_a_nmt
        self.a_to_b_nmt = pl.DataFrame()
        self.b_to_a_nmt = pl.DataFrame()
        self.iterations += 1
