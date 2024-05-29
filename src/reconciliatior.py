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
        join_df = self.odl_df.join(
            self.file_df,
            left_on=self.join_exp["a_columns"],
            right_on=self.join_exp["b_columns"],
            how="inner",
        )
        clean_join_df = self._process_duplicates(join_df, "_id")
        self.a_to_b_mt = pl.concat([self.a_to_b_mt, clean_join_df], how="diagonal")

    def not_match_records(self):
        join_df = self.odl_df.join(
            self.file_df,
            left_on=self.join_exp["a_columns"],
            right_on=self.join_exp["b_columns"],
            how="anti",
        )
        self.a_to_b_nmt = pl.concat([self.a_to_b_nmt, join_df], how="diagonal")

        join_df = self.file_df.join(
            self.odl_df,
            left_on=self.join_exp["b_columns"],
            right_on=self.join_exp["a_columns"],
            how="anti",
        )
        self.b_to_a_nmt = pl.concat([self.b_to_a_nmt, join_df], how="diagonal")

    def apply_tolerance(self):
        calcs_exp = []
        sort_exp = []
        tlr_rules = [{"field": "importe", "tolerance": 1}]
        mt_tolerance_exp = pl.all_horizontal(
            pl.col(f"{rule['field']}_diff") <= pl.lit(rule["tolerance"])
            for rule in tlr_rules
        )
        # nmt_tolerance_exp = pl.all_horizontal(
        #     pl.col(f"{rule['field']}_diff") > pl.lit(rule["tolerance"])
        #     for rule in tlr_rules
        # )
        a_columns = self.join_exp["a_columns"].copy()
        b_columns = self.join_exp["b_columns"].copy()
        drop_columns = []
        
        for rule in tlr_rules:
            clm_name = f"ext_{rule['field']}"
            if clm_name in self.join_exp["b_columns"]:
                index = self.join_exp["b_columns"].index(clm_name)
                diff_clm_name = f"{clm_name.replace('ext_', '')}_diff"
                drop_columns.append(clm_name)
                drop_columns.append(diff_clm_name)
                sort_exp.append(diff_clm_name)
                calcs_exp.append(
                    pl.col(clm_name.replace("ext_", "exd_"))
                    .sub(pl.col(a_columns[index]))
                    .abs()
                    .alias(diff_clm_name)
                )
                b_columns.pop(index)
                a_columns.pop(index)

        b_mdf_cols = [col.replace("ext_", "exd_") for col in self.join_exp["b_columns"]]

        join_df = (
            self.odl_df.join(
                self.file_df, left_on=a_columns, right_on=b_columns, how="inner"
            )
            .with_columns(calcs_exp)
            .sort(diff_clm_name)
        )

        # dups_df = join_df.filter(pl.all_horizontal(pl.col(a_columns).is_duplicated()))
        match_tolerance_df = join_df.filter(mt_tolerance_exp)
        # nmt_tolerance_df = join_df.filter(nmt_tolerance_exp)
        mt_tl_unique_df = match_tolerance_df.unique(subset=a_columns, keep="first", maintain_order=True)

        self.a_to_b_nmt = self.odl_df.join(mt_tl_unique_df, left_on=a_columns, right_on=a_columns, how="anti")
        self.b_to_a_nmt = self.file_df.join(mt_tl_unique_df, left_on=b_mdf_cols, right_on=b_mdf_cols, how="anti")
        self.a_to_b_mt = pl.concat([self.a_to_b_mt, mt_tl_unique_df.drop(drop_columns)], how="diagonal")



        # join_df.write_csv(f"./results/join_tolerance_df({self.iterations}).csv")
        # dups_df.write_csv(f"./results/dup_tolerance_df({self.iterations}).csv")
        # mt_tl_unique_df.write_csv(f"./results/mt_tl_unique_df({self.iterations}).csv")
        # match_tolerance_df.write_csv(f"./results/match_tolerance_df({self.iterations}).csv")
        # mt_tl_unique_df.drop(drop_columns).write_csv(f"./results/clean_mt({self.iterations}).csv")
        # nmt_tolerance_df.write_csv(f"./results/nmt_tolerance_df({self.iterations}).csv")
        # odl_after_df.write_csv(f"./results/odl_after_df({self.iterations}).csv")
        # file_after_df.write_csv(f"./results/file_after_df({self.iterations}).csv")


    def new_rc_step(self):
        self.odl_df = self.a_to_b_nmt
        self.file_df = self.b_to_a_nmt
        self.a_to_b_nmt = pl.DataFrame()
        self.b_to_a_nmt = pl.DataFrame()
        self.iterations += 1
