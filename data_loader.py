"""
data_loader.py — 新規事業推進アシストシステム
DBから3テーブルをpandas DataFrameで読み込んで返す。
バックエンド担当（search_engine.py）との接点となるモジュール。
"""

import sqlite3
import pandas as pd
from pathlib import Path

# DB ファイルのパス（database.py と統一）
DB_PATH = Path("data/projects.db")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    DBから projects / project_members / project_factors を
    pandas DataFrame として読み込んで返す。

    Returns:
        projects_df : 案件基本情報 DataFrame
        members_df  : 関係メンバー DataFrame
        factors_df  : 成功・失敗要因 DataFrame

    Usage:
        projects_df, members_df, factors_df = load_data()

    各DataFrameの主なカラム:
        projects_df
            - project_id            : 案件ID（主キー）
            - project_name          : 案件名
            - proposal_category     : 提案カテゴリ
            - target_group          : 対象部門
            - budget_range          : 予算レンジ
            - expected_effect_type  : 想定効果の種類
            - project_phase         : 事業フェーズ
            - proposal_period       : 提案時期
            - proposal_year         : 年度
            - proposal_department   : 提案部署
            - project_summary       : 案件の詳細
            - ringi_status          : 稟議結果
            - ringi_reason          : 稟議理由
            - implemented_flag      : 実施の有無
            - final_result          : 最終結果

        members_df
            - member_id             : レコードID
            - project_id            : 案件ID（projects_df との結合キー）
            - related_department    : 関与した部署
            - person_name           : 担当者名
            - person_role           : 役割

        factors_df
            - factor_id             : レコードID
            - project_id            : 案件ID（projects_df との結合キー）
            - factor_type           : 'success' または 'failure'
            - factor_text           : 成功・失敗要因のテキスト
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DBファイルが見つかりません: {DB_PATH}\n"
            "先に database.py の init_db() を実行してDBを初期化してください。\n"
            "実行方法: python3 -c \"from database import init_db; init_db()\""
        )

    conn = sqlite3.connect(str(DB_PATH))

    try:
        projects_df = pd.read_sql("SELECT * FROM projects       ORDER BY project_id", conn)
        members_df  = pd.read_sql("SELECT * FROM project_members ORDER BY project_id", conn)
        factors_df  = pd.read_sql("SELECT * FROM project_factors ORDER BY project_id", conn)
    finally:
        conn.close()

    return projects_df, members_df, factors_df


def load_merged() -> pd.DataFrame:
    """
    3テーブルを project_id で結合した1つの DataFrame を返す。
    検索時に全情報を一括で参照したい場合に使用する。

    Returns:
        merged_df : 案件・メンバー・要因を結合した DataFrame

    結合方法:
        - members を project_id でグループ化し「担当者名リスト」を1列にまとめる
        - factors を success / failure に分けてそれぞれリスト化して結合する
    """
    projects_df, members_df, factors_df = load_data()

    # メンバー情報を1案件1行にまとめる（「氏名（部署）」形式）
    if not members_df.empty:
        members_grouped = (
            members_df
            .assign(member_str=lambda df:
                df["person_name"] + "（" + df["related_department"] + "）")
            .groupby("project_id")["member_str"]
            .apply(lambda x: "、".join(x))
            .reset_index()
            .rename(columns={"member_str": "members_text"})
        )
    else:
        members_grouped = pd.DataFrame(columns=["project_id", "members_text"])

    # 成功要因・失敗要因を1案件1行にまとめる
    if not factors_df.empty:
        success_df = (
            factors_df[factors_df["factor_type"] == "success"]
            .groupby("project_id")["factor_text"]
            .apply(lambda x: " / ".join(x))
            .reset_index()
            .rename(columns={"factor_text": "success_factors"})
        )
        failure_df = (
            factors_df[factors_df["factor_type"] == "failure"]
            .groupby("project_id")["factor_text"]
            .apply(lambda x: " / ".join(x))
            .reset_index()
            .rename(columns={"factor_text": "failure_factors"})
        )
    else:
        success_df = pd.DataFrame(columns=["project_id", "success_factors"])
        failure_df = pd.DataFrame(columns=["project_id", "failure_factors"])

    # projects に結合する
    merged_df = (
        projects_df
        .merge(members_grouped, on="project_id", how="left")
        .merge(success_df,      on="project_id", how="left")
        .merge(failure_df,      on="project_id", how="left")
    )

    # 欠損値を空文字で埋める
    merged_df = merged_df.fillna("")

    return merged_df


# ── 動作確認用 ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== load_data() テスト ===")
    projects_df, members_df, factors_df = load_data()
    print(f"  projects : {len(projects_df)} 件  |  カラム: {list(projects_df.columns)}")
    print(f"  members  : {len(members_df)} 件  |  カラム: {list(members_df.columns)}")
    print(f"  factors  : {len(factors_df)} 件  |  カラム: {list(factors_df.columns)}")

    print("\n=== load_merged() テスト ===")
    merged_df = load_merged()
    print(f"  merged   : {len(merged_df)} 件  |  カラム: {list(merged_df.columns)}")
    print("\n--- 先頭1件 ---")
    print(merged_df.head(1).T.to_string())
