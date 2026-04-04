"""
search_engine.py — 新規提案・業務改善案の類似案件検索エンジン

前提:
- data_loader.py の load_data() から
    projects_df, members_df, factors_df
  を受け取る
- search_projects(input_dict) で検索を実行し、
  ranked_results_df（pandas DataFrame）を返す
"""

from typing import Any, Dict, List

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from data_loader import load_data


SEARCH_FIELDS = [
    "proposal_category",
    "target_group",
    "budget_range",
    "expected_effect_type",
    "project_phase",
]


def _aggregate_members(members_df: pd.DataFrame) -> pd.DataFrame:
    """案件ごとに関与部署・担当者を集約する。"""
    # データが空なら、後続の merge で困らないよう空の表を返す
    if members_df is None or members_df.empty:
        return pd.DataFrame(columns=["project_id", "related_departments", "related_members"])

    # 元の DataFrame を壊さないようにコピーして作業する
    work = members_df.copy()

    # 必要カラムが欠けていても落ちないように空文字列で補う
    for col in ["related_department", "person_name", "person_role"]:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str)

    # 案件ごとに関与部署をまとめる
    dept_df = (
        work.groupby("project_id")["related_department"]
        .apply(lambda s: " / ".join(sorted({x for x in s if x})))
        .reset_index(name="related_departments")
    )

    # 案件ごとに担当者を「氏名（役割）」形式でまとめる
    member_df = (
        work.groupby("project_id")
        .apply(
            lambda g: " / ".join(
                sorted(
                    {
                        f"{name}（{role}）" if role else name
                        for name, role in zip(g["person_name"], g["person_role"])
                        if name
                    }
                )
            )
        )
        .reset_index(name="related_members")
    )

    # 部署集約結果と担当者集約結果を project_id で結合する
    return dept_df.merge(member_df, on="project_id", how="outer")


def _aggregate_factors(factors_df: pd.DataFrame) -> pd.DataFrame:
    """案件ごとに成功要因・失敗要因を集約する。"""
    # データが空なら、後続の merge で困らないよう空の表を返す
    if factors_df is None or factors_df.empty:
        return pd.DataFrame(columns=["project_id", "success_factors", "failure_factors"])

    # 元の DataFrame を壊さないようにコピーして作業する
    work = factors_df.copy()

    # 必要カラムが欠けていても落ちないように空文字列で補う
    if "factor_type" not in work.columns:
        work["factor_type"] = ""
    if "factor_text" not in work.columns:
        work["factor_text"] = ""

    # 欠損値を空文字にし、比較しやすいよう factor_type は小文字にそろえる
    work["factor_type"] = work["factor_type"].fillna("").astype(str).str.lower()
    work["factor_text"] = work["factor_text"].fillna("").astype(str)

    # success の要因だけを案件ごとにまとめる
    success_df = (
        work[work["factor_type"] == "success"]
        .groupby("project_id")["factor_text"]
        .apply(lambda s: " / ".join([x for x in s if x]))
        .reset_index(name="success_factors")
    )

    # failure の要因だけを案件ごとにまとめる
    failure_df = (
        work[work["factor_type"] == "failure"]
        .groupby("project_id")["factor_text"]
        .apply(lambda s: " / ".join([x for x in s if x]))
        .reset_index(name="failure_factors")
    )

    # 成功要因と失敗要因を project_id で結合する
    return success_df.merge(failure_df, on="project_id", how="outer")


def _prepare_project_records(
    projects_df: pd.DataFrame,
    members_df: pd.DataFrame,
    factors_df: pd.DataFrame,
) -> List[dict]:
    """DataFrame を検索用の案件辞書リストに整形する。"""
    # 案件データが空なら検索対象が存在しないので空リストを返す
    if projects_df is None or projects_df.empty:
        return []

    # 元の DataFrame を壊さないようにコピーして作業する
    df = projects_df.copy()

    # 検索や表示で使う基本カラムを定義しておく
    required_columns = [
        "project_id",
        "project_name",
        "proposal_category",
        "target_group",
        "budget_range",
        "expected_effect_type",
        "project_phase",
        "proposal_period",
        "proposal_year",
        "proposal_department",
        "project_summary",
        "ringi_status",
        "ringi_reason",
        "implemented_flag",
        "final_result",
    ]

    # 必須カラムが欠けていても動くように空文字列で補う
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""

    # 関与部署・担当者を案件単位にまとめて結合する
    df = df.merge(_aggregate_members(members_df), on="project_id", how="left")

    # 成功要因・失敗要因を案件単位にまとめて結合する
    df = df.merge(_aggregate_factors(factors_df), on="project_id", how="left")

    # 追加列の欠損値を空文字にして後続処理で扱いやすくする
    for col in ["related_departments", "related_members", "success_factors", "failure_factors"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("")

    # SearchEngine が扱いやすいように「辞書のリスト」に変換して返す
    return df.to_dict(orient="records")


class SearchEngine:
    """TF-IDF ベースの案件検索エンジン"""

    def __init__(self):
        # TF-IDF ベクトライザーを初期化する
        self.vectorizer = TfidfVectorizer(
            max_features=5000,
            ngram_range=(1, 2),
            min_df=1,
            max_df=0.95,
            sublinear_tf=True,
        )
        self.tfidf_matrix = None
        self.projects = []
        self.is_fitted = False

    @staticmethod
    def _normalize_text(value: Any) -> str:
        """None / NaN を空文字にして前後空白を除く。"""
        # None はそのままでは文字列結合できないので空文字にする
        if value is None:
            return ""

        # pandas の NaN / NA も空文字にそろえる
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        # 最後に文字列化し、前後の空白を削除して返す
        return str(value).strip()

    def _build_project_text(self, project: Dict[str, Any]) -> str:
        """1案件分の情報を検索用の1つの文字列にまとめる。"""
        # よく一致してほしい項目をあらかじめ取り出す
        category = self._normalize_text(project.get("proposal_category"))
        target = self._normalize_text(project.get("target_group"))
        effect = self._normalize_text(project.get("expected_effect_type"))
        budget = self._normalize_text(project.get("budget_range"))
        phase = self._normalize_text(project.get("project_phase"))

        # ranking.py と同様に、重要項目はあえて繰り返して重みを持たせる
        # 今回のテーマでは、カテゴリ・対象・想定効果の一致が特に重要なため3倍に重み付け。
        # 予算レンジ、事業フェーズ、案件名を2倍に重み付け（重み付けの選択は適当）。
        # 文字列の繰り返しで重みを付ける。
        parts = [
            (category + " ") * 3,
            (target + " ") * 3,
            (effect + " ") * 3,
            (budget + " ") * 2,
            (phase + " ") * 2,
            (self._normalize_text(project.get("project_name")) + " ") * 2,
            (self._normalize_text(project.get("proposal_department")) + " "),
            (self._normalize_text(project.get("project_summary")) + " "),
            (self._normalize_text(project.get("ringi_reason")) + " "),
            (self._normalize_text(project.get("related_departments")) + " "),
            (self._normalize_text(project.get("related_members")) + " "),
            (self._normalize_text(project.get("success_factors")) + " "),
            (self._normalize_text(project.get("failure_factors")) + " "),
        ]

        # 余分な空白を詰めて返す
        return " ".join(x.strip() for x in parts if x.strip())

    def _build_query_text(self, input_dict: Dict[str, Any]) -> str:
        """入力項目を検索用の1つの文字列にまとめる。"""
        # ユーザー入力を順番に取り出して、検索エンジンが扱える文章に変換する
        parts = []
        for field in SEARCH_FIELDS:
            value = self._normalize_text(input_dict.get(field, ""))
            if value:
                # カテゴリ・対象・効果は検索上重要なので 2 回入れて少し強める
                if field in ["proposal_category", "target_group", "expected_effect_type"]:
                    parts.extend([value, value])
                else:
                    parts.append(value)

        # 最後に 1 本の文字列へまとめる
        return " ".join(parts)

    def _field_match_bonus(self, query_value: Any, project_value: Any, exact: float, partial: float) -> float:
        """完全一致・部分一致に応じたボーナス倍率を返す。"""
        # 比較前に None / NaN / 空白を整えて、小文字化して比較しやすくする
        q = self._normalize_text(query_value).lower()
        p = self._normalize_text(project_value).lower()

        # どちらかが空なら加点しない
        if not q or not p:
            return 1.0

        # 完全一致なら強めのボーナス
        if q == p:
            return exact

        # 片方がもう片方に含まれていれば弱めのボーナス
        if q in p or p in q:
            return partial

        # 一致しなければ倍率はそのまま
        return 1.0

    def build_index(self, projects: List[dict]):
        """全案件の TF-IDF インデックスを構築する。"""
        # データが無ければインデックスは作れない
        if not projects:
            self.projects = []
            self.tfidf_matrix = None
            self.is_fitted = False
            return

        # 元データを保持しておく
        self.projects = projects

        # 各案件ごとに「検索対象テキスト」を作る
        corpus = []
        for project in projects:
            corpus.append(self._build_project_text(project))

        # 案件群全体を TF-IDF 行列に変換し、検索準備完了フラグを立てる
        self.tfidf_matrix = self.vectorizer.fit_transform(corpus)
        self.is_fitted = True

    def _calculate_final_score(self, project: Dict[str, Any], base_score: float, input_dict: Dict[str, Any]) -> float:
        """TF-IDF の基本スコアに、案件検索向けの項目一致ボーナスを加えて最終スコアを作る。"""
        # TF-IDF + コサイン類似度で得た基本点からスタート
        score = base_score

        # 提案カテゴリが一致していれば強めに加点
        score *= self._field_match_bonus(input_dict.get("proposal_category"), project.get("proposal_category"), exact=1.35, partial=1.15)

        # 対象顧客・対象部門が一致していれば強めに加点
        score *= self._field_match_bonus(input_dict.get("target_group"), project.get("target_group"), exact=1.30, partial=1.12)

        # 想定効果の種類は案件の方向性を示すので、これも強めに
        score *= self._field_match_bonus(input_dict.get("expected_effect_type"), project.get("expected_effect_type"), exact=1.28, partial=1.10)

        # 予算レンジは重要だが、カテゴリほどではないので中程度の加点
        score *= self._field_match_bonus(input_dict.get("budget_range"), project.get("budget_range"), exact=1.15, partial=1.05)

        # 事業フェーズは近いと比較しやすいので補助的に加点
        score *= self._field_match_bonus(input_dict.get("project_phase"), project.get("project_phase"), exact=1.12, partial=1.04)

        # 稟議結果や最終結果が空の案件は情報量が少ないため、少しだけ減点
        if not self._normalize_text(project.get("ringi_status")):
            score *= 0.97
        if not self._normalize_text(project.get("final_result")):
            score *= 0.97

        # 同点に近い場合は新しい案件の方が参考にしやすいので軽く加点
        try:
            year = project.get("proposal_year")
            if pd.notna(year) and str(year).isdigit():
                year = int(year)
                if year >= 2024:
                    score *= 1.05
                elif year >= 2022:
                    score *= 1.02
        except Exception:
            pass

        return score

    def search(self, input_dict: Dict[str, Any], top_n: int = 20) -> List[dict]:
        """入力条件に対して類似案件検索を実行する。"""
        # インデックスが未構築なら検索できないので空配列を返す
        if not self.is_fitted:
            return []

        # 入力フォームの複数項目を、検索用の 1 本の文字列へ変換する
        query = self._build_query_text(input_dict)
        if not query.strip():
            return []

        # 入力文字列をベクトル化し、全案件とのコサイン類似度を計算する
        query_vec = self.vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, self.tfidf_matrix)[0]

        # 各案件について、基本スコア + 項目一致ボーナスで最終点を作る
        results = []
        for idx, base_score in enumerate(similarities):
            project = self.projects[idx].copy()
            final_score = self._calculate_final_score(project, float(base_score), input_dict)

            # 画面表示しやすいように 0〜100 点風の値へ変換して保存する
            project["similarity_score"] = round(final_score * 100, 1)
            project["base_score"] = round(float(base_score) * 100, 1)
            results.append(project)

        # まずスコア順、同点なら新しい年度順で並べ替える
        results.sort(key=lambda x: (x["similarity_score"], x.get("proposal_year", 0)), reverse=True)

        # 上位 top_n 件だけ返す
        return results[:top_n]


_engine = None


def get_engine() -> SearchEngine:
    """検索エンジンのシングルトンを取得する。"""
    global _engine

    # 毎回インスタンスを作り直さず、1つだけ使い回す
    if _engine is None:
        _engine = SearchEngine()
    return _engine


def rebuild_index():
    """DB から最新データを読み込み、検索用インデックスを再構築する。"""
    # data_loader.py から 3 テーブルを DataFrame で受け取る
    projects_df, members_df, factors_df = load_data()

    # 検索エンジンが扱いやすい案件辞書リストへ整形する
    project_records = _prepare_project_records(projects_df, members_df, factors_df)

    # シングルトンの検索エンジンへ流し込んでインデックスを作り直す
    engine = get_engine()
    engine.build_index(project_records)


def search_projects(input_dict: Dict[str, Any], top_n: int = 20) -> pd.DataFrame:
    """アプリ側から呼び出すための窓口関数。"""
    # 検索エンジンがまだ準備されていなければ、ここで自動的にインデックスを作る
    engine = get_engine()
    if not engine.is_fitted:
        rebuild_index()

    # 検索を実行し、辞書リストで結果を受け取る
    results = engine.search(input_dict, top_n=top_n)

    # フロントエンドが扱いやすいように DataFrame へ変換して返す
    ranked_results_df = pd.DataFrame(results)

    # 常に同じ列順で見られるよう、主要カラムの並びをそろえる
    preferred_order = [
        "project_id", "project_name", "similarity_score", "base_score",
        "proposal_period", "proposal_year", "proposal_department",
        "proposal_category", "target_group", "budget_range",
        "expected_effect_type", "project_phase", "ringi_status",
        "ringi_reason", "implemented_flag", "final_result",
        "related_departments", "related_members",
        "success_factors", "failure_factors", "project_summary",
    ]

    if not ranked_results_df.empty:
        existing = [c for c in preferred_order if c in ranked_results_df.columns]
        others = [c for c in ranked_results_df.columns if c not in existing]
        ranked_results_df = ranked_results_df[existing + others]

    return ranked_results_df
