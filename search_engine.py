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

try:
    from llm_advisor import get_kero_advice
except ImportError:
    get_kero_advice = None

SEARCH_FIELDS = [
    "proposal_category",
    "target_group",
    "budget_range",
    "expected_effect_type",
    "project_phase",
]


def _aggregate_members(members_df: pd.DataFrame) -> pd.DataFrame:
    """案件ごとに関与部署・担当者を集約する。"""
    if members_df is None or members_df.empty:
        return pd.DataFrame(columns=["project_id", "related_departments", "related_members"])

    work = members_df.copy()

    for col in ["related_department", "person_name", "person_role"]:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str)

    dept_df = (
        work.groupby("project_id")["related_department"]
        .apply(lambda s: " / ".join(sorted({x for x in s if x})))
        .reset_index(name="related_departments")
    )

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

    return dept_df.merge(member_df, on="project_id", how="outer")


def _aggregate_factors(factors_df: pd.DataFrame) -> pd.DataFrame:
    """案件ごとに成功要因・失敗要因を集約する。"""
    if factors_df is None or factors_df.empty:
        return pd.DataFrame(columns=["project_id", "success_factors", "failure_factors"])

    work = factors_df.copy()

    if "factor_type" not in work.columns:
        work["factor_type"] = ""
    if "factor_text" not in work.columns:
        work["factor_text"] = ""

    work["factor_type"] = work["factor_type"].fillna("").astype(str).str.lower()
    work["factor_text"] = work["factor_text"].fillna("").astype(str)

    success_df = (
        work[work["factor_type"] == "success"]
        .groupby("project_id")["factor_text"]
        .apply(lambda s: " / ".join([x for x in s if x]))
        .reset_index(name="success_factors")
    )

    failure_df = (
        work[work["factor_type"] == "failure"]
        .groupby("project_id")["factor_text"]
        .apply(lambda s: " / ".join([x for x in s if x]))
        .reset_index(name="failure_factors")
    )

    return success_df.merge(failure_df, on="project_id", how="outer")


def _prepare_project_records(
    projects_df: pd.DataFrame,
    members_df: pd.DataFrame,
    factors_df: pd.DataFrame,
) -> List[dict]:
    """DataFrame を検索用の案件辞書リストに整形する。"""
    if projects_df is None or projects_df.empty:
        return []

    df = projects_df.copy()

    required_columns = [
        "project_id", "project_name", "proposal_category", "target_group",
        "budget_range", "expected_effect_type", "project_phase", "proposal_period",
        "proposal_year", "proposal_department", "project_summary", "ringi_status",
        "ringi_reason", "implemented_flag", "final_result",
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = ""

    df = df.merge(_aggregate_members(members_df), on="project_id", how="left")
    df = df.merge(_aggregate_factors(factors_df), on="project_id", how="left")

    for col in ["related_departments", "related_members", "success_factors", "failure_factors"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("")

    return df.to_dict(orient="records")


class SearchEngine:
    """TF-IDF ベースの案件検索エンジン"""

    def __init__(self):
        # TF-IDF ベクトライザーを初期化する
        self.vectorizer = TfidfVectorizer(
            analyzer='char',       # ★追加：日本語のようにスペース区切りがない言語に対応（文字単位で分割）
            ngram_range=(2, 3),    # ★変更：2文字〜3文字の組み合わせで特徴を捉える（例: "業務", "効率化" など）
            max_features=5000,
            min_df=1,
            max_df=0.95,
            sublinear_tf=True,
        )
        self.tfidf_matrix = None
        self.projects = []
        self.is_fitted = False

    @staticmethod
    def _normalize_text(value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    @staticmethod
    def _normalize_budget_range(value: Any) -> str:
        """
        実数の予算金額を、画面の予算レンジ表現に変換する。
        例:
            240000   -> 100万円未満
            2400000  -> 100万円以上、500万円未満
        """
        if value is None:
            return ""

        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        text = str(value).strip().replace(",", "")

        # 数値として解釈できる場合は、金額帯へ変換
        try:
            amount = float(text)

            if amount < 1_000_000:
                return "100万円未満"
            elif amount < 5_000_000:
                return "100万円以上、500万円未満"
            elif amount < 10_000_000:
                return "500万円以上、1000万円未満"
            else:
                return "1000万円以上"
        except ValueError:
            # すでに「100万円未満」などの文字列ならそのまま返す
            return text

    def _budget_matches(self, query_budget: Any, project_budget: Any) -> bool:
        """
        画面で選んだ予算レンジと、案件の実予算が一致するかを判定する。
        予算指定が空なら常に True。
        """
        q = self._normalize_text(query_budget)
        if not q:
            return True

        p = self._normalize_budget_range(project_budget)
        return q == p

    def _build_project_text(self, project: Dict[str, Any]) -> str:
        category = self._normalize_text(project.get("proposal_category"))
        target = self._normalize_text(project.get("target_group"))
        effect = self._normalize_text(project.get("expected_effect_type"))
        budget = self._normalize_budget_range(project.get("budget_range"))
        phase = self._normalize_text(project.get("project_phase"))

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
        return " ".join(x.strip() for x in parts if x.strip())

    def _build_query_text(self, input_dict: Dict[str, Any]) -> str:
        parts = []
        for field in SEARCH_FIELDS:
            value = self._normalize_text(input_dict.get(field, ""))
            if value:
                if field in ["proposal_category", "target_group", "expected_effect_type"]:
                    parts.extend([value, value])
                else:
                    parts.append(value)
        return " ".join(parts)

    def _field_match_bonus(self, query_value: Any, project_value: Any, exact: float, partial: float) -> float:
        q = self._normalize_text(query_value).lower()
        p = self._normalize_text(project_value).lower()
        if not q or not p:
            return 1.0
        if q == p:
            return exact
        if q in p or p in q:
            return partial
        return 1.0

    def build_index(self, projects: List[dict]):
        if not projects:
            self.projects = []
            self.tfidf_matrix = None
            self.is_fitted = False
            return
        self.projects = projects
        corpus = []
        for project in projects:
            corpus.append(self._build_project_text(project))
        self.tfidf_matrix = self.vectorizer.fit_transform(corpus)
        self.is_fitted = True

    def _calculate_final_score(self, project: Dict[str, Any], base_score: float, input_dict: Dict[str, Any]) -> float:
        score = base_score
        score *= self._field_match_bonus(input_dict.get("proposal_category"), project.get("proposal_category"), exact=1.35, partial=1.15)
        score *= self._field_match_bonus(input_dict.get("target_group"), project.get("target_group"), exact=1.30, partial=1.12)
        score *= self._field_match_bonus(input_dict.get("expected_effect_type"), project.get("expected_effect_type"), exact=1.28, partial=1.10)
        score *= self._field_match_bonus(
            input_dict.get("budget_range"),
            self._normalize_budget_range(project.get("budget_range")),
            exact=1.15,
            partial=1.05,
        )
        score *= self._field_match_bonus(input_dict.get("project_phase"), project.get("project_phase"), exact=1.12, partial=1.04)

        if not self._normalize_text(project.get("ringi_status")):
            score *= 0.97
        if not self._normalize_text(project.get("final_result")):
            score *= 0.97

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
        if not self.is_fitted:
            return []
        query = self._build_query_text(input_dict)
        if not query.strip():
            return []
        query_vec = self.vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, self.tfidf_matrix)[0]

        results = []
        for idx, base_score in enumerate(similarities):
            project = self.projects[idx].copy()

            # 予算レンジが指定されている場合は、合う案件だけ残す
            if not self._budget_matches(
                input_dict.get("budget_range"),
                project.get("budget_range")
            ):
                continue

            final_score = self._calculate_final_score(project, float(base_score), input_dict)

            project["similarity_score"] = round(final_score * 100, 1)
            project["base_score"] = round(float(base_score) * 100, 1)
            results.append(project)

        results.sort(key=lambda x: (x["similarity_score"], x.get("proposal_year", 0)), reverse=True)
        return results[:top_n]


_engine = None

def get_engine() -> SearchEngine:
    global _engine
    if _engine is None:
        _engine = SearchEngine()
    return _engine


def rebuild_index():
    projects_df, members_df, factors_df = load_data()
    project_records = _prepare_project_records(projects_df, members_df, factors_df)
    engine = get_engine()
    engine.build_index(project_records)


def search_projects(input_dict: Dict[str, Any], top_n: int = 20) -> pd.DataFrame:
    engine = get_engine()
    if not engine.is_fitted:
        rebuild_index()

    results = engine.search(input_dict, top_n=top_n)
    ranked_results_df = pd.DataFrame(results)

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