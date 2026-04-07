# =============================================================
# llm_advisor.py
# 検索結果20件をGPTで要約・分析し、「ケロさんアドバイス」を返す
# =============================================================

import json
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from openai import OpenAI

from dotenv import load_dotenv
load_dotenv()

# OpenAIクライアントを初期化する
# APIキーは環境変数 OPENAI_API_KEY から読む想定
client = OpenAI()

# モデルの切り替え　→　5-nanoでは上手くいかず、4o-miniで
LLM_MODEL = "gpt-4o-mini"
# LLM_MODEL = "gpt-5-nano"


def _safe_text(value: Any) -> str:
    # None / NaN を空文字にそろえる
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _clean_json_text(text: str) -> str:
    # ```json ... ``` のようなコードブロックが付いていても読めるようにする
    if not text:
        return ""

    text = text.strip()

    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]

    if text.endswith("```"):
        text = text[:-3]

    return text.strip()


def _build_case_payload(df_results: pd.DataFrame, top_n: int = 20) -> List[Dict[str, Any]]:
    # 検索結果の上位N件を、GPTに渡しやすい辞書リストへ整形する
    if df_results is None or df_results.empty:
        return []

    work = df_results.head(top_n).copy()

    cases = []
    for _, row in work.iterrows():
        cases.append({
            "project_id": row.get("project_id"),
            "project_name": _safe_text(row.get("project_name")),
            "similarity_score": row.get("similarity_score", 0),
            "proposal_category": _safe_text(row.get("proposal_category")),
            "target_group": _safe_text(row.get("target_group")),
            "budget_range": _safe_text(row.get("budget_range")),
            "expected_effect_type": _safe_text(row.get("expected_effect_type")),
            "project_phase": _safe_text(row.get("project_phase")),
            "proposal_department": _safe_text(row.get("proposal_department")),
            "ringi_status": _safe_text(row.get("ringi_status")),
            "ringi_reason": _safe_text(row.get("ringi_reason")),
            "implemented_flag": _safe_text(row.get("implemented_flag")),
            "final_result": _safe_text(row.get("final_result")),
            "related_departments": _safe_text(row.get("related_departments")),
            "related_members": _safe_text(row.get("related_members")),
            "success_factors": _safe_text(row.get("success_factors")),
            "failure_factors": _safe_text(row.get("failure_factors")),
            "project_summary": _safe_text(row.get("project_summary")),
        })
    return cases


def _build_prompt(input_dict: Dict[str, Any], cases: List[Dict[str, Any]]) -> str:
    # 検索条件と検索結果をまとめて、GPTに渡す指示文を作る
    prompt = f"""
あなたは社内案件検索アプリ「ヨミガエル」のアドバイザーAI「ケロさん」です。
以下の検索条件と類似案件20件を分析して、日本語で簡潔かつ実務的に助言してください。

【検索条件】
{json.dumps(input_dict, ensure_ascii=False, indent=2)}

【類似案件】
{json.dumps(cases, ensure_ascii=False, indent=2)}

【分析ルール】
- 類似度が高い案件をより重視してください。
- 「ケロさん評価」では、検索結果に基づいて総合評価を4〜5文のしっかりした記述でまとめてください。成功要因や失敗要因の記述も十分踏まえてください。
- 「ケロさん評価」には、①検索条件のサマリ、②この案件の勝ち筋の有無、③まずどこから着手すべきか、④稟議を通すために補強すべき点、⑤前向きに進めるべきか慎重にすべきか、を自然な文章の流れで含めてください。
- 「ケロさん評価」は、短すぎる一言で終わらせず、作成者が次に何をすべきかが分かるレベルの具体性を持たせてください。
- 「稟議に通る確率、事業の成功確率を上げるための戦略」は5点に整理してください。
- 戦略の各項目は、1項目につき2〜3文で書いてください。
- 戦略の各項目は、1文目で提案内容、2文目でその理由、必要なら3文目で実務上の進め方や注意点を書くようにしてください。
- 戦略は抽象論だけでなく、類似案件の成功・失敗パターンを踏まえた示唆にしてください。
- 「アドバイスを求めるべき部署と担当者」を具体的に整理してください。
- 類似案件に成功が多く、失敗要因が軽い場合は、前向きに励ましてください。
- 類似案件に失敗が多く、稟議却下や実施中止が多い場合は、やんわり撤退や縮小検証を勧めてください。
- ただしデータが少ない、または判断が割れる場合は、無理に背中を押したり撤退を促したりしないでください。
- 必ず、検索結果に基づく推論として書いてください。断定しすぎないでください。
- 出力される文章の語尾は、基本的に「〜ケロ。」でそろえてください。
- 例えば「十分勝ち筋はありそうだケロ。」「まずはPoCで小さく試すのがよさそうだケロ。」のような口調にしてください。
- 出力はJSONのみで返してください。
- JSON本体のみを返してください。```json のコードブロックや前置き説明は不要です。

【出力JSON形式】
{{
  "title": "ケロさんからのおすすめアドバイス",
  "strategy": [
    "戦略1",
    "戦略2",
    "戦略3",
    "戦略4",
    "戦略5"
  ],
  "ask_people": [
    {{
      "department": "部署名",
      "person_or_role": "担当者名または役割",
      "reason": "相談すべき理由"
    }}
  ],
  "tone_comment": "3〜4文の総合評価コメント"
}}
"""
    return prompt.strip()


def _build_project_summary_prompt(project: Dict[str, Any]) -> str:
    """
    1案件分の情報をもとに、GPTへ渡す要約用プロンプトを作る。
    """
    prompt = f"""
あなたは社内案件検索アプリ「ヨミガエル」のアシスタントAI「ケロさん」です。
以下の案件情報をもとに、この案件の要点を日本語で2〜4文程度に簡潔に要約してください。

【要約ルール】
- 何を狙った案件か
- 稟議や実施の結果がどうだったか
- 成功要因または失敗要因で重要な点
を短く整理してください。
- 語尾は「〜ケロ。」でそろえてください。
- 長くなりすぎないでください。
- 出力はプレーンな文章のみで返してください。

【案件情報】
{json.dumps(project, ensure_ascii=False, indent=2)}
"""
    return prompt.strip()


@st.cache_data(show_spinner=False, ttl=3600)
def _summarize_project_cached(project_json: str, model_name: str) -> str:
    """
    同じ案件・同じモデルの要約結果を1時間キャッシュする。
    """
    project = json.loads(project_json)
    prompt = _build_project_summary_prompt(project)

    response = client.responses.create(
        model=model_name,
        input=prompt,
        max_output_tokens=300,
    )

    return response.output_text.strip()


def summarize_project(project_row: dict) -> str:
    """
    app.py から呼び出すための1案件要約関数。
    """
    project = {
        "project_name": _safe_text(project_row.get("project_name")),
        "proposal_department": _safe_text(project_row.get("proposal_department")),
        "proposal_category": _safe_text(project_row.get("proposal_category")),
        "budget_range": _safe_text(project_row.get("budget_range")),
        "project_summary": _safe_text(project_row.get("project_summary")),
        "ringi_status": _safe_text(project_row.get("ringi_status")),
        "ringi_reason": _safe_text(project_row.get("ringi_reason")),
        "implemented_flag": _safe_text(project_row.get("implemented_flag")),
        "final_result": _safe_text(project_row.get("final_result")),
        "success_factors": _safe_text(project_row.get("success_factors")),
        "failure_factors": _safe_text(project_row.get("failure_factors")),
    }

    project_json = json.dumps(project, ensure_ascii=False, sort_keys=True)
    return _summarize_project_cached(project_json, LLM_MODEL)


# 検索時間を短くするためにキャッシュを追加（1時間）
@st.cache_data(show_spinner=False, ttl=3600)
def _get_kero_advice_cached(input_dict_json: str, cases_json: str, model_name: str):
    input_dict = json.loads(input_dict_json)
    cases = json.loads(cases_json)

    prompt = _build_prompt(input_dict, cases)

    response = client.responses.create(
        model=model_name,
        instructions="あなたは慎重で実務的な社内アドバイザーです。出力は必ずJSONのみで返してください。",
        input=prompt,
        max_output_tokens=2000,
    )

    text = response.output_text.strip()
    cleaned_text = _clean_json_text(text)

    try:
        advice = json.loads(cleaned_text)
    except json.JSONDecodeError:
        advice = {
            "title": "ケロさんからのおすすめアドバイス",
            "tone_comment": "JSONとして読めない返答だったケロ。まずは返答文を確認してほしいケロ。",
            "strategy": ["AI出力の解析に失敗したケロ。"],
            "ask_people": []
        }

    advice["ask_people"] = advice.get("ask_people", [])[:3]
    return advice


def get_kero_advice(input_dict: Dict[str, Any], df_results: pd.DataFrame, top_n: int = 20) -> Dict[str, Any]:
    # 検索結果をGPTに渡し、ケロさんアドバイスをJSONで受け取る
    if df_results is None or df_results.empty:
        return {
            "title": "ケロさんからのおすすめアドバイス",
            "strategy": ["検索結果がないため、アドバイスを生成できません。"],
            "ask_people": [],
            "tone_comment": "まずは検索条件を調整して、類似案件を増やしてみてください。"
        }

    cases = _build_case_payload(df_results, top_n=top_n)

    input_dict_json = json.dumps(input_dict, ensure_ascii=False, sort_keys=True)
    cases_json = json.dumps(cases, ensure_ascii=False, sort_keys=True)

    return _get_kero_advice_cached(input_dict_json, cases_json, LLM_MODEL)