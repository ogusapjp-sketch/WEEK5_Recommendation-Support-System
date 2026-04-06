# =============================================================
# app.py — ヨミガエル（新規事業・業務改善ナレッジ検索アプリ）
# 【サイドバー最適化版】アイコン削除 ＆ 実用機能追加
# =============================================================

import streamlit as st
import pandas as pd
from PIL import Image

# けんちゃんが作成したモジュールをインポート
try:
    from search_engine import search_projects
except ImportError:
    st.error("⚠️ `search_engine.py` が見つからないケロ！確認してほしいケロ。")
    st.stop()

try:
    from llm_advisor import get_kero_advice, summarize_project
except ImportError:
    get_kero_advice = None
    summarize_project = None

# 画像の読み込み
image_path = "yomi-gaeru.png"
try:
    char_image = Image.open(image_path)
except FileNotFoundError:
    char_image = None

# ── ページ設定 ──────────────────────────────────────────────────
st.set_page_config(
    page_title="ヨミガエル | レトロAIナビゲーター",
    page_icon="🐸",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── カスタムCSS ──────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #FFFFFF; }
    div.stButton > button:first-child, div.stFormSubmitButton > button:first-child {
        background-color: #009688 !important;
        color: white !important;
        border: none !important;
        border-radius: 4px;
        font-weight: bold;
    }
    div.stButton > button:first-child:hover, div.stFormSubmitButton > button:first-child:hover {
        background-color: #00796B !important;
    }
    [data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #f0f2f6;
    }
    [data-testid="stSidebar"] [data-testid="stImage"] > img {
        width: 100% !important;
        height: auto !important;
    }
    .advice-box {
        background-color: #e0f2f1;
        padding: 10px;
        border-left: 5px solid #009688;
        border-radius: 4px;
        margin-bottom: 15px;
    }
</style>
""", unsafe_allow_html=True)

# ── サイドバー（ケロ博士の相談室：さらに有効活用） ──────────────────
if char_image:
    st.sidebar.image(char_image, use_container_width=True)
    # ★修正：タイトル横のアイコンを削除
    st.sidebar.title("ケロ博士の相談室")
    st.sidebar.caption("レトロAIナビゲーター「ケロさん」")
    
    st.sidebar.divider()
    
    # 【有効活用：アーカイブ統計】
    st.sidebar.subheader("📊 アーカイブ状況")
    s_col1, s_col2 = st.sidebar.columns(2)
    s_col1.metric("総案件数", "91件")
    s_col2.metric("今月の新着", "5件")
    
    st.sidebar.divider()
    
    # 【新機能：社内ナレッジの鉄則】
    st.sidebar.subheader("✅ 成功への鉄則")
    st.sidebar.markdown("""
    過去の優良案件に共通するポイントだケロ：
    1. **現場の「不」を具体化している**
    2. **スモールスタートで検証している**
    3. **他部署のキーマンを早期に巻き込む**
    """)

    st.sidebar.divider()
    
    # 【新機能：クイックリンク】
    st.sidebar.subheader("🔗 お役立ちリンク")
    st.sidebar.markdown("""
    - [新規事業立案ガイドライン](#)
    - [今期の重点注力ドメイン](#)
    - [おぐさん・けんちゃんへの相談窓口](#)
    """)

# ── ヘッダー ──────────────────────────────────────────────────
st.title("🐸 ナレッジ検索システム「ヨミガエル」")
st.caption("〜 過去のドロ沼を回避し、未来のハスに乗れ！ 〜")
st.divider()

# ── 検索入力フォーム ───────────────────────────────────────────
st.subheader("💾 検索条件をセットするケロ")

st.markdown("""
<div class="advice-box">
    <strong>💡 ケロさんからの検索のコツ：</strong><br>
    最初から全部埋めると、条件が厳しすぎて0件になりやすいケロ！<br>
    まずは<b>「事業のカテゴリ」だけ</b>、あるいは<b>「ターゲット」だけ</b>など、
    少ない条件（1〜2個）からはじめるのがおすすめだケロ。
</div>
""", unsafe_allow_html=True)

with st.form("search_form"):
    col1, col2 = st.columns(2)
    
    with col1:
        category_list = [
            "（指定なし）", "テクノロジー / デジタル事業", "ライフ / ヘルスケア事業", 
            "産業向け（B2B）ソリューション事業", "消費者向けサービス / コンテンツ事業", "社会課題・サステナビリティ事業"
        ]
        category = st.selectbox("📁 事業のカテゴリ", category_list)
        target = st.text_input("🎯 顧客ターゲット", placeholder="例: 経営層、全社従業員など")
        budget_list = [
            "（指定なし）",
            "100万円未満",
            "100万円以上、500万円未満",
            "500万円以上、1000万円未満",
            "1000万円以上"
        ]
        budget = st.selectbox("💰 予算レンジ", budget_list)

    with col2:
        effect_list = ["（指定なし）", "業務効率化", "売上向上", "コスト削減"]
        effect = st.selectbox("📈 事業の方向性", effect_list)
        phase_list = ["（指定なし）", "アイデア・企画段階", "PoC・検証中", "本稼働", "運用・改善"]
        phase = st.selectbox("🏁 プロジェクトフェーズ", phase_list)
        st.write("") 
        submit_btn = st.form_submit_button("💡 ケロさんにアーカイブを探してもらう", use_container_width=True)

# ── session_state 初期化 ───────────────────────────────────────
if "last_results" not in st.session_state:
    st.session_state["last_results"] = None

if "last_input_dict" not in st.session_state:
    st.session_state["last_input_dict"] = None

if "project_summaries" not in st.session_state:
    st.session_state["project_summaries"] = {}

# ── 検索実行 ───────────────────────────────────────────────────
if submit_btn:
    input_dict = {
        "proposal_category": category if category != "（指定なし）" else "",
        "target_group": target,
        "budget_range": budget if budget != "（指定なし）" else "",
        "expected_effect_type": effect if effect != "（指定なし）" else "",
        "project_phase": phase if phase != "（指定なし）" else ""
    }

    if not any(val for val in input_dict.values() if val != ""):
        st.warning("🐸 検索条件をどれか1つでも選んでほしいケロ！")
        st.session_state["last_results"] = None
        st.session_state["last_input_dict"] = None
        st.session_state["project_summaries"] = {}
    else:
        with st.spinner("照合中ケロ..."):
            try:
                df_results = search_projects(input_dict, top_n=20)

                # 検索条件と結果を session_state に保存する
                st.session_state["last_input_dict"] = input_dict
                st.session_state["last_results"] = df_results

                # 新しい検索をしたら、案件サマリーは一度リセットする
                st.session_state["project_summaries"] = {}

            except Exception as e:
                st.error(f"エラーが発生したケロ: {e}")
                st.session_state["last_results"] = None
                st.session_state["last_input_dict"] = None


# ── 検索結果表示 ───────────────────────────────────────────────
df_results = st.session_state.get("last_results")
input_dict = st.session_state.get("last_input_dict")

if df_results is not None:
    if not df_results.empty:
        st.success(f"✅ {len(df_results)} 件見つかったケロ！")
        t1, t2, t3 = st.tabs(["📄 詳細レポート", "📊 ローデータ", "🐸 ケロさんアドバイス"])

        with t1:
            for idx, row in df_results.iterrows():
                title = row.get("project_name", f"アーカイブ #{idx}")
                score = row.get("similarity_score", 0.0)
                project_id = row.get("project_id", idx)

                with st.expander(f"🐸 {title} (類似度: {score}%)"):
                    c1, c2 = st.columns(2)

                    with c1:
                        st.write(f"**カテゴリ:** {row.get('proposal_category', '-')}")
                        st.write(f"**予算:** {row.get('budget_range', '-')}")

                        st.write("**案件サマリー:**")
                        if summarize_project is None:
                            st.write("要約機能は未設定だケロ。")
                        else:
                            if st.button("この案件を要約するケロ", key=f"summary_{project_id}"):
                                with st.spinner("案件を要約中だケロ..."):
                                    summary_text = summarize_project(row.to_dict())
                                st.session_state["project_summaries"][project_id] = summary_text

                            # すでに要約済みなら表示する
                            if project_id in st.session_state["project_summaries"]:
                                st.write(st.session_state["project_summaries"][project_id])

                    with c2:
                        st.info(f"**担当:** {row.get('related_members', '-')}")

                    st.divider()

                    success_text = row.get("success_factors", "")
                    failure_text = row.get("failure_factors", "")

                    if pd.isna(success_text) or str(success_text).strip() == "":
                        success_text = "記録なし"
                    if pd.isna(failure_text) or str(failure_text).strip() == "":
                        failure_text = "記録なし"

                    st.write(f"✅ **成功要因:** {success_text}")
                    st.write(f"⚠️ **失敗要因:** {failure_text}")

        with t2:
            st.dataframe(df_results)

        with t3:
            st.subheader("🐸 ケロさんからのおすすめアドバイス")

            if get_kero_advice is None:
                st.info("LLMアドバイス機能が未設定です。`llm_advisor.py` を配置してください。")
            else:
                with st.spinner("ケロさんが過去案件を分析中ケロ..."):
                    try:
                        advice = get_kero_advice(input_dict, df_results, top_n=20)

                        st.markdown("### ケロさん評価")
                        st.info(advice.get("tone_comment", "コメントなし"))

                        st.markdown("### 稟議に通る確率・成功確率を上げるための戦略")
                        for item in advice.get("strategy", []):
                            st.write(f"- {item}")

                        st.markdown("### アドバイスを求めるべき部署と担当者")
                        ask_people = advice.get("ask_people", [])[:3]
                        if ask_people:
                            for p in ask_people:
                                department = p.get("department", "")
                                person_or_role = p.get("person_or_role", "")
                                reason = p.get("reason", "")
                                st.write(f"- **{department} / {person_or_role}**：{reason}")
                        else:
                            st.write("- 特に推奨候補なし")

                    except Exception as e:
                        st.error(f"ケロさんアドバイス生成中にエラーが発生したケロ: {e}")

    else:
        st.info("🐸 該当なしだケロ。条件を減らして再検索してみてケロ！")