import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import glob
import os

# 1. DB（箱）に接続
conn = sqlite3.connect("data/projects.db")
print("💿 おぐさんのデータをDBに流し込むケロ...")

# 2. 成功・失敗要因のCSVをインポート
try:
    factors = pd.read_csv("data/project_factors.csv")
    factors.to_sql("project_factors", conn, if_exists="append", index=False)
    print("✅ project_factors.csv をインポートしたケロ！")
except Exception as e:
    print("⚠️ 要因データのインポートに失敗したケロ:", e)

# 3. HTMLファイルから案件・メンバー・要因を抽出
projects_list = []
members_list = []
factors_list = []   # ★追加：HTMLから要因も拾うためのリスト

# ★修正： "project_*.html" を指定して関係ないHTMLを読み込まないようにする
for file_path in glob.glob("data/project_*.html"):
    with open(file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # ファイル名からIDを抽出 (例: project_005.html -> 5)
    pid_str = os.path.basename(file_path).replace("project_", "").replace(".html", "")

    # ★追加：もしIDが数字じゃなくても、エラーで止まらずにスキップする
    try:
        pid = int(pid_str)
    except ValueError:
        print(f"⚠️ {file_path} は案件データじゃないみたいだからスキップするケロ！")
        continue

    # HTML内の<td>タグからテキストを取得するヘルパー関数
    def get_td(cls):
        td = soup.find("td", class_=cls)
        return td.get_text(strip=True) if td else ""

    # 案件基本情報
    projects_list.append({
        "project_id": pid,
        "project_name": get_td("project-name"),
        "proposal_category": get_td("proposal-category"),
        "business_category": get_td("business-category"),
        "target_group": get_td("target-group"),
        "budget_range": get_td("budget-range"),
        "expected_effect_type": get_td("expected-effect-type"),
        "project_phase": get_td("project-phase"),
        "proposal_period": get_td("proposal-period"),
        "proposal_year": get_td("proposal-year"),
        "proposal_department": get_td("proposal-department"),
        "project_summary": get_td("project-summary"),
        "ringi_status": get_td("ringi-status"),
        "ringi_reason": get_td("ringi-reason"),
        "implemented_flag": get_td("implemented-flag"),
        "final_result": get_td("final-result")
    })

    # 関係メンバー情報
    member_table = soup.select_one("table.project-members")
    if member_table:
        for row in member_table.select("tbody tr"):
            cells = row.find_all("td")
            if len(cells) >= 3:
                members_list.append({
                    "project_id": pid,
                    "related_department": cells[0].get_text(strip=True),
                    "person_name": cells[1].get_text(strip=True),
                    "person_role": cells[2].get_text(strip=True)
                })

    # ★追加：成功要因・失敗要因をHTMLから抽出
    for li in soup.select("ul.project-factors li"):
        factor_type = (li.get("data-factor-type") or "").strip().lower()

        # <strong>success</strong>: のような表示部分を除く
        strong = li.find("strong")
        if strong:
            strong.extract()

        factor_text = li.get_text(strip=True).lstrip(":").strip()

        if factor_type in ("success", "failure") and factor_text:
            factors_list.append({
                "project_id": pid,
                "factor_type": factor_type,
                "factor_text": factor_text
            })

# 4. 抽出したデータをSQLite（箱の中）に書き込む
if projects_list:
    pd.DataFrame(projects_list).to_sql("projects", conn, if_exists="append", index=False)
    print(f"✅ {len(projects_list)}件のHTMLファイルから projects をインポートしたケロ！")

if members_list:
    pd.DataFrame(members_list).to_sql("project_members", conn, if_exists="append", index=False)
    print("✅ HTMLファイルから project_members をインポートしたケロ！")

# ★追加：HTMLから抽出した成功・失敗要因を書き込む
if factors_list:
    pd.DataFrame(factors_list).to_sql("project_factors", conn, if_exists="append", index=False)
    print("✅ HTMLファイルから project_factors をインポートしたケロ！")

conn.close()
print("🐸 すべての流し込みが完了だケロ！これで検索できるはずだケロ！")