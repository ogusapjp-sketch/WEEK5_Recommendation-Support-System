"""
crawler.py — 案件検索システム
① 一般 Web ページのクロール（fetch_page / parse_html / crawl_url）
② 案件 HTML の構造化パース（parse_project_html）
"""

import re
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup


# ════════════════════════════════════════════════════════════════════════════
# ① 一般 Web クローラー
# ════════════════════════════════════════════════════════════════════════════

def fetch_page(url: str, timeout: int = 10) -> Optional[str]:
    """
    指定 URL の HTML を取得する。

    Args:
        url    : 取得対象 URL
        timeout: タイムアウト秒数

    Returns:
        HTML 文字列。取得失敗時は None。
    """
    try:
        headers = {"User-Agent": "ProjectSearchBot/1.0 (Educational Purpose)"}
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding
        return resp.text
    except requests.RequestException as e:
        print(f"取得エラー: {e}")
        return None


def parse_html(html: str, url: str) -> dict:
    """
    汎用 HTML をパースしてページ情報を返す。

    Args:
        html: HTML 文字列
        url : 元の URL

    Returns:
        ページ情報の辞書
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    title = "No Title"
    if soup.find("title"):
        title = soup.find("title").get_text().strip()
    elif soup.find("h1"):
        title = soup.find("h1").get_text().strip()

    description = ""
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        description = meta["content"][:200]

    keywords = []
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        keywords = [kw.strip() for kw in meta_kw["content"].split(",")][:10]

    elems = soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"])
    full_text = " ".join(e.get_text().strip() for e in elems)
    full_text = re.sub(r"\s+", " ", full_text).strip()

    links = [
        a["href"]
        for a in soup.find_all("a", href=True)
        if a["href"].startswith("http")
    ][:20]

    return {
        "url"         : url,
        "title"       : title,
        "description" : description,
        "keywords"    : keywords,
        "full_text"   : full_text,
        "word_count"  : len(full_text.split()),
        "links"       : links,
        "crawled_at"  : datetime.now().isoformat(),
        "crawl_status": "success",
    }


def crawl_url(url: str) -> dict:
    """URL をクロールしてページ情報を返す（fetch → parse を一括で行う）。"""
    html = fetch_page(url)
    if not html:
        return {"url": url, "crawl_status": "failed", "crawled_at": datetime.now().isoformat()}
    try:
        return parse_html(html, url)
    except Exception:
        return {"url": url, "crawl_status": "error", "crawled_at": datetime.now().isoformat()}


# ════════════════════════════════════════════════════════════════════════════
# ② 案件 HTML パーサー
# ════════════════════════════════════════════════════════════════════════════

def parse_project_html(html: str, filename: str = "") -> dict:
    """
    案件 HTML（project_NNN.html）を解析して構造化データを返す。

    抽出する情報:
        - 案件基本情報（CSS クラスで特定）
        - 関係メンバー（table.project-members の tbody 行）
        - 成功・失敗要因（ul.project-factors の li 要素）

    Args:
        html    : HTML 文字列
        filename: ファイル名（ログ用）

    Returns:
        {
            project_id, project_name, proposal_category, ...,  # 基本情報
            members: [ {related_department, person_name, person_role}, ... ],
            factors: [ {factor_type, factor_text}, ... ],
        }
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── ヘルパー: CSS クラスを持つ最初の <td> のテキストを返す ──────────
    def td(cls: str) -> str:
        el = soup.select_one(f"td.{cls}")
        return el.get_text(strip=True) if el else ""

    # ── ヘルパー: バッジ（span.badge）からテキストを返す ──────────────
    def badge(cls: str) -> str:
        el = soup.select_one(f"span.badge.{cls}")
        return el.get_text(strip=True) if el else ""

    # ── 案件ID ──────────────────────────────────────────────────────────
    project_id: Optional[int] = None
    pid_text = badge("project-id")          # 例: "案件ID: 1"
    m = re.search(r"\d+", pid_text)
    if m:
        project_id = int(m.group())

    # ── 年度 ────────────────────────────────────────────────────────────
    proposal_year: Optional[int] = None
    year_raw = td("proposal-year")          # <td> の場合は数字のみ
    if year_raw.isdigit():
        proposal_year = int(year_raw)
    else:
        m = re.search(r"\d{4}", year_raw)
        if m:
            proposal_year = int(m.group())

    # ── 案件名（h1 優先、なければ td） ─────────────────────────────────
    project_name = ""
    h1 = soup.select_one("h1.project-name")
    if h1:
        project_name = h1.get_text(strip=True)
    if not project_name:
        project_name = td("project-name")

    # ── 基本情報 ────────────────────────────────────────────────────────
    budget_range_raw = td("budget-range")
    budget_range = int(budget_range_raw) if budget_range_raw.isdigit() else None
    
    project: dict = {
        "project_id"          : project_id,
        "project_name"        : project_name,
        "proposal_category"   : td("proposal-category"),
        "target_group"        : td("target-group"),
        "budget_range"        : budget_range,
        "expected_effect_type": td("expected-effect-type"),
        "project_phase"       : td("project-phase"),
        "proposal_period"     : td("proposal-period"),
        "proposal_year"       : proposal_year,
        "proposal_department" : td("proposal-department"),
        "project_summary"     : td("project-summary"),
        "ringi_status"        : td("ringi-status"),
        "ringi_reason"        : td("ringi-reason"),
        "implemented_flag"    : td("implemented-flag"),
        "final_result"        : td("final-result"),
    }

    # ── 関係メンバー ─────────────────────────────────────────────────────
    members: list[dict] = []
    member_table = soup.select_one("table.project-members")
    if member_table:
        for row in member_table.select("tbody tr"):
            cells = row.find_all("td")
            if len(cells) >= 3:
                members.append({
                    "related_department": cells[0].get_text(strip=True),
                    "person_name"       : cells[1].get_text(strip=True),
                    "person_role"       : cells[2].get_text(strip=True),
                })
    project["members"] = members

    # ── 成功・失敗要因 ───────────────────────────────────────────────────
    factors: list[dict] = []
    for li in soup.select("ul.project-factors li"):
        factor_type = li.get("data-factor-type", "").strip()
        # <strong> タグ（"success:" / "failure:" 表示）を除去してテキストを取得
        strong = li.find("strong")
        if strong:
            strong.decompose()
        factor_text = li.get_text(strip=True).lstrip(":").strip()
        if factor_type in ("success", "failure") and factor_text:
            factors.append({
                "factor_type": factor_type,
                "factor_text": factor_text,
            })
    project["factors"] = factors

    return project
