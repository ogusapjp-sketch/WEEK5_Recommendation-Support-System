"""
database.py — 案件検索システム
SQLite DB への接続・初期化・CRUD 操作を一元管理する。
対応スキーマ: projects / project_members / project_factors
"""

import sqlite3
from pathlib import Path

DB_PATH     = Path("data/projects.db")
SCHEMA_PATH = Path("schema.sql")


# ── 接続 ────────────────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    """DB 接続を返す。data/ フォルダが無ければ自動作成する。"""
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ── 初期化 ──────────────────────────────────────────────────────────────────

def init_db():
    """schema.sql を読み込んで DB を初期化する（CREATE IF NOT EXISTS なので安全）。"""
    conn = get_connection()
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()


# ── 内部ヘルパー ─────────────────────────────────────────────────────────────

def _insert_members(cur: sqlite3.Cursor, project_id: int, members: list):
    """メンバーリストを project_members へ挿入する（空行はスキップ）。"""
    for m in members:
        if m.get("person_name") or m.get("related_department"):
            cur.execute("""
                INSERT INTO project_members
                    (project_id, related_department, person_name, person_role)
                VALUES (?, ?, ?, ?)
            """, (
                project_id,
                (m.get("related_department") or "").strip(),
                (m.get("person_name")        or "").strip(),
                (m.get("person_role")        or "").strip(),
            ))


def _insert_factors(cur: sqlite3.Cursor, project_id: int, factors: list):
    """要因リストを project_factors へ挿入する（空行・不正タイプはスキップ）。"""
    for f in factors:
        text  = (f.get("factor_text") or "").strip()
        ftype = f.get("factor_type", "success")
        if text and ftype in ("success", "failure"):
            cur.execute("""
                INSERT INTO project_factors (project_id, factor_type, factor_text)
                VALUES (?, ?, ?)
            """, (project_id, ftype, text))


# ── CREATE ──────────────────────────────────────────────────────────────────

def insert_project(project: dict) -> int:
    """
    案件を新規登録する。同じ project_id が存在する場合は上書き更新する。

    Args:
        project: parse_project_html() が返す辞書（members / factors を含む）

    Returns:
        保存した project_id
    """
    conn = get_connection()
    cur  = conn.cursor()
    pid  = project.get("project_id")

    base_vals = (
        project.get("project_name"),
        project.get("proposal_category"),
        project.get("target_group"),
        project.get("budget_range"),
        project.get("expected_effect_type"),
        project.get("project_phase"),
        project.get("proposal_period"),
        project.get("proposal_year"),
        project.get("proposal_department"),
        project.get("project_summary"),
        project.get("ringi_status"),
        project.get("ringi_reason"),
        project.get("implemented_flag"),
        project.get("final_result"),
    )

    if pid:
        cur.execute("SELECT project_id FROM projects WHERE project_id=?", (pid,))
        exists = cur.fetchone()
    else:
        exists = None

    if exists:
        cur.execute("""
            UPDATE projects SET
                project_name=?, proposal_category=?, target_group=?,
                budget_range=?, expected_effect_type=?, project_phase=?,
                proposal_period=?, proposal_year=?, proposal_department=?,
                project_summary=?, ringi_status=?, ringi_reason=?,
                implemented_flag=?, final_result=?
            WHERE project_id=?
        """, (*base_vals, pid))
        cur.execute("DELETE FROM project_members WHERE project_id=?", (pid,))
        cur.execute("DELETE FROM project_factors  WHERE project_id=?", (pid,))
    elif pid:
        cur.execute("""
            INSERT INTO projects
                (project_id, project_name, proposal_category, target_group,
                 budget_range, expected_effect_type, project_phase, proposal_period,
                 proposal_year, proposal_department, project_summary,
                 ringi_status, ringi_reason, implemented_flag, final_result)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (pid, *base_vals))
    else:
        cur.execute("""
            INSERT INTO projects
                (project_name, proposal_category, target_group,
                 budget_range, expected_effect_type, project_phase, proposal_period,
                 proposal_year, proposal_department, project_summary,
                 ringi_status, ringi_reason, implemented_flag, final_result)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, base_vals)
        pid = cur.lastrowid

    _insert_members(cur, pid, project.get("members", []))
    _insert_factors(cur, pid, project.get("factors", []))

    conn.commit()
    conn.close()
    return pid


# ── READ ────────────────────────────────────────────────────────────────────

def get_all_projects() -> list:
    """全案件を project_id 順で取得し、メンバー・要因も付加して返す。"""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SELECT * FROM projects ORDER BY project_id")
    projects = [dict(row) for row in cur.fetchall()]

    for p in projects:
        pid = p["project_id"]
        cur.execute("SELECT * FROM project_members WHERE project_id=?", (pid,))
        p["members"] = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM project_factors  WHERE project_id=?", (pid,))
        p["factors"]  = [dict(r) for r in cur.fetchall()]

    conn.close()
    return projects


def get_project_by_id(project_id: int):
    """指定 ID の案件をメンバー・要因込みで返す。存在しない場合は None。"""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SELECT * FROM projects WHERE project_id=?", (project_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    p = dict(row)
    cur.execute("SELECT * FROM project_members WHERE project_id=?", (project_id,))
    p["members"] = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT * FROM project_factors  WHERE project_id=?", (project_id,))
    p["factors"]  = [dict(r) for r in cur.fetchall()]

    conn.close()
    return p


# ── UPDATE ──────────────────────────────────────────────────────────────────

def update_project(project_id: int, project: dict) -> bool:
    """
    既存案件を更新する。メンバー・要因は一括置き換えする。

    Args:
        project_id: 更新対象の案件 ID
        project   : 更新後のデータ辞書（members / factors を含む）

    Returns:
        True: 更新成功 / False: 対象案件が存在しない
    """
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SELECT project_id FROM projects WHERE project_id=?", (project_id,))
    if not cur.fetchone():
        conn.close()
        return False

    cur.execute("""
        UPDATE projects SET
            project_name=?,           proposal_category=?,    target_group=?,
            budget_range=?,           expected_effect_type=?, project_phase=?,
            proposal_period=?,        proposal_year=?,         proposal_department=?,
            project_summary=?,        ringi_status=?,          ringi_reason=?,
            implemented_flag=?,       final_result=?
        WHERE project_id=?
    """, (
        project.get("project_name"),
        project.get("proposal_category"),
        project.get("target_group"),
        project.get("budget_range"),
        project.get("expected_effect_type"),
        project.get("project_phase"),
        project.get("proposal_period"),
        project.get("proposal_year"),
        project.get("proposal_department"),
        project.get("project_summary"),
        project.get("ringi_status"),
        project.get("ringi_reason"),
        project.get("implemented_flag"),
        project.get("final_result"),
        project_id,
    ))

    # メンバー・要因を一括置き換えする
    cur.execute("DELETE FROM project_members WHERE project_id=?", (project_id,))
    cur.execute("DELETE FROM project_factors  WHERE project_id=?", (project_id,))
    _insert_members(cur, project_id, project.get("members", []))
    _insert_factors(cur, project_id, project.get("factors", []))

    conn.commit()
    conn.close()
    return True


# ── DELETE ──────────────────────────────────────────────────────────────────

def delete_project(project_id: int) -> bool:
    """
    案件を削除する。ON DELETE CASCADE によりメンバー・要因も連動削除される。

    Returns:
        True: 削除成功 / False: 対象が存在しない
    """
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM projects WHERE project_id=?", (project_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


# ── 検索ログ ─────────────────────────────────────────────────────────────────

def log_search(query: str, results_count: int, user_id: str = None) -> int:
    """検索クエリとヒット件数を search_logs テーブルへ記録する。"""
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO search_logs (query, results_count, user_id)
        VALUES (?, ?, ?)
    """, (query, results_count, user_id))
    conn.commit()
    log_id = cur.lastrowid
    conn.close()
    return log_id
