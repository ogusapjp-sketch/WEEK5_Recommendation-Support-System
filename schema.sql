PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects (
    project_id            INTEGER PRIMARY KEY,
    project_name          TEXT NOT NULL,
    proposal_category     TEXT,
    target_group          TEXT,
    budget_range          INTEGER,
    expected_effect_type  TEXT,
    project_phase         TEXT,
    proposal_period       TEXT,
    proposal_year         INTEGER NOT NULL
    proposal_department   TEXT NOT NULL,
    project_summary       TEXT,
    ringi_status          TEXT NOT NULL,
    ringi_reason          TEXT,
    implemented_flag      TEXT NOT NULL
    final_result          TEXT
);

CREATE TABLE IF NOT EXISTS project_members (
    member_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id            INTEGER NOT NULL,
    related_department    TEXT,
    person_name           TEXT,
    person_role           TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS project_factors (
    factor_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id            INTEGER NOT NULL,
    factor_type           TEXT NOT NULL CHECK (factor_type IN ('success', 'failure')),
    factor_text           TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS keywords (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id            INTEGER NOT NULL,
    keyword               TEXT NOT NULL,
    tf_score              REAL DEFAULT 0.0,
    tfidf_score           REAL DEFAULT 0.0,
    FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS search_logs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    query                 TEXT NOT NULL,
    results_count         INTEGER DEFAULT 0,
    user_id               TEXT,
    searched_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS click_logs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    search_log_id         INTEGER,
    project_id            INTEGER NOT NULL,
    position              INTEGER,
    clicked_at            DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (search_log_id) REFERENCES search_logs(id) ON DELETE SET NULL,
    FOREIGN KEY (project_id)    REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_members_project_id ON project_members(project_id);
CREATE INDEX IF NOT EXISTS idx_project_factors_project_id ON project_factors(project_id);
CREATE INDEX IF NOT EXISTS idx_projects_proposal_year    ON projects(proposal_year);
CREATE INDEX IF NOT EXISTS idx_projects_ringi_status     ON projects(ringi_status);
CREATE INDEX IF NOT EXISTS idx_keywords_keyword          ON keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_keywords_project_id       ON keywords(project_id);
CREATE INDEX IF NOT EXISTS idx_search_logs_query         ON search_logs(query);
CREATE INDEX IF NOT EXISTS idx_search_logs_searched_at   ON search_logs(searched_at);
CREATE INDEX IF NOT EXISTS idx_click_logs_search_log_id  ON click_logs(search_log_id);
CREATE INDEX IF NOT EXISTS idx_click_logs_project_id     ON click_logs(project_id);
