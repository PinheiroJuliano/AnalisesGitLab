import os
import csv
import requests
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================
# CONFIG
# =============================

DATA_INICIO = (date.today() - timedelta(days=1500)).isoformat()
DATA_FIM = date.today().isoformat()

LABELS_EXCLUIR = {"Ready", "Specification"}

GITLAB_URL = "https://gitlab.4mti.com.br"
TOKEN = "MEU_TOKEN_AQUI"

GROUP_ID = 94

PER_PAGE = 100
TIMEOUT = 20
MAX_WORKERS = 6

HEADERS = {"PRIVATE-TOKEN": TOKEN}

HOJE = date.today()
HOJE_STR = HOJE.isoformat()

CSV_PATH = r"C:\Users\4mti\Documents\BI Acompanhamento\csv"
ARQUIVO_BASE = "issues_base.csv"

PROJETOS_ATIVOS = {
    "Gaspar",
    "cotz",
    "consulta_gsi",
    "ds_python",
    "carga_precatorios",
    "DevOps",
    "enforce-ingestao-canonicalizer",
}

# =================================
# REQUEST
# =================================

def request_gitlab(url, params=None):

    r = requests.get(
        url,
        headers=HEADERS,
        params=params,
        timeout=TIMEOUT
    )

    r.raise_for_status()

    return r.json()


# =================================
# PAGINAÇÃO
# =================================

def paginated_request(url, params=None):

    page = 1
    results = []

    while True:

        p = dict(params or {})
        p.update({"per_page": PER_PAGE, "page": page})

        data = request_gitlab(url, p)

        if not data:
            break

        results.extend(data)
        page += 1

    return results


# =================================
# PROJETOS
# =================================

def get_projects():

    print("🔍 Buscando projetos...")

    data = paginated_request(
        f"{GITLAB_URL}/api/v4/groups/{GROUP_ID}/projects",
        {"include_subgroups": True}
    )

    filtrados = {
        p["id"]: p["name"]
        for p in data
        if p["name"] in PROJETOS_ATIVOS
    }

    print(f"✅ {len(filtrados)} projetos ativos encontrados")

    return filtrados


# =================================
# ISSUES DE UM PROJETO
# =================================

def get_project_issues(project_id, nome):

    params = {
        "state": "opened",
        "created_after": DATA_INICIO,
        "created_before": DATA_FIM
    }

    data = paginated_request(
        f"{GITLAB_URL}/api/v4/projects/{project_id}/issues",
        params
    )

    issues = []

    for i in data:

        labels = set(i.get("labels", []))

        if labels.intersection(LABELS_EXCLUIR):
            continue

        i["project_name"] = nome
        issues.append(i)

    print(f"📦 {nome}: {len(issues)} issues")

    return issues


# =================================
# ISSUES TODOS PROJETOS
# =================================

def get_open_issues(projetos):

    issues = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {
            executor.submit(get_project_issues, pid, nome): nome
            for pid, nome in projetos.items()
        }

        for future in as_completed(futures):

            issues.extend(future.result())

    return issues


# =================================
# ATRASO
# =================================

def calcula_atraso(due_date_str):

    if not due_date_str:
        return None, None

    due = datetime.strptime(due_date_str, "%Y-%m-%d").date()

    dias = (HOJE - due).days

    if dias > 0:
        return "Sim", dias

    return "Não", 0


# =================================
# GERAR LINHA CSV
# =================================

def gerar_linha_issue(issue):

    atrasada, dias = calcula_atraso(issue.get("due_date"))

    return [
        HOJE_STR,
        issue.get("id"),
        issue.get("iid"),
        issue.get("project_name"),
        issue.get("title"),
        (issue.get("author") or {}).get("name") or "",
        (issue.get("assignee") or {}).get("name") or "",
        issue.get("created_at"),
        issue.get("due_date"),
        atrasada,
        dias,
        ", ".join(issue.get("labels", [])),
        issue.get("state"),
        issue.get("web_url")
    ]


# =================================
# SALVAR BASE (RECRIANDO ARQUIVO)
# =================================

def salvar_base(issues):

    print("💾 Recriando base de issues...")

    os.makedirs(CSV_PATH, exist_ok=True)

    arquivo = os.path.join(CSV_PATH, ARQUIVO_BASE)

    with open(arquivo, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)

        writer.writerow([
            "data_snapshot",
            "issue_id",
            "iid",
            "projeto",
            "titulo",
            "autor",
            "responsavel",
            "criacao",
            "data_planejada",
            "esta_atrasada",
            "dias_atraso",
            "labels",
            "state",
            "url"
        ])

        for issue in issues:

            writer.writerow(gerar_linha_issue(issue))

    print(f"✅ Base recriada com {len(issues)} issues")
    print(f"📄 Arquivo: {arquivo}")


# =================================
# MAIN
# =================================

def main():

    if not TOKEN:
        raise Exception("Defina TOKEN")

    projetos = get_projects()

    print("📥 Buscando backlog...")

    issues = get_open_issues(projetos)

    print(f"✅ {len(issues)} issues coletadas")

    salvar_base(issues)

    print("🎉 Processo finalizado")


if __name__ == "__main__":
    main()