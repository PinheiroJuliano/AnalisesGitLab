import csv
import requests
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================
# CONFIGURAÇÃO
# =============================

GITLAB_URL = "https://gitlab.4mti.com.br"
TOKEN = "chave-privada-gitlab"  # substitua pelo seu token
GROUP_ID = 94

DATA_INICIO = "2026-02-01"
DATA_FIM = "2026-02-28"

PER_PAGE = 100
TIMEOUT = 20
MAX_WORKERS = 6

HEADERS = {"PRIVATE-TOKEN": TOKEN}

ARQUIVO = "relatorio_fevereiro_detalhado.csv"

HOJE = date.today()

PROJETOS_ATIVOS = {
    "Gaspar",
    "cotz",
    "consulta_gsi",
    "ds_python",
    "carga_precatorios",
    "DevOps",
    "enforce-ingestao-canonicalizer",
}

# =============================
# FUNÇÕES DE REQUISIÇÃO
# =============================

def request_gitlab(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

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

# =============================
# PROJETOS
# =============================

def get_projects():
    data = paginated_request(
        f"{GITLAB_URL}/api/v4/groups/{GROUP_ID}/projects",
        {"include_subgroups": True}
    )
    return {p["id"]: p["name"] for p in data if p["name"] in PROJETOS_ATIVOS}

# =============================
# ISSUES DE UM PROJETO
# =============================

def get_project_issues(project_id, project_name):
    params = {
        "state": "all",
        "created_after": DATA_INICIO,
        "created_before": DATA_FIM,
        "closed_after": DATA_INICIO,
        "closed_before": DATA_FIM,
    }
    data = paginated_request(f"{GITLAB_URL}/api/v4/projects/{project_id}/issues", params)
    for i in data:
        i["project_name"] = project_name
    return data

# =============================
# TODAS ISSUES
# =============================

def get_all_issues(projetos):
    issues = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_project_issues, pid, name): name for pid, name in projetos.items()}
        for future in as_completed(futures):
            issues.extend(future.result())
    return issues

# =============================
# MARCAR ATRASO
# =============================

def calcula_atraso(due_date_str, estado):
    if not due_date_str:
        return "Não", 0
    due_date = datetime.strptime(due_date_str, "%Y-%m-%d").date()
    dias = (HOJE - due_date).days
    if dias > 0 and estado != "closed":
        return "Sim", dias
    return "Não", 0

# =============================
# SALVAR CSV DETALHADO
# =============================

def salvar_csv_detalhado(issues):
    with open(ARQUIVO, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "author",
            "projeto",
            "titulo",
            "estado",
            "criacao",
            "fechamento",
            "data_planejada",
            "esta_atrasada",
            "dias_atraso",
            "labels",
            "url"
        ])
        for i in issues:
            author = (i.get("author") or {}).get("name", "Sem autor")
            estado = i.get("state")
            criada = i.get("created_at")[:10]
            fechada = i.get("closed_at")[:10] if i.get("closed_at") else ""
            due = i.get("due_date") or ""
            atrasada, dias = calcula_atraso(due, estado)
            labels = ", ".join(i.get("labels", []))
            writer.writerow([
                author,
                i.get("project_name"),
                i.get("title"),
                estado,
                criada,
                fechada,
                due,
                atrasada,
                dias,
                labels,
                i.get("web_url")
            ])
    print(f"CSV detalhado gerado: {ARQUIVO}")

# =============================
# MAIN
# =============================

def main():
    print("Buscando projetos...")
    projetos = get_projects()
    print(f"{len(projetos)} projetos ativos encontrados")

    print("Buscando issues...")
    issues = get_all_issues(projetos)
    print(f"{len(issues)} issues encontradas")

    salvar_csv_detalhado(issues)

if __name__ == "__main__":
    main()