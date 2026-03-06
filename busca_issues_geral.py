import csv
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIGURAÇÃO
GITLAB_URL = "https://gitlab.4mti.com.br"
TOKEN = "SEU_TOKEN"  # substitua pelo seu token
GROUP_ID = 94
PER_PAGE = 100
MAX_WORKERS = 6
HEADERS = {"PRIVATE-TOKEN": TOKEN}

CSV_FILE = "issues_sem_fechamento_detalhado.csv"
JSON_FILE = "resumo_issues_sem_fechamento.json"

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
    r = requests.get(url, headers=HEADERS, params=params)
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

def get_projects():
    data = paginated_request(f"{GITLAB_URL}/api/v4/groups/{GROUP_ID}/projects", {"include_subgroups": True})
    return {p["id"]: p["name"] for p in data if p["name"] in PROJETOS_ATIVOS}

def get_project_issues(project_id, project_name):
    # Puxar todas, vamos filtrar depois
    params = {"state": "all"}
    data = paginated_request(f"{GITLAB_URL}/api/v4/projects/{project_id}/issues", params)
    for i in data:
        i["project_name"] = project_name
    return data

def get_all_issues(projetos):
    issues = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_project_issues, pid, name): name for pid, name in projetos.items()}
        for future in as_completed(futures):
            issues.extend(future.result())
    return issues

# =============================
# FILTRO: sem data de fechamento
# =============================
def filtrar_sem_fechamento(issues):
    return [i for i in issues if not i.get("closed_at")]

# =============================
# SALVAR CSV DETALHADO
# =============================
def salvar_csv(issues):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "projeto", "titulo", "author", "estado", "labels", "url"
        ])
        for i in issues:
            author = (i.get("author") or {}).get("name", "Sem autor")
            writer.writerow([
                i.get("id"),
                i.get("project_name"),
                i.get("title"),
                author,
                i.get("state"),
                ", ".join(i.get("labels", [])),
                i.get("web_url")
            ])
    print(f"CSV detalhado gerado: {CSV_FILE}")

# =============================
# GERAR JSON RESUMO
# =============================
def gerar_json_resumo(issues):
    resumo = {}
    total_geral = 0
    for i in issues:
        projeto = i.get("project_name")
        resumo[projeto] = resumo.get(projeto, 0) + 1
        total_geral += 1
    resumo["total_geral"] = total_geral
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(resumo, f, indent=4)
    print(f"JSON resumo gerado: {JSON_FILE}")

# =============================
# MAIN
# =============================
def main():
    print("Buscando projetos...")
    projetos = get_projects()
    print(f"{len(projetos)} projetos ativos encontrados")

    print("Buscando todas as issues...")
    issues = get_all_issues(projetos)
    print(f"{len(issues)} issues encontradas")

    sem_fechamento = filtrar_sem_fechamento(issues)
    print(f"{len(sem_fechamento)} issues sem data de fechamento")

    salvar_csv(sem_fechamento)
    gerar_json_resumo(sem_fechamento)

if __name__ == "__main__":
    main()