import os
import glob
from ruamel.yaml import YAML
import logging
from databricks.sdk import WorkspaceClient

# Required Environment Variables:
# - DATABRICKS_URL_DEV: URL of the dev workspace (for auth_dtb)
# - ADB_DEV_TENANT, ADB_DEV_APPLICATION, ADB_DEV_SECRET: SP credentials for Dev
# - DATABRICKS_URL: URL of the target workspace (PRD/QA)
# - ADB_TENANT, ADB_APPLICATION, ADB_SECRET: SP credentials for Target
# - <GIT_REPO_NAME_UPPER>: Notebook path prefix map (used in adjust_notebook_path)
# - AMBIENTE: Target environment name ('QA' or other)
# - WEBHOOK_TEAMS_ID: Teams webhook ID for notifications
# - DATABRICKS_CLUSTER_ID_UC: New cluster ID
# - POLICE_JOB_COMPUTE_ID: Policy ID for job compute
# - DATALAKE_STORAGE_NAME: Name of the storage account
# - SERVICE_PRINCIPAL_NAME: SP name for 'run_as'

# Configuração do Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


# Função de autenticação no Databricks
def auth_dtb():
    databricks_host = os.environ["DATABRICKS_URL_DEV"]
    tenant_id = os.environ["ADB_DEV_TENANT"]
    client_id = os.environ["ADB_DEV_APPLICATION"]
    client_secret = os.environ["ADB_DEV_SECRET"]
    
    w = WorkspaceClient(
        host = databricks_host,
        azure_tenant_id = tenant_id,
        azure_client_id = client_id,
        azure_client_secret = client_secret
    )
    return w

# Função de autenticação no Databricks
def auth_dtb_prdqa():
    """
    Função para autenticar no Databricks para ambientes de produção e QA baseado em variáveis de ambiente.
    Essa função foi criada para buscar o pipeline_id/job_id correto para o ambiente de deploy.
    """
    
    databricks_host = os.environ["DATABRICKS_URL"]
    tenant_id = os.environ["ADB_TENANT"]
    client_id = os.environ["ADB_APPLICATION"]
    client_secret = os.environ["ADB_SECRET"]
    
    w = WorkspaceClient(
        host = databricks_host,
        azure_tenant_id = tenant_id,
        azure_client_id = client_id,
        azure_client_secret = client_secret
    )
    return w


# Função para ajustar o notebook_path com base no git_url
def adjust_notebook_path(git_url, notebook_path):
    """Função para ajustar o notebook_path com base no git_url.
    Se o notebook_path começar com 'Notebooks/' ou 'notebooks/', ele será substituído pelo prefixo do caminho do git_url.
    Essa função foi criada para garantir que o desenvolvimento no git do Devops seja mantido no mesmo padrão de deploy."""
    last_segment = git_url.split('/')[-1]
    new_path_prefix = os.environ[last_segment.upper()]
    
    if notebook_path.startswith('Notebooks/'):
        return notebook_path.replace('Notebooks/', new_path_prefix, 1)
    elif notebook_path.startswith('notebooks/'):
        return notebook_path.replace('notebooks/', new_path_prefix, 1)
    return new_path_prefix + notebook_path

#função para buscar o pipeline id do ambiente de deploy e alterar
def alter_pipeId(job):
    for task in job.get('tasks', []):
        if 'pipeline_task' in task and 'pipeline_id' in task['pipeline_task']:
            
            #obtem o id em dev para buscr o nome
            pipeline_id_dev = task['pipeline_task']['pipeline_id']
            
            #busca o nome de acordo com o id em dev
            w = auth_dtb()
            pip_get = w.pipelines.get(pipeline_id=pipeline_id_dev)
            pipe_name = pip_get.name

            #busca o id em prd ou qa
            w2 = auth_dtb_prdqa()
            pipelines_dep = w2.pipelines.list_pipelines()

            #procura o pipeline com base no nome
            for pipe in pipelines_dep:
                ambiente = os.environ["AMBIENTE"]
                if ambiente.lower() == 'qa':
                    pipe_name = f"[QA deploy_devops_qa] {pipe_name}"
                    if pipe.name == pipe_name:
                        pipe_id = pipe.pipeline_id
                else:
                    if pipe.name == pipe_name:
                        pipe_id = pipe.pipeline_id
            
            task['pipeline_task']['pipeline_id'] = pipe_id

# Função para processar o arquivo YAML e fazer os replaces
def replace_main(file_path):
    yaml = YAML()
    yaml.preserve_quotes = True

    webhook_teams = os.environ["WEBHOOK_TEAMS_ID"]
    new_cluster_id = os.environ["DATABRICKS_CLUSTER_ID_UC"]
    
    with open(file_path, 'r') as file:
        yaml_content = yaml.load(file)

    for job in yaml_content['resources']['jobs'].values():
        if 'git_source' in job:
            git_url = job['git_source'].get('git_url', '')
            for task in job.get('tasks', []):
                if 'notebook_task' in task:
                    task['notebook_task']['notebook_path'] = adjust_notebook_path(git_url, task['notebook_task'].get('notebook_path', ''))
                    task['notebook_task']['source'] = 'WORKSPACE'
                if 'existing_cluster_id' in task:
                    task['existing_cluster_id'] = new_cluster_id
                if 'webhook_notifications' in task:
                    for notification in task['webhook_notifications'].get('on_failure', []):
                        notification['id'] = webhook_teams

 
        update_run_job_task(job)
        alter_pipeId(job) 
        update_job_clusters(job)
        update_webhook_notifications(job)
        handle_qa_environment(job)
        update_file_arrival_trigger(job)
        update_service_principal_name(job)

    # Remove git_source e salva alterações
    remove_git_source(yaml_content)
    save_yaml(file_path, yaml, yaml_content)

# Função para atualizar o run_job_task com o job_id correto
def update_run_job_task(job):
    for task in job.get('tasks', []): 
        if isinstance(task, dict) and "run_job_task" in task:
            if isinstance(task["run_job_task"], dict) and "job_id" in task["run_job_task"]:
                job_id_input = task['run_job_task']['job_id']
                w = auth_dtb()
                job_get = w.jobs.get(job_id=job_id_input)
                job_name = job_get.settings.name.replace("-", "_").replace(" ", "_").lower()
                var_job = "${resources.jobs.%.id}"
                replace_var_job = var_job.replace("%", job_name)
                task['run_job_task']['job_id'] = replace_var_job
                logger.info(f"Job_id dev {job_id_input} e job_id de deploy: {replace_var_job}")

# Função para atualizar os job_clusters com o policy_id correto
def update_job_clusters(job):
    if "job_clusters" in job:
        policy_job_compute = os.environ["POLICE_JOB_COMPUTE_ID"]
        for cluster in job["job_clusters"]:
            if "new_cluster" in cluster and "policy_id" in cluster["new_cluster"]:
                cluster["new_cluster"]["policy_id"] = policy_job_compute

# Função para atualizar o webhook_notifications no nível de workflow
def update_webhook_notifications(job):
    if "webhook_notifications" in job:
        webhook_teams = os.environ["WEBHOOK_TEAMS_ID"]
        for notification in job["webhook_notifications"].get("on_failure", []):
            notification["id"] = webhook_teams

# Função para tratar o ambiente QA
def handle_qa_environment(job):
    ambiente = os.environ["AMBIENTE"]
    if ambiente.lower() == 'qa':
        if 'schedule' in job:
            job['schedule'].pop('pause_status', None)
        elif 'trigger' in job:
            job['trigger'].pop('pause_status', None)
        elif 'continuous' in job:
            job['continuous'].pop('pause_status', None)

# Função para atualizar o trigger de file_arrival com o nome correto do datalake
def update_file_arrival_trigger(job):
    if 'trigger' in job and 'file_arrival' in job['trigger']:
        storage_name = os.environ["DATALAKE_STORAGE_NAME"]
        url_stg = job['trigger']['file_arrival']['url']
        job['trigger']['file_arrival']['url'] = url_stg.replace('storagedev', storage_name)

# Função para atualizar o nome do service principal que executará o job
def update_service_principal_name(job):
    service_principal_name = os.environ["SERVICE_PRINCIPAL_NAME"]
    job['run_as'] = {'service_principal_name': service_principal_name}

# Função para remover a chave git_source
def remove_git_source(yaml_content):
    for job in yaml_content['resources']['jobs'].values():
        if 'git_source' in job:
            del job['git_source']

# Função para salvar as alterações no arquivo YAML
def save_yaml(file_path, yaml, yaml_content):
    with open(file_path, 'w') as file:
        yaml.dump(yaml_content, file)
    logger.info(f"{file_path} Alterado com sucesso!")

# Função principal para mapear e processar os arquivos .yml
def process_yml_files(resources_path):
    yml_files = glob.glob(resources_path, recursive=True)
    for file in yml_files:
        logger.info(f"Executando replaces para: {file}")
        replace_main(file)

    logger.info("Processo finalizado!")

# Executa o processo
if __name__ == "__main__":
    ambiente = os.environ["AMBIENTE"]
    logger.info(f"Iniciando replace para o ambiente de {ambiente}")
    process_yml_files("bundle/resources/**/*.yml")