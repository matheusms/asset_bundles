import os
import glob
from ruamel.yaml import YAML
import logging
from databricks.sdk import WorkspaceClient

# Required Environment Variables (Combined):
# - DATABRICKS_URL_DEV, ADB_DEV_TENANT, ADB_DEV_APPLICATION, ADB_DEV_SECRET (for auth_dtb)
# - DATABRICKS_URL, ADB_TENANT, ADB_APPLICATION, ADB_SECRET (for auth_dtb_prdqa)
# - <GIT_REPO_NAME_UPPER> (for adjust_notebook_path)
# - AMBIENTE ('QA' or other)
# - WEBHOOK_TEAMS_ID
# - DATABRICKS_CLUSTER_ID_UC
# - POLICE_JOB_COMPUTE_ID
# - DATALAKE_STORAGE_NAME
# - SERVICE_PRINCIPAL_NAME
# - EMAIL_DADOS (for pipeline notification)
# - DTB_CATALOGO (for pipeline catalog)

# Configuração do Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# --- Common Helpers ---

def save_yaml(file_path, yaml, yaml_content):
    with open(file_path, 'w') as file:
        yaml.dump(yaml_content, file)
    logger.info(f"{file_path} Alterado com sucesso!")

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

# --- Workflow Specific Helpers (from replace_wkf.py) ---

def adjust_notebook_path(git_url, notebook_path):
    """Função para ajustar o notebook_path com base no git_url."""
    last_segment = git_url.split('/')[-1]
    new_path_prefix = os.environ[last_segment.upper()]
    
    if notebook_path.startswith('Notebooks/'):
        return notebook_path.replace('Notebooks/', new_path_prefix, 1)
    elif notebook_path.startswith('notebooks/'):
        return notebook_path.replace('notebooks/', new_path_prefix, 1)
    return new_path_prefix + notebook_path

def alter_pipeId(job):
    for task in job.get('tasks', []):
        if 'pipeline_task' in task and 'pipeline_id' in task['pipeline_task']:
            pipeline_id_dev = task['pipeline_task']['pipeline_id']
            w = auth_dtb()
            pip_get = w.pipelines.get(pipeline_id=pipeline_id_dev)
            pipe_name = pip_get.name

            w2 = auth_dtb_prdqa()
            pipelines_dep = w2.pipelines.list_pipelines()

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

def update_job_clusters(job):
    if "job_clusters" in job:
        policy_job_compute = os.environ["POLICE_JOB_COMPUTE_ID"]
        for cluster in job["job_clusters"]:
            if "new_cluster" in cluster and "policy_id" in cluster["new_cluster"]:
                cluster["new_cluster"]["policy_id"] = policy_job_compute

def update_webhook_notifications(job):
    if "webhook_notifications" in job:
        webhook_teams = os.environ["WEBHOOK_TEAMS_ID"]
        for notification in job["webhook_notifications"].get("on_failure", []):
            notification["id"] = webhook_teams

def handle_qa_environment(job):
    ambiente = os.environ["AMBIENTE"]
    if ambiente.lower() == 'qa':
        if 'schedule' in job:
            job['schedule'].pop('pause_status', None)
        elif 'trigger' in job:
            job['trigger'].pop('pause_status', None)
        elif 'continuous' in job:
            job['continuous'].pop('pause_status', None)

def update_file_arrival_trigger(job):
    if 'trigger' in job and 'file_arrival' in job['trigger']:
        storage_name = os.environ["DATALAKE_STORAGE_NAME"]
        url_stg = job['trigger']['file_arrival']['url']
        job['trigger']['file_arrival']['url'] = url_stg.replace('storagedev', storage_name)

def update_service_principal_name(job):
    service_principal_name = os.environ["SERVICE_PRINCIPAL_NAME"]
    job['run_as'] = {'service_principal_name': service_principal_name}

def remove_git_source(yaml_content):
    for job in yaml_content['resources']['jobs'].values():
        if 'git_source' in job:
            del job['git_source']

# --- Pipeline Specific Helpers (from replace_pip.py) ---

def emailNotification(job, email_correto):
    if 'notifications' in job:
        for notification in job['notifications']:
            if 'email_recipients' in notification:
                if email_correto not in notification['email_recipients']:
                    notification['email_recipients'] = [email_correto]
            else:
                notification['email_recipients'] = [email_correto]
            if 'alerts' not in notification:
                notification['alerts'] = [{'on-update-fatal-failure'}]
    else:
        job['notifications'] = [{
            'email_recipients': [email_correto],
            'alerts': ['on-update-fatal-failure']
        }]

# --- Logic Implementations ---

def process_workflow(file_path):
    logger.info(f"Processing Workflow: {file_path}")
    yaml = YAML()
    yaml.preserve_quotes = True
    
    webhook_teams = os.environ["WEBHOOK_TEAMS_ID"]
    new_cluster_id = os.environ["DATABRICKS_CLUSTER_ID_UC"]
    
    with open(file_path, 'r') as file:
        yaml_content = yaml.load(file)

    if 'resources' in yaml_content and 'jobs' in yaml_content['resources']:
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

        remove_git_source(yaml_content)
        save_yaml(file_path, yaml, yaml_content)

def process_pipeline(file_path):
    logger.info(f"Processing Pipeline: {file_path}")
    yaml = YAML()
    yaml.preserve_quotes = True
    email_correto = os.environ["EMAIL_DADOS"]
    catalog_dest = os.environ["DTB_CATALOGO"]
    
    with open(file_path, 'r') as file:
        yaml_content = yaml.load(file)

    if 'resources' in yaml_content and 'pipelines' in yaml_content['resources']:
        for job in yaml_content['resources']['pipelines'].values():
            if 'ingestion_definition' in job:
                for obj in job['ingestion_definition']['objects']:
                    if 'table' in obj and 'destination_catalog' in obj['table']:
                        obj['table']['destination_catalog'] = catalog_dest
            if 'catalog' in job:
                job['catalog'] = catalog_dest
        
            emailNotification(job, email_correto)
        
        save_yaml(file_path, yaml, yaml_content)

def main():
    ambiente = os.environ["AMBIENTE"]
    logger.info(f"Iniciando replace unificado para o ambiente de {ambiente}")
    
    resources_path = "bundle/resources/**/*.yml"
    yml_files = glob.glob(resources_path, recursive=True)
    
    for file_path in yml_files:
        # Normalize path separators for consistent checking
        file_path_norm = file_path.replace('\\', '/')
        
        if '/pipelines/' in file_path_norm:
            process_pipeline(file_path)
        elif '/workflows/' in file_path_norm:
            process_workflow(file_path)
        elif '/dashboards/' in file_path_norm:
            logger.info(f"Skipping Dashboard: {file_path}")
        else:
            logger.warning(f"File found in unknown resource folder (not pipelines, workflows, or dashboards): {file_path}")

if __name__ == "__main__":
    main()
