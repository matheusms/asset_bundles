import os
import glob
from ruamel.yaml import YAML
import logging
from databricks.sdk import WorkspaceClient

# Configuração do Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def emailNotification(job, email_correto):
    if 'notifications' in job:
        for notification in job['notifications']:
            # Verifica se já tem o e-mail correto
            if 'email_recipients' in notification:
                # Substitui se for diferente
                if email_correto not in notification['email_recipients']:
                    notification['email_recipients'] = [email_correto]
            else:
                # Se não existir, adiciona
                notification['email_recipients'] = [email_correto]

            # Garante que os alerts estão presentes
            if 'alerts' not in notification:
                notification['alerts'] = [{'on-update-fatal-failure'}]
    else:
        # Cria o bloco completo caso não exista
        job['notifications'] = [{
            'email_recipients': [email_correto],
            'alerts': ['on-update-fatal-failure']
        }]

# Função para processar o arquivo YAML e fazer os replaces
def replace_main(file_path):
    yaml = YAML()
    yaml.preserve_quotes = True
    email_correto = os.environ["EMAIL_DADOS"]   #email correto para notificação de falha
    catalog_dest = os.environ["DTB_CATALOGO"]  #nome do catálogo de destino
    
    with open(file_path, 'r') as file:
        yaml_content = yaml.load(file)

    for job in yaml_content['resources']['pipelines'].values():
        if 'ingestion_definition' in job:
            for obj in job['ingestion_definition']['objects']:
                if 'table' in obj and 'destination_catalog' in obj['table']:
                    obj['table']['destination_catalog'] = catalog_dest
        if 'catalog' in job:
            job['catalog'] = catalog_dest
    
    #verifica se esta com notificação de falha e com emal correto
    emailNotification(job, email_correto)
        

    #salva o yml atualizado para finalizar
    save_yaml(file_path, yaml, yaml_content)

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