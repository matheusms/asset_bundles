import glob
import re
from ruamel.yaml import YAML

def validate_job_name(job_name):
    errors = []
    
    # Valida se o nome está com o prefixo 'pipe_'
    if not (job_name.startswith('pipe_') or job_name.startswith('wf_')):
        errors.append(f"O nome do Workflow '{job_name}' NÃO começa com 'pipe_'.")
    
    # Verificar se o nome atende ao padrão
    if not re.match(r"^[a-z0-9_]+$", job_name):
        errors.append(f"O nome do job '{job_name}' está fora do padrão.")
    
    return errors

def validate_git_source(job):
    errors = []
    
    # Verificar se o job está vinculado ao DevOps
    if 'git_source' not in job:
        errors.append(f"Workflow não contém DevOps vinculado")
    
    return errors

def validate_task_git_source(job):
    errors = []
    
    # Verificar se os notebooks estão vinculados ao DevOps
    for task in job.get('tasks', []):
        if 'notebook_task' in task and 'GIT' not in task['notebook_task'].get('source', ''):
            errors.append(f"A Task: {task['task_key']} não possui DevOps vinculado")
    
    return errors

def validate_yml(file_path):
    yaml = YAML()
    yaml.preserve_quotes = True  # Preserva aspas, se existirem
    
    with open(file_path, 'r') as file:
        yaml_content = yaml.load(file)
    
    job_errors = []
    
    for job in yaml_content['resources']['jobs'].values():
        job_name = job['name']
        errors = []
        
        # Validação do nome do job
        errors += validate_job_name(job_name)
        
        # Validação do vínculo com DevOps
        #errors += validate_git_source(job)
        
        # Validação das tasks
        errors += validate_task_git_source(job)
        
        if errors:
            job_errors.append({job_name: errors})
    
    return job_errors

def format_errors(errors):
    formatted_errors = ""
    total_errors = 0
    
    for item in errors:
        for key, values in item.items():
            formatted_errors += f"\nErro no workflow: {key}\n"
            for mensagem in values:
                formatted_errors += f"  - {mensagem}\n"
                total_errors += 1
            formatted_errors += ">------------*------------*------------*------------*------------<\n"
    
    return formatted_errors, total_errors

def main():
    resources_path = "bundle/resources/**/*.yml"
    yml_files = glob.glob(resources_path, recursive=True)
    list_erros = []

    for file_path in yml_files:
        job_errors = validate_yml(file_path)
        if job_errors:
            list_erros.extend(job_errors)  # Adiciona erros encontrados ao total

    if not list_erros:
        print("Workflows validados com sucesso!")
    else:
        print("Foram encontrados erros nos seguintes workflows:\n")
        erro_formatado, total_erros = format_errors(list_erros)
        print(erro_formatado)
        
        # Lançar o erro com a quantidade de erros
        raise RuntimeError(f"Foram encontrados erro(s) em {total_erros} Workflow(s).")

if __name__ == "__main__":
    main()