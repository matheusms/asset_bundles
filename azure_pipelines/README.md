# Databricks Asset Bundle Deployment Pipelines

This README aims to assist in setting up the DevOps pipelines for deploying the Databricks asset bundle.

## Overview

### `build_deploy_artifact.yml`
The first file, `build_deploy_artifact.yml`, is a YAML pipeline that performs the following tasks:
- Replaces tokens in the `databricks.yml` file.
- Executes `replace_wkf.py` to handle transformations for the workflow `.yml` files.

This file must be located at the same directory level as the `bundle` folder containing all deployment files.

In this YAML pipeline, you need to define the name of the variable group that will be used to replace tokens wherever necessary. In this example, tokens are replaced only in `databricks.yml`.

After replacing tokens and processing the workflow YAML files, the pipeline saves the updated artifact to the Azure Pipelines branch.

### Build Pipeline
Once the `build` pipeline is complete, the next step is to configure the `release` pipeline. 

The `release` pipeline:
1. Reads the artifact created by the `build` pipeline.
2. Configures the environment by installing required libraries.

Additionally, the `release` pipeline must include a step to retrieve credentials (application_id, client_secret, and tenant_id) from the Key Vault. 

> **Note:** These credentials belong to a service principal with permissions to the target Databricks workspace where the deployment will occur.

3. Define Python Version 
   Define the Python version to be used. In this case, Python 3.11.

4. Install Dependencies 
   Add PowerShell steps to install required libraries:  
   ```powershell
   python -m pip install wheel
   python -m pip install setuptools
   python -m pip install pyspark

These commands are stored in the `install_dependencies.ps1` script but are executed directly as inline code in the PowerShell activity during the release pipeline. If you choose to use the `.ps1` script file instead, modifications may be necessary.

5. Authenticate and Configure Databricks CLI
    Add a PowerShell step to:
- Authenticate to Azure using the service principal credentials.
- Install the Databricks CLI.
- Configure the access token to be used.

6. Execute Databricks Bundle Commands
    Add another PowerShell step to:
- Locate the folder containing the `databricks.yml` file and set it as the root directory.
- Execute the following commands:
  ```powershell
  databricks bundle validate - $(env)
  databricks bundle deploy -t $(env)

As with the previous step, these commands are stored in scripts but executed as inline code in the release pipeline.

> **Note:** These PowerShell scripts (`install_dependencies.ps1`, etc.) are stored as `.ps1` files but were not used directly in the release steps. Instead, their contents were passed as inline code. If you choose to use the `.ps1` files, some adjustments to the scripts may be required.