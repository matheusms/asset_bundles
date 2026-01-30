# Authenticate to Azure using Service Principal
Write-Host "Autenticando no Azure..."
az login --service-principal --username $(adb-application) --password $(adb-secret) --tenant $(adb-tenant)

# Getting access token
Write-Host "Getting access token..."
$databricksToken = az account get-access-token --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" --query "accessToken" -o tsv

# Sets the variable in the Azure DevOps pipeline
Write-Host "##vso[task.setvariable variable=DATABRICKS_TOKEN]$databricksToken"

# Display the token (optional, for testing only)
Write-Host "Using the token... (just test, you can remove)"
Write-Host $databricksToken

###################################################
# Installing the Databricks CLI
Write-Host "Installing the Databricks CLI..."

# Downloading and installing the Databricks CLI
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh" -OutFile "install.sh"
bash ./install.sh

# Configuring the Databricks CLI manually
Write-Host "Configuring the Databricks CLI manually..."

# Setting the environment variables
$databricksHost = $env:DATABRICKS_WORKSPACE_URL
$databricksToken = $env:DATABRICKS_TOKEN

# Creating the configuration file for the Databricks CLI (~/.databrickscfg)
$databricksConfig = @"
[DEFAULT]
host = $databricksHost
token = $databricksToken
"@

# Saving the configuration file to the correct path
$databricksConfigPath = [System.IO.Path]::Combine($env:HOME, ".databrickscfg")
$databricksConfig | Out-File -FilePath $databricksConfigPath -Force

Write-Host "Successfully configured Databricks CLI."