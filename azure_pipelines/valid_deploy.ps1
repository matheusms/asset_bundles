# Browse the file databricks.yml
Set-Location $(System.DefaultWorkingDirectory)
$file = Get-ChildItem -Path . -Filter "databricks.yml" -Recurse | Select-Object -First 1

if ($file) {
    Write-Host "File found at: $($file.FullName)"
    # Change the directory to where the file is
    $dirPath = Split-Path -Path $file.FullName
    Set-Location -Path $dirPath
    Write-Host "Current directory changed to: $dirPath"
} else {
    Write-Host "Databricks.yml file not found."
}

Write-Host "Validate bundle for $(env) enviroment"
databricks bundle validate -t $(env)
Write-Host "validation completed!"

Write-Host "Deploy job on $(env) enviroment"
databricks bundle deploy -t $(env)
Write-Host "Deploy completed!"