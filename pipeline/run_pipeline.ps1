# Single script that runs the pipeline. Excludes the load_reference_data.py which can be run manually when needed.

Write-Host "Starting market data pipeline..."

$ProjectRoot = Split-Path $PSScriptRoot -Parent
$BundleRoot = Join-Path $ProjectRoot "databricks"

Write-Host "Validating Databricks bundle..."
Set-Location $BundleRoot

databricks bundle validate

if ($LASTEXITCODE -ne 0) {
    Write-Error "Bundle validation failed."
    exit 1
}

Write-Host "Deploying Databricks bundle..."
databricks bundle deploy

if ($LASTEXITCODE -ne 0) {
    Write-Error "Bundle deployment failed."
    exit 1
}

Write-Host "Starting local market data producer..."
Set-Location $ProjectRoot

python "$ProjectRoot\producer\stream_trades.py"

if ($LASTEXITCODE -ne 0) {
    Write-Error "Market data producer failed."
    exit 1
}