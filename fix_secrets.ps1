$content = Get-Content config\airflow.cfg -Raw
$content = $content -replace 'fernet_key = JYz-hzmGt2CoBFTAoM2mBy9mACEtgW2VWWynn8j40Wc=', 'fernet_key = jtFYbhNC_3dVTsn6W4NdY-RkVpbMT1DVKbo8EbEAYz8='
$content = $content -replace 'internal_api_secret_key = xWt4SnKFdZGUZBH/rYk6zA==', 'internal_api_secret_key = nPoJmgGwqJYCffcYTaozYw=='
$content = $content -replace 'secret_key = xWt4SnKFdZGUZBH/rYk6zA==', 'secret_key = nPoJmgGwqJYCffcYTaozYw=='
$content = $content -replace 'jwt_secret = gpN6sFJFEXkKmROGQJfhaw==', 'jwt_secret = Z5PuT9gJT23f4fj6VHfg4g=='
Set-Content config\airflow.cfg -Value $content -NoNewline
Write-Host "Done"