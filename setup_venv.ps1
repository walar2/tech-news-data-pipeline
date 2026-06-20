if (!(Test-Path ".venv")) {
    py -m venv .venv
}

.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r Requirements.txt

Write-Host ""
Write-Host "Virtual environment is ready."
Write-Host "Use this interpreter in PyCharm:"
Write-Host "$PWD\.venv\Scripts\python.exe"