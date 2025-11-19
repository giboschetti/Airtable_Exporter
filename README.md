# Airtable_Exporter

This repository contains the Airtable Exporter project files.

Files included:

- `main.py` — main script
- `requirements.txt` — Python dependencies
- `procfile.txt` — process/runtime info

How to push locally-created repo to GitHub:

1. Install GitHub CLI (`gh`) and authenticate: `gh auth login`.
2. From this folder run: `gh repo create Airtable_Exporter --public --source=. --remote=origin --push`

Or create an empty repository on GitHub in your account named `Airtable_Exporter` and add the remote:

```
git remote add origin https://github.com/<YOUR_USERNAME>/Airtable_Exporter.git
git branch -M main
git push -u origin main
```

Replace `<YOUR_USERNAME>` with your GitHub username.
