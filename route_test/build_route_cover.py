name: Route Cover (SDE → GH)

on:
  workflow_dispatch:

permissions:
  contents: write

concurrency:
  group: eou_route_cover_sde_to_gh
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 20

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          # Necesario para poder hacer rebase contra origin/main
          fetch-depth: 0

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Build route cover from SDE
        run: |
          python -u route_test/build_route_cover.py

      - name: Commit & push if changed (rebase + retry)
        run: |
          set -euo pipefail

          # Solo cambios dentro de route_test/
          if [ -z "$(git status --porcelain -- route_test/)" ]; then
            echo "No changes."
            exit 0
          fi

          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add -- route_test/route.jsonl route_test/route_names.json route_test/route.meta.json

          # Si no hay cambios staged (por seguridad)
          if git diff --cached --quiet; then
            echo "No staged changes."
            exit 0
          fi

          git commit -m "route_test: rebuild stargate cover route"

          # Rebase contra main remoto para evitar non-fast-forward
          git fetch origin main
          git rebase origin/main

          # Push con reintentos (carreras con otros workflows)
          for i in 1 2 3 4 5; do
            if git push origin HEAD:main; then
              echo "Pushed successfully."
              exit 0
            fi
            echo "Push failed (attempt $i). Retrying after rebase..."
            git fetch origin main
            git rebase origin/main
            sleep $((i * 2))
          done

          echo "Failed to push after retries."
          exit 1
