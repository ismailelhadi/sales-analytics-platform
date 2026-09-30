#!/usr/bin/env bash
# Reproduce everything from scratch: data -> load + checks -> dbt -> RAG index -> evaluations.
set -euo pipefail
cd "$(dirname "$0")"

echo "== 1. Reset database"
psql -v ON_ERROR_STOP=1 -q -c "drop schema if exists raw, ops, staging, marts, rag cascade; create extension if not exists vector;"

echo "== 2. Generate synthetic multi-source data"
rm -rf data/raw data/quarantine
python data_gen/generate.py

echo "== 3. Validate and load 180 days x 2 sources"
start=$(date +%s)
python -m pipeline.extract_load --backfill 2>&1 | grep -E "ERROR|processed" || true
echo "load time: $(( $(date +%s) - start ))s"

echo "== 4. dbt build (models + tests)"
(cd dbt && dbt build --profiles-dir . 2>&1 | grep -E "Done\.|ERROR|FAIL" )

echo "== 5. Build RAG index + ground truth"
python -m rag.index
python -m rag.eval.build_ground_truth

echo "== 6. Evaluate"
python -m pipeline.evaluate > /dev/null
python -m rag.eval.run_eval > /dev/null
pytest -q
echo "Results written to results/"
