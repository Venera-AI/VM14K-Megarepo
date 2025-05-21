1. Install requirements

```
pip install -r requirements.txt
```

2. How to run

```
python deduplication.py --input_path PATH_TO_YOUR_INPUT_CSV_FILE
```

Input file must have at least these columns:
- question
- optionA
- optionB
- optionC
- optionD
- optionE
- optionF
- optionG

3. Output
- `dedup_v1.csv` Remove questions that are exact match after normalization
- `dedup_v2.csv` Remove questions that are near match (> 0.9 edit distance)
- `dedup_v3.csv` Remove off-topic questions