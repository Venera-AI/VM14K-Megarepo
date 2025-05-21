import pandas as pd
import numpy as np

import os
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer

from tqdm.auto import tqdm

import re
from thefuzz import fuzz, process

from dedup_utils import normalize_vietnamese, rm_cau
import argparse

parser = argparse.ArgumentParser(description="Process an input file path.")
parser.add_argument("--input_path", help="Path to the input file")

args = parser.parse_args()

dataset = pd.read_csv(args.input_path)

df = dataset.reset_index(drop=True).copy()

# Normalize the text, including removing accents, special characters, and 
# converting to lowercase
df["q"] = df["question"].apply(normalize_vietnamese)
df["oa"] = df["optionA"].apply(normalize_vietnamese)
df["ob"] = df["optionB"].apply(normalize_vietnamese)
df["oc"] = df["optionC"].apply(normalize_vietnamese)
df["od"] = df["optionD"].apply(normalize_vietnamese)
df["oe"] = df["optionE"].apply(normalize_vietnamese)
df["of"] = df["optionF"].apply(normalize_vietnamese)
df["og"] = df["optionG"].apply(normalize_vietnamese)

# Convert to TF-IDF vector to find exact and near duplicates
vectorizer = TfidfVectorizer(
    analyzer="word",
    lowercase=True,
    stop_words=None,
    # max_features=100_000,
    ngram_range=(1, 1),
    min_df=5,
    max_df=0.8,
)

X = vectorizer.fit_transform(df["q"])
features = vectorizer.get_feature_names_out()

# Look for duplicates, epsilon1 as threshold for exact match, epsilon2 as 
# threshold for near duplicates
epsilon1 = 1e-8
epsilon2 = 0.1
all_dup_idx_pairs = []
all_near_dup_idx_pairs = []
for i in tqdm(range(X.shape[0] // 2000 + 1)):
    idx_start = i * 2000
    dist_matrix = 1 - X[idx_start:idx_start + 2000, :].dot(X.T).toarray()
    dup_idx_pairs_tmp = np.argwhere(dist_matrix < epsilon1)
    dup_idx_pairs = dup_idx_pairs_tmp + [[idx_start, 0]] * len(dup_idx_pairs_tmp)
    all_dup_idx_pairs.append(dup_idx_pairs)
    
    near_dup_idx_pairs_tmp = np.argwhere((dist_matrix < epsilon2) & (dist_matrix >= epsilon1))
    near_dup_idx_pairs = near_dup_idx_pairs_tmp + [[idx_start, 0]] * len(near_dup_idx_pairs_tmp)
    all_near_dup_idx_pairs.append(near_dup_idx_pairs)

all_dup_idx_pairs = np.concatenate(all_dup_idx_pairs, axis=0)
all_dup_idx_pairs = all_dup_idx_pairs[all_dup_idx_pairs[:, 0] != all_dup_idx_pairs[:, 1]]

all_near_dup_idx_pairs = np.concatenate(all_near_dup_idx_pairs, axis=0)
all_near_dup_idx_pairs = all_near_dup_idx_pairs[all_near_dup_idx_pairs[:, 0] != all_near_dup_idx_pairs[:, 1]]

all_dup_idx_pairs = np.unique(all_dup_idx_pairs, axis=0)
all_dup_idx_pairs = all_dup_idx_pairs[all_dup_idx_pairs[:, 0] < all_dup_idx_pairs[:, 1]]


# Remove questions of Exact match
val_array = df[["q", "oa", "ob", "oc", "od", "oe", "of", "og"]].fillna('').values
removed_idx = set()

for i, j in all_dup_idx_pairs:
    if i in removed_idx or j in removed_idx:
        continue
    if (val_array[i,0] == val_array[j,0]) and sorted(val_array[i,1:]) == sorted(val_array[j,1:]):
        removed_idx.add(j)
    

FILENAME_DEDUP_V1 = "dedup_v1.csv"
df[~df.index.isin(removed_idx)].reset_index(drop=True).to_csv(f"./{FILENAME_DEDUP_V1}", index=False)


# Remove questions of Near match
data = []
for i, j in all_near_dup_idx_pairs:
    if i in removed_idx or j in removed_idx:
        continue
    fuzz_ratio = fuzz.ratio(rm_cau(val_array[i,0]), rm_cau(val_array[j,0]))
    same_answers = sorted(val_array[i,1:]) == sorted(val_array[j,1:])
    i_in_j = val_array[i,0] in val_array[j,0]
    j_in_i = val_array[j,0] in val_array[i,0]

    data.append((i, j, fuzz_ratio, same_answers, i_in_j, j_in_i, val_array[i,0], val_array[j,0], ", ".join(sorted(val_array[i,1:])), ", ".join(sorted(val_array[j,1:]))))
df_near_dup = pd.DataFrame(data, columns=["i", "j", "fuzz_ratio", "same_answers", "i_in_j", "j_in_i", "q_i", "q_j", "options_i", "options_j"])

# Pick 90 as the threshold
removed_idx_fuzz = set()
for (i, j, fuzz_ratio, same_answers, i_in_j, j_in_i, q_i, q_j, options_i, options_j) in data:
    if i in removed_idx_fuzz or j in removed_idx_fuzz:
        continue
    if fuzz_ratio >= 90 and same_answers:
        removed_idx_fuzz.add(j)
FILENAME_DEDUP_V2 = "dedup_v2.csv"
df[
    ~(df.index.isin(removed_idx) | df.index.isin(removed_idx_fuzz))
    ].reset_index(drop=True).to_csv(
        f"./{FILENAME_DEDUP_V2}", index=False)


df_dedup_v2 = pd.read_csv(f"./{FILENAME_DEDUP_V2}")

df_dedup_v2[
    (df_dedup_v2["medicalTopic"] != "Other(No Category)")
].reset_index(drop=True).reset_index(drop=False).rename(
    columns={"index": "dedup_v3_id"}
).to_csv(
    "./dedup_v3.csv", index=False
)

