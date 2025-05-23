import json
import jsonlines
from collections import defaultdict
from functools import reduce

from collections import Counter

answer_map = {
    "A": 0,
    "B": 1,
    "C": 2,
    "D": 3,
    "E": 4,
    "F": 5,
}


def find_majority(lst):
    count = Counter(lst)
    n = len(lst)
    for elem, freq in count.items():
        if freq > n // 2:
            return elem
    return None


def eval_pass_of_k(pred_files, gt_file, save_path):
    print("source files", pred_files)
    print("ground truth file", gt_file)
    source_data = []
    for pred_file in pred_files:
        with jsonlines.open(pred_file) as reader:
            file_pred = list(reader)
            source_data.append(file_pred)
            print("len(file_pred)", len(file_pred))
    with jsonlines.open(gt_file) as reader:
        ground_truth_data = list(reader)

    total_count = len(source_data[0])
    print("total_count", total_count)
    correct_count = 0
    all_qids = {}
    pred_qids = []

    for i in range(len(source_data)):
        pred_data = source_data[i]
        pred_map = {}
        for j in range(len(pred_data)):
            pred_map[pred_data[j]["qid"]] = pred_data[j]["answer"]
        pred_qids.append(pred_map)
    for q in ground_truth_data:
        all_qids[q["id"]] = q

    q_ids = list(pred_qids[0].keys())
    set_qids = [set(p.keys()) for p in pred_qids]
    set_qids.append(set(q_ids))
    # get the intersection of all qids since there may be missing some question during the api call.
    intersect_qids = reduce(set.intersection, set_qids)
    intersect_qids = list(intersect_qids)
    print("intersect_qid", len(intersect_qids))

    category_all = defaultdict(int)
    catergory_correct = defaultdict(int)
    difficulty_all = defaultdict(int)
    difficulty_correct = defaultdict(int)

    for q_id in intersect_qids:
        all_preds = set([p[q_id].lower() for p in pred_qids])
        gt = all_qids[q_id]
        gt_answer = gt["answer"]
        difficulty_all[gt["difficulty_level"]] += 1
        for c in gt["medical_topic"]:
            category_all[c] += 1
        if gt_answer.lower() in all_preds:
            difficulty_correct[gt["difficulty_level"]] += 1
            for c in gt["medical_topic"]:
                catergory_correct[c] += 1

            correct_count += 1
    accuracy = correct_count / total_count * 100
    print(f"Accuracy: {accuracy:.2f}%")
    print(f"output_file for eval_pass_of_k: {save_path}")
    with open(save_path, "w") as f:
        f.write(
            f"Accuracy: {accuracy:.2f}%\n"
            f"Category_all: {json.dumps(category_all)}\n"
            f"Category_correct: {json.dumps(catergory_correct)}\n"
            f"Difficulty_all: {json.dumps(difficulty_all)}\n"
            f"Difficulty_correct: {json.dumps(difficulty_correct)}\n"
        )


def eval_ensemble(pred_files, gt_files, save_path):
    print("source files", pred_files)
    print("ground truth files", gt_files)

    source_data = []
    gt_data = []

    # Load predictions
    for pred_file in pred_files:
        with jsonlines.open(pred_file) as reader:
            file_pred = list(reader)
            source_data.append(file_pred)
            print("len(file_pred)", len(file_pred))

    # Load ground truths
    for gt_file in gt_files:
        with jsonlines.open(gt_file) as reader:
            file_gt = list(reader)
            gt_data.append(file_gt)
            print("len(file_gt)", len(file_gt))

    assert len(source_data) == len(gt_data), (
        f"len(source_data) {len(source_data)} not equal to len(gt_data) {len(gt_data)}"
    )

    total_count = len(source_data[0])
    print("total_count", total_count)

    pred_qids = []
    gt_qids = []

    for preds in source_data:
        pred_map = {item["qid"]: item["answer"] for item in preds}
        pred_qids.append(pred_map)

    for gts in gt_data:
        gt_map = {item["id"]: item for item in gts}
        gt_qids.append(gt_map)

    # Get intersection of qids across all prediction and ground truth files
    set_qids = [set(p.keys()) for p in pred_qids] + [set(g.keys()) for g in gt_qids]
    intersect_qids = list(reduce(set.intersection, set_qids))
    print("intersect_qid", len(intersect_qids))

    category_all = defaultdict(int)
    category_correct = defaultdict(int)
    difficulty_all = defaultdict(int)
    difficulty_correct = defaultdict(int)

    correct_count = 0

    for q_id in intersect_qids:
        correct = 0
        for i in range(len(pred_qids)):
            pred_data = pred_qids[i]
            pred_ans = pred_data[q_id].lower()
            gt = gt_qids[i][q_id]
            gt_answer = gt["answer"]
            # print("qid", q_id)
            # print("i", i)
            # print("pred_ans", pred_ans)
            # print("gt_answer", gt_answer)
            if gt_answer.lower() == pred_ans:
                correct += 1

        difficulty_all[gt["difficulty_level"]] += 1
        for c in gt["medical_topic"]:
            category_all[c] += 1
        if correct > len(pred_qids) / 2:
            difficulty_correct[gt["difficulty_level"]] += 1
            for c in gt["medical_topic"]:
                category_correct[c] += 1

            correct_count += 1

    accuracy = correct_count / len(intersect_qids) * 100
    print(f"Accuracy: {accuracy:.2f}%")
    print(f"output_file for ensemble: {save_path}")

    with open(save_path, "w") as f:
        f.write(
            f"Accuracy: {accuracy:.2f}%\n"
            f"Category_all: {json.dumps(category_all)}\n"
            f"Category_correct: {json.dumps(category_correct)}\n"
            f"Difficulty_all: {json.dumps(difficulty_all)}\n"
            f"Difficulty_correct: {json.dumps(difficulty_correct)}\n"
        )

if __name__ == "__main__":
    import os

    pred_file = "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-chat/091f197a-4156-4740-891a-224290ecd45c.jsonl"
    gt_file = (
        "/home/ubuntu/repo/batch_infer_template/raw_data/data-processed-shuffled1.jsonl"
    )
    gt_file = (
        "/home/ubuntu/repo/batch_infer_template/raw_data/data-processed-shuffled0.jsonl"
    )
    pass_of_k_files = [
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/ef869a64-0a1d-40e2-b33d-707d4392d000_epoch_0.jsonl",
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/ef869a64-0a1d-40e2-b33d-707d4392d000_epoch_1.jsonl",
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/ef869a64-0a1d-40e2-b33d-707d4392d000_epoch_2.jsonl",
    ]
    pass_of_1_file = [
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/ef869a64-0a1d-40e2-b33d-707d4392d000_epoch_0.jsonl"
    ]

    # the order of ensemble file must match it corresponding process file
    ensemble_files = [
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/ef869a64-0a1d-40e2-b33d-707d4392d000_epoch_0.jsonl",
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/30672fbf-0671-49b8-8ace-f41d53dfae25_epoch_0.jsonl",
        "/home/ubuntu/repo/batch_infer_template/batch_out/deepseek-reasoner/c0b5f6c2-b445-4766-9cda-cf09431cb0f3_epoch_0.jsonl",
    ]
    ensemble_gt_files = [
        "/home/ubuntu/repo/batch_infer_template/raw_data/data-processed-shuffled0.jsonl",
        "/home/ubuntu/repo/batch_infer_template/raw_data/data-processed-shuffled1.jsonl",
        "/home/ubuntu/repo/batch_infer_template/raw_data/data-processed-shuffled2.jsonl",
    ]
    save_dir = "/home/ubuntu/repo/batch_infer_template/eval_output"
    os.makedirs(save_dir, exist_ok=True)
    save_pass_of_1 = os.path.join(save_dir, "pass_of_1.txt")
    save_path_of_k = os.path.join(save_dir, "pass_of_3.txt")
    save_ensemble = os.path.join(save_dir, "ensemble.txt")
    # eval_pass_of_k(pass_of_1_file, gt_file,save_pass_of_1)
    # eval_pass_of_k(pass_of_k_files, gt_file,save_path_of_k)
    eval_ensemble(ensemble_files, ensemble_gt_files, save_ensemble)
