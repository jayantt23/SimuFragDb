#!/usr/bin/env python3

def main():
    exp_path = "expected_output.txt"
    act_path = "output.txt"

    with open(exp_path) as fe, open(act_path) as fa:
        exp = [l.rstrip("\n") for l in fe]
        act = [l.rstrip("\n") for l in fa]

    total = max(len(exp), len(act))
    diffs = 0

    for i in range(total):
        e = exp[i] if i < len(exp) else ""
        a = act[i] if i < len(act) else ""
        if e != a:
            diffs += 1

    matches = total - diffs if total > 0 else 0
    accuracy = (matches / total * 100) if total > 0 else 0.0

    print(f"Total lines: {total}")
    print(f"Differing lines: {diffs}")
    print(f"Matching lines: {matches}")
    print(f"Accuracy: {accuracy:.2f}%")


if __name__ == "__main__":
    main()

