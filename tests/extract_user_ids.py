import json

with open("tests/data/test_sample.json", "rb") as f:
    with open("tests/data/user_ids.txt", "w") as out:
        for line in f:
            author_id = json.loads(line)["data"]["author_id"]
            out.write(author_id)
            out.write("\n")
