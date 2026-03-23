import daft

from examples.synth.pair_generator import generate_triplets


def test_generate_triplets_from_labels():
    segments = [
        {"segment__content": f"msg {i}", "perspective__lens": lens, "voice__classification": voice}
        for i, (lens, voice) in enumerate([
            ("objective", "actor"), ("objective", "actor"),
            ("subjective", "observer"), ("subjective", "observer"),
            ("abjective", "actor"), ("abjective", "observed"),
        ])
    ]
    df = daft.from_pydict({k: [s[k] for s in segments] for k in segments[0]})
    triplets = generate_triplets(df, label_col="perspective__lens", min_per_group=2)
    rows = triplets.collect().to_pylist()
    assert len(rows) > 0
    for row in rows:
        assert row["anchor_text"] != ""
        assert row["positive_text"] != ""
        assert row["negative_text"] != ""
        assert row["label_source"] == "perspective__lens"


def test_generate_triplets_skips_small_groups():
    segments = [
        {"segment__content": "msg 0", "voice__classification": "actor"},
        {"segment__content": "msg 1", "voice__classification": "observer"},
    ]
    df = daft.from_pydict({k: [s[k] for s in segments] for k in segments[0]})
    triplets = generate_triplets(df, label_col="voice__classification", min_per_group=2)
    rows = triplets.collect().to_pylist()
    assert len(rows) == 0
