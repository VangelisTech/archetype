def unique(values):
    seen = []
    for v in values:
        if v not in seen:
            seen.append(v)
    return seen