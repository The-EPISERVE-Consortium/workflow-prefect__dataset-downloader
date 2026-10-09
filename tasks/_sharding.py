"""QID sharding shared by the tasks that write to lakeFS."""


def shard_qid(qid: str) -> str:
    """Return the sharded directory prefix for a QID using 2-2-2 padding.

    Args:
        qid: Identifier beginning with ``Q`` followed by digits.

    Returns:
        str: Sharded prefix in the form ``pp/qq/rr/Qxxxx``.
    """
    normalized = qid.upper()
    if not normalized.startswith("Q"):
        raise ValueError("QID must start with 'Q'")
    digits = normalized[1:]
    if not digits.isdigit():
        raise ValueError("QID must contain digits after 'Q'")
    padded = digits.zfill(6)
    return f"{padded[0:2]}/{padded[2:4]}/{padded[4:6]}/{normalized}"
