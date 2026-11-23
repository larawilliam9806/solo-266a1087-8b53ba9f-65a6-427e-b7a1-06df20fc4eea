"""领域常量：高度指标与输出列定义。"""

# (指标键, 中文名, 输出 CSV 列名)
HEIGHT_METRICS = (
    ("tree_height", "树高", "tree_height_m"),
    ("under_branch_height", "枝下高", "under_branch_height_m"),
)

METRIC_KEYS = tuple(m[0] for m in HEIGHT_METRICS)
METRIC_LABELS = {m[0]: m[1] for m in HEIGHT_METRICS}
OUTPUT_HEIGHT_COLUMNS = {m[0]: m[2] for m in HEIGHT_METRICS}

# 合并明细 CSV 的固定列顺序
OUTPUT_COLUMNS = [
    "batch_no",
    "version_no",
    "tree_id",
    "tree_height_m",
    "under_branch_height_m",
]

UNITS = ("m", "cm")

# 流式读取 Parquet 的批大小：内存只与批大小有关，与文件总行数无关
READ_BATCH_SIZE = 10_000
PREVIEW_ROWS = 20
