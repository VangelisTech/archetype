import daft
from daft import col
from daft.functions import llm_generate



df = daft.read_csv("prompts.csv")
df = df.with_column("response", llm_generate(col("prompt"), model="facebook/opt-125m"))
df.collect()