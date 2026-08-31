import pandas as pd

df = pd.read_excel("data/raw/Online Retail.xlsx")

sample = df.sample(10000, random_state=42)

sample.to_csv(
    "data/sample/ecommerce_sample.csv",
    index=False
)

print("Sample created:", sample.shape)