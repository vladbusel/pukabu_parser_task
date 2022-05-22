import pandas as pd

df = pd.read_csv('db.csv')
df = df.drop_duplicates(subset=['story_id'])
df = df.sort_values('story_datetime')
print(f"{len(df['story_id'].unique())} unique records")
df.to_csv("cleaned_db.csv", index=False, encoding='utf8')
