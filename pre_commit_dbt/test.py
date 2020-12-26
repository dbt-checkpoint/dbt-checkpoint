import timeit

prep = """
import json
from pathlib import Path

large_obj_data = Path('manifest.json', encoding='utf-8').read_text()
large_obj = json.loads(large_obj_data)
result = {}

"""

stmt = """
for key, value in large_obj.get("nodes").items():
    result[key.split('.')[-1]] = value
for item in ['seduo_log_activities', 'credit_balance_monthly']:
    result.get(item)
"""

# print(timeit.repeat(stmt=stmt, setup=prep, number=10000, repeat=5))


large_obj_data = Path("manifest.json", encoding="utf-8").read_text()
large_obj = json.loads(large_obj_data)
result = {}
