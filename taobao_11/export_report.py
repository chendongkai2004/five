import redis
import json
from datetime import date

today = date.today().isoformat()
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 提取总销售额
total = r.hget(f"orders:money:total:{today}", "global") or "0"

# 提取省份销售
province_items = r.hgetall(f"orders:money:province:{today}")
# 提取城市销售
city_items = r.hgetall(f"orders:money:city:{today}")

report = {
    "date": today,
    "total_sales": float(total),
    "province_sales": [{"province": k, "sales": float(v)} for k, v in province_items.items()],
    "city_sales": [{"city": k, "sales": float(v)} for k, v in city_items.items()]
}

with open("report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

print("导出成功：report.json")
