import redis
import matplotlib.pyplot as plt

r = redis.Redis(host='localhost', port=6379, db=0)
today = "2026-07-25"   # 请修改为实际日期

# 获取数据
total = float(r.hget(f"orders:money:total:{today}", "global") or 0)
province_data = r.hgetall(f"orders:money:province:{today}")
city_data = r.hgetall(f"orders:money:city:{today}")

# 准备绘图
labels = ['Total']
values = [total]
for k, v in province_data.items():
    labels.append(k.decode())
    values.append(float(v))

plt.figure(figsize=(10, 5))
plt.bar(labels, values)
plt.title(f'Real-time Sales Report - {today}')
plt.ylabel('Sales (CNY)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('report.png', dpi=150)
print("Chart saved as report.png")
