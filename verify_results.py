#!/usr/bin/env python3
import re
from collections import defaultdict

# Read the SQL file
with open('/Users/pranavhabib/esql-processor/schema/load_sales_10000_table.sql', 'r') as f:
    content = f.read()

# Parse all insert statements
pattern = r"insert into sales values \('([^']+)', '[^']+', \d+, \d+, \d+, '([A-Z]{2})', (\d+), '[^']+'\);"
matches = re.findall(pattern, content, re.IGNORECASE)

print(f"Total rows found: {len(matches)}")

# Aggregate by customer and state
data = defaultdict(lambda: {'NY': [], 'NJ': [], 'CT': []})

for cust, state, quant in matches:
    if state in ['NY', 'NJ', 'CT']:
        data[cust][state].append(int(quant))

# Calculate aggregates and apply HAVING clause
print("\n" + "="*140)
print(f"{'Customer':<10} {'SUM(NY)':<12} {'SUM(NJ)':<12} {'SUM(CT)':<12} {'AVG(NY)':<12} {'AVG(CT)':<12} {'Cond1':<8} {'Cond2':<8} {'Pass':<6}")
print("="*140)

results = []
for cust in sorted(data.keys()):
    sum_ny = sum(data[cust]['NY'])
    sum_nj = sum(data[cust]['NJ'])
    sum_ct = sum(data[cust]['CT'])

    avg_ny = sum_ny / len(data[cust]['NY']) if data[cust]['NY'] else 0
    avg_ct = sum_ct / len(data[cust]['CT']) if data[cust]['CT'] else 0

    # HAVING clause: sum(x.quant) > 2 * sum(y.quant) OR avg(x.quant) > avg(z.quant)
    cond1 = sum_ny > 2 * sum_nj
    cond2 = avg_ny > avg_ct
    passes = cond1 or cond2

    print(f"{cust:<10} {sum_ny:<12} {sum_nj:<12} {sum_ct:<12} {avg_ny:<12.2f} {avg_ct:<12.2f} {str(cond1):<8} {str(cond2):<8} {str(passes):<6}")

    if passes:
        results.append((cust, sum_ny, sum_nj, sum_ct))

print("="*140)
print(f"\n\nFINAL RESULTS (customers that pass HAVING clause):")
print("="*80)
print(f"{'cust':<10} | {'sum(x.quant)':<15} | {'sum(y.quant)':<15} | {'sum(z.quant)':<15}")
print("-"*80)
for cust, sum_ny, sum_nj, sum_ct in results:
    print(f"{cust:<10} | {sum_ny:<15} | {sum_nj:<15} | {sum_ct:<15}")
print("="*80)

print(f"\nTotal customers passing HAVING clause: {len(results)}")

# Compare with provided results
print("\n\nCOMPARISON:")
print("="*80)
teammate_custs = ['Dan', 'Claire', 'Chae', 'Mia', 'Sam', 'Wally', 'Helen', 'Emily', 'Boo']
user_custs = ['Dan', 'Emily', 'Mia', 'Chae', 'Sam', 'Helen']
actual_custs = [r[0] for r in results]

print(f"Teammate's result has {len(teammate_custs)} customers: {teammate_custs}")
print(f"Your result has {len(user_custs)} customers: {user_custs}")
print(f"Actual result has {len(actual_custs)} customers: {actual_custs}")

if set(actual_custs) == set(user_custs):
    print("\n✓ YOUR RESULT IS CORRECT!")
elif set(actual_custs) == set(teammate_custs):
    print("\n✓ TEAMMATE'S RESULT IS CORRECT!")
else:
    print("\n✗ NEITHER RESULT MATCHES!")