import psycopg2
import psycopg2.extras
from random import randint, choice, shuffle, randrange
import string
from math import ceil, floor
import matplotlib.pyplot as plt
import numpy as np
import os

def get_crossover_point(n_rows, more_size):
    percentile_low = 0
    percentile_high = 100
    while True:
        conn = psycopg2.connect(os.environ['CONN_STRING'])
        conn.set_session(autocommit=True)

        cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

        cur.execute("""CREATE TEMPORARY TABLE bigtable (
                       Id SERIAL PRIMARY KEY,
                       data varchar(100),
                       more varchar(10000));""")

        percentile = ceil((percentile_high + percentile_low) / 2)
        print(n_rows, more_size, percentile)

        dist = [1]*ceil(n_rows * (percentile / 100))
        dist.extend([0]*ceil(n_rows * ((100-percentile) / 100)))
        shuffle(dist)

        insert_sql = ''
        for i in range(n_rows):
            rand_val = dist[i]
            fmt = '%%%dx' % more_size
            more_str = fmt % randrange(16**more_size)
            insert_sql += "INSERT INTO bigtable(data, more) VALUES ('data{0}', '{1}');\n".format(rand_val, more_str)

        cur.execute(insert_sql.encode('ascii'))
        cur.execute("CREATE INDEX idx ON bigtable(data);")
        cur.execute("ANALYZE bigtable;")

        cur.execute("""EXPLAIN SELECT * from bigtable
                        WHERE data='data1'""")

        uses_index = False
        for row in cur.fetchall():
            uses_index = 'Index Cond' in row.QUERY_PLAN

        if uses_index:
            percentile_low = percentile
        else:
            percentile_high = percentile

        conn.close()

        if (percentile_high - percentile_low) <= 1:
            break
    return percentile


more_size = (100, 1000)
n_rows = [int(n) for n in np.logspace(2, 6, 100)]

results = []
for m in more_size:
    row_results = []
    for n in n_rows:
        x_over = get_crossover_point(n, m)
        row_results.append(x_over)
    results.append(row_results)
print(results)

f = open('results/results.csv', 'w')
f.write('rows,%s\n' % ','.join([str(m) for m in more_size]))
for i in range(len(n_rows)):
    f.write('%s,%s\n' % (n_rows[i], ','.join([str(results[j][i]) for j in range(len(more_size))])))
f.close()


plt.figure(figsize=(10, 6.5))
plt.title('Onset of Table Scan vs Number of Rows')
for i, row_results in enumerate(results):
    plot_label = '~%d bytes / row' % more_size[i]
    plt.semilogx(n_rows, row_results, label=plot_label)
plt.ylabel('% of data returned which initiates table scan')
plt.xlabel('Number of Rows')
plt.minorticks_on()
plt.ylim((0,50))
plt.legend(loc='lower right')
plt.grid(True, which='major', linestyle='solid', axis='y')
plt.grid(True, which='minor', linestyle='dashed', axis='y')
plt.grid(True, which='major', linestyle='solid', axis='x')
plt.grid(True, which='minor', linestyle='dashed', axis='x')
plt.savefig('results/results.png', format='png', transparent=True)
plt.close()


more_size = range(100, 3000, 100)
n_rows = (5000,10000)

results = []
for n in n_rows:
    row_results = []
    for m in more_size:
        x_over = get_crossover_point(n, m)
        row_results.append(x_over)
    results.append(row_results)
print(results)

f = open('results/results_size.csv', 'w')
f.write('bytes,%s\n' % ','.join([str(n) for n in n_rows]))
for i in range(len(more_size)):
    f.write('%s,%s\n' % (more_size[i], ','.join([str(results[j][i]) for j in range(len(n_rows))])))
f.close()


plt.figure(figsize=(10,6.5))
plt.title('Onset of Table Scan vs Row Size')
for i, row_results in enumerate(results):
    plot_label = '%d rows' % n_rows[i]
    plt.plot(more_size, row_results, label=plot_label)
plt.ylabel('% of data returned which initiates table scan')
plt.xlabel('Row Size (bytes)')
plt.minorticks_on()
plt.ylim((0,50))
plt.legend(loc='lower right')
plt.grid(True, which='major', linestyle='solid', axis='y')
plt.grid(True, which='minor', linestyle='dashed', axis='y')
plt.grid(True, which='major', linestyle='solid', axis='x')
plt.grid(True, which='minor', linestyle='dashed', axis='x')
plt.savefig('results/results_size.png', format='png', transparent=True)
plt.close()
