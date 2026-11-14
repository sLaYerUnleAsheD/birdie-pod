import sqlglot
from sqlglot import exp

print(sqlglot.parse_one("SELECT CAST(a AS BIGINT) DIV CAST(b AS BIGINT)", read="spark").sql(dialect="spark"))
w = sqlglot.parse_one(
    "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r",
    read="spark",
)
spec = w.find(exp.Window).args["spec"]
print(spec)
print(spec.args)
print(w.sql(dialect="spark"))
node = sqlglot.parse_one("SELECT DATEDIFF(day, a, b)", read="tsql")
print(node.sql(dialect="spark"))
print(node.find(exp.DateDiff).args["unit"])
