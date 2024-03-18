--Create three date sharded sample tables
CREATE TABLE IF NOT EXISTS demo.Sales_20240301
(
  date DATE,
  amount NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS demo.Sales_20240302
(
  date DATE,
  amount NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS demo.Sales_20240303
(
  date DATE,
  amount NUMERIC(5,2)
);

--Insert sample rows into data sharded tables
INSERT INTO demo.Sales_20240301 VALUES ('2024-03-01', 10.00);
INSERT INTO demo.Sales_20240302 VALUES ('2024-03-02', 20.00);
INSERT INTO demo.Sales_20240303 VALUES ('2024-03-03', 30.00);

--Create partitioned table on column in table (not ingestion time partitioning)
CREATE TABLE IF NOT EXISTS demo.Sales_Partitioned
(
  date DATE
  , amount NUMERIC(5,2)
)
PARTITION BY date

--This statement isn't allowed as you can't use wildcard table and column based partitioning isn't supported
INSERT INTO demo.Sales_Partitioned (date, amount) SELECT date, amount FROM `demo.Sales_*`;

--You can insert into a column based partitioned table if you select a specific date of a date sharded table
INSERT INTO demo.Sales_Partitioned (date, amount) SELECT date, amount FROM `demo.Sales_20240301`;

--Dynamic SQL can be used to create a procedure with a merge statement that accepts a parameter for a specific date sharded table to merge
CREATE OR REPLACE PROCEDURE demo.Sales_MERGE(tablename STRING)
BEGIN

  EXECUTE IMMEDIATE FORMAT("""
  MERGE demo.Sales_Partitioned AS a
  USING (SELECT date, amount FROM `amm-demo.demo.Sales_%s`) AS b
  ON a.date = b.date
  WHEN NOT MATCHED THEN 
    INSERT (date, amount)
    VALUES (date, amount)
  WHEN MATCHED THEN
    UPDATE SET a.amount = b.amount;
  """, tablename);

END

--Example calling procedure to load specific date sharded table to partitioned table
CALL demo.Sales_MERGE('20240301')

--Initial backfill can be done by querying INFORMATION_SCHEMA.TABLES and looping over all dates to load into partitioned table
FOR record IN
  (SELECT RIGHT(table_name, 8) tablename FROM demo.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'Sales_2024%')
DO
  CALL demo.Sales_MERGE(record.tablename);
END FOR;

--Cleanup
drop table demo.Sales_Partitioned;
drop table demo.Sales_20240301;
drop table demo.Sales_20240302;
drop table demo.Sales_20240303;



