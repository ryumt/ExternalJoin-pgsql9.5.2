DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1(key int, dval float);
CREATE TABLE t2(key int, dval float);

INSERT INTO t1 (SELECT generate_series(1, 10000), random() * 100);
INSERT INTO t2 (SELECT generate_series(1, 10000), generate_series(1, 10) * 10 * random());
