CREATE TABLE t_user_stat (
  stat_id bigint,
  uid bigint,
  stat VARCHAR(50),
  created_at TIMESTAMP,
  PRIMARY KEY (stat_id)
);