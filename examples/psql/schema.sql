CREATE TABLE IF NOT EXISTS configs (
    id SERIAL PRIMARY KEY,
    benchmark TEXT NOT NULL,
    scenario TEXT NOT NULL,
    store TEXT NOT NULL,
    instance_type TEXT NOT NULL,
    cache BOOLEAN NOT NULL,
    UNIQUE(benchmark,
           scenario,
           store,
           cache,
           instance_type)
);

CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    branch TEXT NOT NULL,
    commit TEXT NOT NULL,
    commit_ts TIMESTAMPTZ NOT NULL,
    username TEXT NOT NULL,
    details_url TEXT NOT NULL,
    exclude_from_analysis BOOLEAN DEFAULT false NOT NULL,
    exclude_reason TEXT
);

CREATE TABLE IF NOT EXISTS results (
  experiment_id TEXT NOT NULL REFERENCES experiments(id),
  config_id INTEGER NOT NULL REFERENCES configs(id),

  process_cumulative_rate_mean BIGINT NOT NULL,
  process_cumulative_rate_stderr BIGINT NOT NULL,
  process_cumulative_rate_diff BIGINT NOT NULL,

  process_cumulative_rate_mean_rel_forward_change DOUBLE PRECISION,
  process_cumulative_rate_mean_rel_backward_change DOUBLE PRECISION,
  process_cumulative_rate_mean_p_value DECIMAL,

  process_cumulative_rate_stderr_rel_forward_change DOUBLE PRECISION,
  process_cumulative_rate_stderr_rel_backward_change DOUBLE PRECISION,
  process_cumulative_rate_stderr_p_value DECIMAL,

  process_cumulative_rate_diff_rel_forward_change DOUBLE PRECISION,
  process_cumulative_rate_diff_rel_backward_change DOUBLE PRECISION,
  process_cumulative_rate_diff_p_value DECIMAL,

  PRIMARY KEY (experiment_id, config_id)
);