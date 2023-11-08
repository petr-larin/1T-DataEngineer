CREATE DATABASE IF NOT EXISTS hw;

CREATE TABLE IF NOT EXISTS hw.vac
(
  Ids UInt32,
  Employer String,
  Name String,
  Salary Bool,
  From Nullable(Float32),
  To Nullable(Float32),
  Experience String,
  Schedule String,
  Keys String,
  Description String,
  Area String,
  Professional_roles String,
  Specializations String,
  Profarea_names String,
  Published_at DateTime
)
ENGINE = MergeTree()
ORDER BY (Area, Ids);
