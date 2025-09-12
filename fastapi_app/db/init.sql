CREATE TABLE IF NOT EXISTS symbols (
  symbol TEXT PRIMARY KEY,
  name   TEXT NOT NULL
);

INSERT INTO symbols(symbol, name) VALUES
  ('APPL','Apple Inc.'),
  ('MSFT','Microsoft Corporation')
ON CONFLICT (symbol) DO NOTHING;
