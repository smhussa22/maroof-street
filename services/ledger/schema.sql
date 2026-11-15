CREATE TABLE IF NOT EXISTS accounts (
    account_id  INT          PRIMARY KEY,
    name        VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS balances (
    account_id  INT          PRIMARY KEY REFERENCES accounts(account_id),
    cash        NUMERIC      NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS positions (
    account_id  INT         NOT NULL REFERENCES accounts(account_id),
    symbol      VARCHAR(16) NOT NULL,
    quantity    INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE IF NOT EXISTS trades (
    seq          BIGINT      NOT NULL,
    symbol       VARCHAR(16) NOT NULL,
    aggressor_id BIGINT      NOT NULL,
    passive_id   BIGINT      NOT NULL,
    price        INT         NOT NULL,
    quantity     INT         NOT NULL,
    settled_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (seq, symbol)
);

-- the engine's seq restarts every session while this table persists, so a trade is identified by (session, seq, symbol);
-- without the session, a new session's first fills would collide with old rows and be skipped as already settled
ALTER TABLE trades ADD COLUMN IF NOT EXISTS session TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_pkey;
ALTER TABLE trades ADD CONSTRAINT trades_pkey PRIMARY KEY (session, seq, symbol);

CREATE TABLE IF NOT EXISTS sessions (
    session    TEXT        PRIMARY KEY,
    opened_at  TIMESTAMPTZ,
    closed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trades     BIGINT      NOT NULL,
    volume     BIGINT      NOT NULL,
    traders    JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash   CHAR(64)    PRIMARY KEY,  -- SHA-256 hex digest of the raw API key
    account_id INT         NOT NULL REFERENCES accounts(account_id)
);

INSERT INTO accounts (account_id, name) VALUES
    (1, 'alice'),
    (2, 'bob'),
    (3, 'market-maker'),
    (4, 'random-trader'),
    (5, 'demo'),
    (6, 'go-momentum'),
    (7, 'rust-mean-reversion'),
    (8, 'java-twap')
ON CONFLICT DO NOTHING;

INSERT INTO balances (account_id, cash) VALUES
    (1, 100000),
    (2, 100000),
    (3, 10000000),
    (4, 5000000),
    (5, 1000000),
    (6, 5000000),
    (7, 5000000),
    (8, 10000000)
ON CONFLICT DO NOTHING;

INSERT INTO positions (account_id, symbol, quantity) VALUES
    (3, 'AAPL',  10000), (3, 'TSLA',  10000), (3, 'NVDA',  10000), (3, 'MSFT',  10000), (3, 'GOOGL',  10000),
    (4, 'AAPL',  5000),  (4, 'TSLA',  5000),  (4, 'NVDA',  5000),  (4, 'MSFT',  5000),  (4, 'GOOGL',  5000),
    (5, 'AAPL',  500),   (5, 'TSLA',  500),   (5, 'NVDA',  500),   (5, 'MSFT',  500),   (5, 'GOOGL',  500),
    (6, 'AAPL',  5000),  (6, 'TSLA',  5000),  (6, 'NVDA',  5000),  (6, 'MSFT',  5000),  (6, 'GOOGL',  5000),
    (7, 'AAPL',  5000),  (7, 'TSLA',  5000),  (7, 'NVDA',  5000),  (7, 'MSFT',  5000),  (7, 'GOOGL',  5000),
    (8, 'AAPL',  10000), (8, 'TSLA',  10000), (8, 'NVDA',  10000), (8, 'MSFT',  10000), (8, 'GOOGL',  10000)
ON CONFLICT DO NOTHING;

INSERT INTO api_keys (key_hash, account_id) VALUES
    (encode(sha256('alice-key-123'),    'hex'), 1),
    (encode(sha256('bob-key-456'),      'hex'), 2),
    (encode(sha256('mm-bot-key-111'),   'hex'), 3),
    (encode(sha256('rt-bot-key-222'),   'hex'), 4),
    (encode(sha256('demo-key-789'),     'hex'), 5),
    (encode(sha256('go-bot-key-333'),   'hex'), 6),
    (encode(sha256('rust-bot-key-444'), 'hex'), 7),
    (encode(sha256('java-bot-key-555'), 'hex'), 8)
ON CONFLICT DO NOTHING;