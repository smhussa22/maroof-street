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

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash   CHAR(64)    PRIMARY KEY,  -- SHA-256 hex digest of the raw API key
    account_id INT         NOT NULL REFERENCES accounts(account_id)
);

-- TODO: remove before production; test accounts and keys only
INSERT INTO accounts (account_id, name) VALUES
    (1, 'alice'),
    (2, 'bob')
ON CONFLICT DO NOTHING;

INSERT INTO balances (account_id, cash) VALUES
    (1, 100000),
    (2, 100000)
ON CONFLICT DO NOTHING;

INSERT INTO api_keys (key_hash, account_id) VALUES
    (encode(sha256('alice-key-123'), 'hex'), 1),
    (encode(sha256('bob-key-456'),   'hex'), 2)
ON CONFLICT DO NOTHING;