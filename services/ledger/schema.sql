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
