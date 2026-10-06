CREATE KEYSPACE IF NOT EXISTS banking_analysis
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE banking_analysis;

CREATE TABLE IF NOT EXISTS api_requests (
    service text,
    request_date date,
    timestamp timestamp,
    request_id text,
    method text,
    path text,
    status int,
    duration_ms bigint,
    PRIMARY KEY ((service, request_date), timestamp, request_id)
);
