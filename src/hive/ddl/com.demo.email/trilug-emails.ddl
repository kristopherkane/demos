CREATE DATABASE IF NOT EXISTS trilug;

USE trilug;

DROP TABLE IF EXISTS emails;

CREATE TABLE emails (
    from_who STRING,
    subject STRING,
    email_date STRING,
    messageid STRING,
    message STRING
)
STORED AS orc;
