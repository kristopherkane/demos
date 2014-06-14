CREATE DATABASE IF NOT EXISTS trilug;

USE trilug;

DROP TABLE IF EXISTS emails;

CREATE TABLE emails (
    from_who STRING,
    from_email_id STRING,
    from_domain_name STRING,
    from_domain_tld STRINg,
    from_name STRING,
    subject STRING,
    email_date STRING,
    messageid STRING,
    message STRING
)
STORED AS orc;
