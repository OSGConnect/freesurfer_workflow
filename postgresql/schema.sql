BEGIN;

CREATE SCHEMA freesurfer_interface;

CREATE TYPE freesurfer_interface.job_state AS ENUM (
    'UPLOADED',
    'PROCESSING',
    'FAILED',
    'COMPLETED',
    'DELETE PENDING',
    'DELETED',
    'ERROR'
);

CREATE TABLE freesurfer_interface.users (
    id              SERIAL PRIMARY KEY,
    username        varchar(128) NOT NULL UNIQUE CHECK ( username <> ''),
    first_name      varchar(128) NOT NULL CHECK ( first_name <> ''),
    last_name       varchar(128) NOT NULL CHECK ( last_name <> ''),
    email           varchar(128) NOT NULL CHECK ( email <> ''),
    institution     varchar(128) NOT NULL CHECK ( institution <> ''),
    phone           varchar(128) NOT NULL CHECK ( phone <> ''),
    password        char(64) NOT NULL CHECK ( password <> ''),
    salt            char(64) NOT NULL CHECK ( salt <> '')
);

CREATE TABLE freesurfer_interface.jobs (
    id              SERIAL PRIMARY KEY,
    name            varchar(128) NOT NULL,
    username        varchar(128) NOT NULL REFERENCES freesurfer_interface.users(username),
    subject         varchar(128) NOT NULL,
    multicore       BOOLEAN NOT NULL DEFAULT FALSE,
    image_filename  varchar(128) NOT NULL,
    log_filename    varchar(128) NOT NULL,
    pegasus_ts      varchar(128),
    state           freesurfer_interface.job_state NOT NULL,
    job_date        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    purged          boolean NOT NULL DEFAULT FALSE
);

COMMIT;