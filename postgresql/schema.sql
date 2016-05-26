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
    username        VARCHAR(128) NOT NULL UNIQUE CHECK ( username <> ''),
    first_name      VARCHAR(128) NOT NULL CHECK ( first_name <> ''),
    last_name       VARCHAR(128) NOT NULL CHECK ( last_name <> ''),
    email           VARCHAR(128) NOT NULL CHECK ( email <> ''),
    institution     VARCHAR(128) NOT NULL CHECK ( institution <> ''),
    phone           VARCHAR(128) NOT NULL CHECK ( phone <> ''),
    password        CHAR(64) NOT NULL CHECK ( password <> ''),
    salt            CHAR(64) NOT NULL CHECK ( salt <> '')
);

CREATE TABLE freesurfer_interface.jobs (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    username        VARCHAR(128) NOT NULL REFERENCES freesurfer_interface.users(username),
    subject         VARCHAR(128) NOT NULL,
    multicore       BOOLEAN NOT NULL DEFAULT FALSE,
    image_filename  VARCHAR(128) NOT NULL,
    pegasus_ts      VARCHAR(128),
    state           freesurfer_interface.job_state NOT NULL,
    job_date        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    options         VARCHAR(1024),
    purged          BOOLEAN NOT NULL DEFAULT FALSE,
    num_inputs      INTEGER NOT NULL DEFAULT 0,
);

CREATE TABLE freesurfer_interface.input_files (
    id              SERIAL PRIMARY KEY,
    filename        VARCHAR(255) NOT NULL,
    path            VARCHAR(1024) NOT NULL,
    job_id          INTEGER NOT NULL REFERENCES freesurfer_interface.jobs(id),
    purged          BOOLEAN NOT NULL DEFAULT FALSE,
    subject_dir     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE freesurfer_interface.verifications (
    id              SERIAL PRIMARY KEY,
    kernel_version  VARCHAR(128) NOT NULL,
    successful      INTEGER DEFAULT 0,
    attempts        INTEGER DEFAULT 0,
    log_directory   VARCHAR(256) DEFAUlT '',
);

COMMIT;