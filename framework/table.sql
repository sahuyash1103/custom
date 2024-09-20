CREATE TYPE action_type AS ENUM ('script', 'docker_image');

CREATE TABLE actions (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    type action_type NOT NULL,
    kafka_topic VARCHAR,
    script VARCHAR,
    docker_image VARCHAR,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT actions_name_key UNIQUE (name)
);

CREATE TABLE action_triggers (
    id SERIAL PRIMARY KEY,
    action_id INTEGER REFERENCES actions(id) ON DELETE CASCADE,
    kafka_topic VARCHAR,
    data_pattern VARCHAR,
    created_at TIMESTAMPTZ DEFAULT now()
);
