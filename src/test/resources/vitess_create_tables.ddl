DROP TABLE IF EXISTS numeric_table;
CREATE TABLE numeric_table
(
    id            BIGINT NOT NULL AUTO_INCREMENT,
    tinyint_col   TINYINT,
    smallint_col  SMALLINT,
    mediumint_col MEDIUMINT,
    int_col       INT,
    bigint_col    BIGINT,
    float_col     FLOAT,
    double_col    DOUBLE,
    decimal_col   decimal(10, 4),
    boolean_col   BOOLEAN,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS string_table;
CREATE TABLE string_table
(
    id             BIGINT NOT NULL AUTO_INCREMENT,
    char_col       CHAR(2),
    varchar_col    VARCHAR(32),
    binary_col     BINARY(2),
    varbinary_col  VARBINARY(32),
    tinytext_col   TINYTEXT,
    text_col       TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col   LONGTEXT,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS enum_table;
CREATE TABLE enum_table
(
    id       BIGINT                            NOT NULL AUTO_INCREMENT,
    enum_col ENUM ('small', 'medium', 'large') NOT NULL DEFAULT 'medium',
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS set_table;
CREATE TABLE set_table
(
    id      BIGINT                   NOT NULL AUTO_INCREMENT,
    set_col SET ('a', 'b', 'c', 'd') NOT NULL DEFAULT 'b',
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS time_table;
CREATE TABLE time_table
(
    id            BIGINT    NOT NULL AUTO_INCREMENT,
    time_col      TIME      NOT NULL DEFAULT '00:00:00',
    date_col      DATE      NOT NULL DEFAULT '2020-02-12',
    datetime_col  DATETIME  NOT NULL DEFAULT '2020-02-12 00:00:00',
    timestamp_col TIMESTAMP NOT NULL DEFAULT '2020-02-12 00:00:00',
    year_col      YEAR      NOT NULL DEFAULT '2020',
    PRIMARY KEY (id)
);