DROP TABLE IF EXISTS numeric_table;
CREATE TABLE numeric_table
(
    id                              BIGINT NOT NULL AUTO_INCREMENT,
    tinyint_col                     TINYINT,
    tinyint_unsigned_col            TINYINT UNSIGNED,
    smallint_col                    SMALLINT,
    smallint_unsigned_col           SMALLINT UNSIGNED,
    mediumint_col                   MEDIUMINT,
    mediumint_unsigned_col          MEDIUMINT UNSIGNED,
    int_col                         INT,
    int_unsigned_col                INT UNSIGNED,
    bigint_col                      BIGINT,
    bigint_unsigned_col             BIGINT UNSIGNED,
    bigint_unsigned_overflow_col    BIGINT UNSIGNED,
    float_col                       FLOAT,
    double_col                      DOUBLE,
    decimal_col                     DECIMAL(10, 4),
    boolean_col                     BOOLEAN,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS string_table;
CREATE TABLE string_table
(
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    char_col            CHAR(2),
    varchar_col         VARCHAR(32),
    varchar_kr_col      VARCHAR(32),
    varchar_ja_col      VARCHAR(32),
    binary_col          BINARY(2),
    varbinary_col       VARBINARY(32),
    tinytext_col        TINYTEXT,
    text_col            TEXT,
    mediumtext_col      MEDIUMTEXT,
    longtext_col        LONGTEXT,
    json_col            JSON,
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

DROP TABLE IF EXISTS no_pk_table;
CREATE TABLE no_pk_table
(
    id             BIGINT NOT NULL,
    int_col        INT NOT NULL
);

DROP TABLE IF EXISTS pk_single_unique_key_table;
CREATE TABLE pk_single_unique_key_table
(
    id             BIGINT NOT NULL,
    int_col        INT,
    PRIMARY KEY (id),
    UNIQUE KEY unique_col (int_col)
);

DROP TABLE IF EXISTS no_pk_multi_unique_keys_table;
CREATE TABLE no_pk_multi_unique_keys_table
(
    id             BIGINT NOT NULL,
    int_col        INT,
    int_col2       INT,
    UNIQUE KEY unique_col (int_col),
    UNIQUE KEY unique_col2 (int_col2)
);

DROP TABLE IF EXISTS no_pk_multi_comp_unique_keys_table;
CREATE TABLE no_pk_multi_comp_unique_keys_table
(
    id             BIGINT NOT NULL,
    int_col        INT,
    int_col2       INT,
    int_col3       INT,
    int_col4       INT,
    int_col5       INT,
    UNIQUE KEY unique_col_1_2 (int_col, int_col2),
    UNIQUE KEY unique_col_3 (int_col3),
    UNIQUE KEY unique_col_4_5 (int_col4, int_col5)
);

DROP TABLE IF EXISTS comp_pk_table;
CREATE TABLE comp_pk_table
(
    id             BIGINT NOT NULL,
    int_col        INT,
    int_col2        INT,
    PRIMARY KEY (id, int_col)
);