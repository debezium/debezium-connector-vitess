create table my_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into my_seq(id, next_id, cache) values(0, 1000, 100);
create table t1 (id bigint not null, varchar_col varchar(16), decimal_col decimal(12,4), decimal_col2 decimal(13,4), primary key (`id`));