create table my_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into my_seq(id, next_id, cache) values(0, 1000, 100);
