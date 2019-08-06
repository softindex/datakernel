create table if not exists article
(
  id    bigint auto_increment,
  title varchar(100) not null,
  text  text         not null,
  primary key (id),
  constraint article_id_uindex
    unique (id)
);
