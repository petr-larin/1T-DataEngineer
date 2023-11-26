drop table if exists s;

create table if not exists s(
    id int,
    ts bigint
);

with s_part as(
select distinct id, date(to_timestamp(ts) at time zone 'MSK') as date,
  max(ts) over(wnd) -
  min(ts) over(wnd) as s_len
from s
window wnd as (partition by id, date(to_timestamp(ts) at time zone 'MSK'))
order by id, date
	)
select id, round(avg(s_len) / 3600, 2)
as avg_s_len_hours
from s_part
group by id
order by id