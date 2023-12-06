delete from airlines

select *
from public.airlines
limit 10

select *
from public.airports a 
limit 10

select *
from public.flights 
limit 10

select *
from public.planes


--alter table public.flights drop constraint fk_airline;
--alter table public.flights drop constraint fk_dest;
--alter table public.flights drop constraint fk_origin;
--alter table public.flights drop constraint fk_plane;

select carrier || '-' || flight as flight
, left(LPAD(cast(sched_dep_time as varchar),4,'0'),2)||':'||right(cast(sched_dep_time as varchar),2) as sched_dep_time
from public.flights fl 
