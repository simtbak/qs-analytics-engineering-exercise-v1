create table if not exists glass_usage as 
select
	 a.bar
	,a.ts_start
	,a.ts_end
	,a.glass
	,count(*) in_use
from all_transaction_glasses a 
inner join all_transaction_glasses b on a.glass = b.glass
where a.bar = b.bar
and a.ts_end >= b.ts_start
and b.ts_end >= a.ts_start
group by 
	 a.bar
	,a.ts_start
	,a.ts_end
	,a.glass
