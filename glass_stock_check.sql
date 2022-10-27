with cte as (
	select
		 ts_start as busiest_hour
		,glass
		,bar
		,in_use
		,row_number() over ( partition by bar, glass order by in_use desc ) in_use_rank
	from glass_usage
) 

select 
	 cte.busiest_hour
	,cte.glass
	,cte.bar
	,cte.in_use as most_in_use
	,gs.stock
	,cte.in_use - gs.stock as glasses_needed
from cte 
inner join glass_stock gs on gs.glass_type = cte.glass and cte.bar = gs.bar
where cte.in_use_rank = 1 
and cte.in_use - gs.stock > 0
order by glasses_needed desc
