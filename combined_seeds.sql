create table if not exists combined_seeds as 		  
		  select 'london' 	bar, id, ts, drink, amount from seed_london_transactions
union all select 'budapest' bar, id, ts, drink, amount from seed_budapest
union all select 'new york' bar, id, ts, drink, amount from seed_ny
