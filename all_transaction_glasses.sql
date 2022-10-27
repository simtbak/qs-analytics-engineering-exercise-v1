create table if not exists all_transaction_glasses as 
select
	bar
	,ts as ts_start
	,datetime(ts,'+1 hour') as ts_end
	,drink
	,amount
	,strGlass as glass
from combined_seeds
inner join drink_glass_types on drink = strDrink;
