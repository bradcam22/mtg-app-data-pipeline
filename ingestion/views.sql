-- drop views if they exist (in correct dependency order)
drop materialized view if exists v_unique_cards_per_year;
drop materialized view if exists v_count_unique_cards;

-- recreate materialized views
create materialized view v_count_unique_cards as
select count(distinct name) as unique_cards 
from cards;

create materialized view v_unique_cards_per_year as
with cards_initial_release as (
	select
		cards.name,
		min(coalesce(cards."originalReleaseDate", sets."releaseDate"))::date as min_release_date
	from cards
	join sets
		on cards."setCode" = sets.code
	group by 1
)
select 
	date_trunc('year', min_release_date)::date as release_year,
	count(*) as unique_cards
from cards_initial_release
group by 1
order by 1;

-- TODO: add indexes