
create materialized view v_atomic_cards as
with commander_cards as (
  -- fetching cards that are legal in commander and not marked as reprint
  select cards.*
  from cards
  join card_legalities
    on card_legalities.uuid = cards.uuid
  where 
    card_legalities.commander = 'Legal'
    and cards."isReprint" is null
),
atomic_cards as (
  -- one row per "name" unless it has two modes/faces/sides
  -- double sided cards will have two rows, one for each side 
    -- for example, there's two rows with name="Wear // Tear"
    -- "faceName" will be unique, either "Wear" or "Tear"
    -- "side" will be "a" or "b"
  select
    cards.name,
    cards."faceName",
    cards.side,
    cards."colorIdentity",
    cards."manaCost",
    cards."manaValue",
    cards.text,
    cards.supertypes,
    cards.subtypes,
    cards.types,
    cards.power,
    cards.toughness,
    cards.loyalty,
    cards.defense,
    cards.life,
    min(coalesce(cards."originalReleaseDate", sets."releaseDate"))::date as release_date
  from commander_cards as cards
  join sets
    on cards."setCode" = sets.code
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
)
select * from atomic_cards;


create materialized view v_count_unique_cards as
select count(distinct name) as unique_cards 
from v_atomic_cards;


create materialized view v_unique_cards_per_year as
with distinct_cards as (
	select distinct name, release_date
	from v_atomic_cards
)
select 
	date_trunc('year', release_date)::date as release_year,
	count(*) as unique_cards
from distinct_cards
group by 1
order by 1;

-- TODO: add indexes