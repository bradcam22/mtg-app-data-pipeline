-- materialized views
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
  select
    cards.name,
    cards."faceName" as face_name,
    cards.side,
    cards."colorIdentity" as color_identity,
    cards."manaCost" as mana_cost,
    cards."manaValue" as mana_value,
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
-- TODO: add indexes

-- functions
drop function if exists get_unique_cards CASCADE;
drop function if exists get_unique_cards_per_year CASCADE;
drop function if exists get_distinct_color_identities CASCADE;

create function get_unique_cards(color_filter text default null)
returns table (unique_cards bigint) 
language sql
as $$
  select count(distinct name) as unique_cards
  from v_atomic_cards
  where (color_filter is null or color_identity = color_filter);
$$;

create function get_unique_cards_per_year(color_filter text default null)
returns table (release_year date, unique_cards bigint)
language sql
as $$
  select 
      date_trunc('year', release_date)::date as release_year,
      count(distinct name) as unique_cards
  from v_atomic_cards
  where (color_filter is null or color_identity = color_filter)
  group by release_year
  order by release_year;
$$;

create function get_distinct_color_identities()
returns table (color_identity text)
language sql
as $$
    select distinct color_identity
    from v_atomic_cards
    order by color_identity;
$$;
