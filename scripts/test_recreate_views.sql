-- Drop views if they exist (in correct dependency order)
DROP MATERIALIZED VIEW IF EXISTS v_unique_cards_over_time;
DROP MATERIALIZED VIEW IF EXISTS v_count_unique_cards;

-- Recreate materialized views
CREATE MATERIALIZED VIEW v_count_unique_cards AS
SELECT count(distinct name) as unique_cards 
from cards;

CREATE MATERIALIZED VIEW v_unique_cards_over_time AS
with cards_initial_release as (
	SELECT
		cards.name,
		min("releaseDate")::date as min_release_date
	FROM cards
	JOIN sets
		on cards."setCode" = sets.code
	GROUP BY 1
)
SELECT 
	date_trunc('month', min_release_date)::date as release_month,
	count(*) as unique_cards
FROM cards_initial_release
GROUP BY 1
ORDER BY 1;
