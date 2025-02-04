SELECT uniq(user_id) AS unique_users
	,count(event_id) AS events_count
FROM events 
WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now();

SELECT event_type AS event
	,count(event_id) AS events_count
FROM events 
WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
GROUP BY event_type;


SELECT sum(t1.revenue) AS revenue
	,avg(t1.revenue) AS average_revenue_per_user
FROM (
	SELECT user_id 
		,sum(revenue) AS revenue
	FROM events  
	WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
	AND event_type = 'purchase'
	GROUP BY user_id
) t1
;

SELECT product_id 
	,sum(revenue) AS revenue
FROM events  
WHERE event_timestamp BETWEEN toStartOfMonth(now()) AND now()
AND event_type = 'purchase'
GROUP BY product_id 
ORDER BY revenue DESC
LIMIT 5
;


CREATE TABLE events (
        event_id UUID,
        user_id UUID,
        event_type String,
        event_timestamp DateTime,
        product_id UUID,
        revenue Float
    )
    ENGINE = MergeTree
    ORDER BY (event_timestamp);


CREATE TABLE IF NOT EXISTS events_by_day (
    day Date,
    events_count UInt16,
    revenue Float
) engine=SummingMergeTree()
ORDER BY (day);

CREATE MATERIALIZED VIEW events_by_day_mat_view TO events_by_day AS
SELECT 
    toDate(event_timestamp) as day, 
    count(event_id) as events_count, 
    sum(revenue) as revenue 
FROM events 
GROUP BY day;











