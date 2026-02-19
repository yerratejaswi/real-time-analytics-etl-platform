-- Daily active users
SELECT COUNT(DISTINCT user_id)
FROM curated_events;

-- Average watch time per event type
SELECT event_type, AVG(watch_time)
FROM curated_events
GROUP BY event_type;
