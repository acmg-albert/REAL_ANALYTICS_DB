-- Create raw_sql function for executing dynamic SQL
CREATE OR REPLACE FUNCTION raw_sql(query text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN (SELECT json_agg(t) FROM (SELECT * FROM json_to_recordset(query::json) as t) as t);
EXCEPTION WHEN others THEN
    EXECUTE query;
    RETURN '{"status": "success"}'::json;
END;
$$; 