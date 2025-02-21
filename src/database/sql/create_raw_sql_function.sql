-- Create raw_sql function for executing dynamic SQL
CREATE OR REPLACE FUNCTION raw_sql(command text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    EXECUTE command;
    RETURN '{"status": "success"}'::json;
EXCEPTION WHEN others THEN
    RETURN json_build_object(
        'status', 'error',
        'message', SQLERRM,
        'detail', SQLSTATE
    );
END;
$$; 