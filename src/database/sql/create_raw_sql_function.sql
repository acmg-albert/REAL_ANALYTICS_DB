CREATE OR REPLACE FUNCTION public.raw_sql(command text)
RETURNS void AS $$
BEGIN
  EXECUTE command;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER; 