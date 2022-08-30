CREATE TYPE querycount AS (query_name text,  query_count integer);

CREATE OR REPLACE FUNCTION testext()
RETURNS SETOF querycount
AS 'MODULE_PATHNAME', 'testext'
LANGUAGE C STRICT;