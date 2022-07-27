-- function which takes one positional arg and two named args with defaults
CREATE or replace FUNCTION films_f(title text,
    is_feature boolean default true,
    duration integer default 180)
RETURNS TABLE(
    title character varying,
    is_feature boolean,
    duration integer
)
as $$select title, is_feature, duration$$
language sql;

CREATE OR REPLACE FUNCTION inc_f(integer) RETURNS integer AS $$
BEGIN
    RETURN $1 + 1;
END;
$$ LANGUAGE plpgsql stable;

CREATE OR REPLACE FUNCTION inc_f_out(integer, out outparam integer) returns integer AS $$
    select 1;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION inc_f_noargs() RETURNS void AS $$
begin
perform 1;
end;
$$ LANGUAGE plpgsql stable;
