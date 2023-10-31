-- 1) Создана хранимая процедура, которая, не уничтожая базу данных, уничтожает все те таблицы
-- текущей базы данных, имена которых начинаются с фразы 'TableName'.

CREATE TABLE IF NOT EXISTS "TableName_1"
(
    id serial primary key
);

CREATE TABLE IF NOT EXISTS "TableName_2"
(
    id serial primary key
);

CREATE TABLE IF NOT EXISTS "Table3"
(
    id serial primary key
);

CREATE OR REPLACE PROCEDURE drop_tables_table_name()
    LANGUAGE plpgsql
AS
$$
DECLARE
    table_name varchar;
BEGIN
    FOR table_name IN
        SELECT tablename
        FROM pg_tables
        WHERE tablename LIKE 'TableName%'
          AND schemaname = 'public'
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_name) || ' CASCADE';
        END LOOP;
END
$$;

SELECT * FROM pg_tables WHERE schemaname = 'public';
CALL drop_tables_table_name();
SELECT * FROM pg_tables WHERE schemaname = 'public';

-- 2) Создана хранимая процедура с выходным параметром, которая выводит список имен и параметров
-- всех скалярных SQL функций пользователя в текущей базе данных.
-- Имена функций без параметров не выводятся.
-- Имена и список параметров должны выводиться в одну строку.
-- Выходной параметр возвращает количество найденных функций.

CREATE OR REPLACE PROCEDURE sp_names_parameters(OUT count INT, REFCURSOR DEFAULT 'cur')
AS
$$
BEGIN
    OPEN $2 FOR
        SELECT routine_name                      as functions,
               STRING_AGG(p.parameter_name, ',') AS parameters
        FROM information_schema.routines r
                 JOIN information_schema.parameters p
                      ON r.specific_name = p.specific_name
        WHERE routine_type = 'FUNCTION'
          AND r.specific_schema = 'public'
          AND p.specific_schema = 'public'
          AND p.parameter_name IS NOT NULL
        GROUP BY routine_name
        ORDER BY 1;
    count := (SELECT COUNT(*) OVER ()
              FROM information_schema.routines r
                       JOIN information_schema.parameters p
                            ON r.specific_name = p.specific_name
              WHERE routine_type = 'FUNCTION'
                AND r.specific_schema = 'public'
                AND p.specific_schema = 'public'
                AND p.parameter_name IS NOT NULL
              GROUP BY routine_name
              LIMIT 1);
    RAISE NOTICE 'the number of functions found: %', count;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL sp_names_parameters(NULL);
FETCH ALL FROM cur;
END;

-- 3) Создана хранимая процедура с выходным параметром, которая уничтожает все SQL DML триггеры
-- в текущей базе данных. Выходной параметр возвращает количество уничтоженных триггеров.

CREATE OR REPLACE PROCEDURE drop_triggers(OUT dropped int)
    LANGUAGE plpgsql
    AS
$$
DECLARE
    tn varchar;
    et varchar;
BEGIN
    SELECT COUNT(1)
    INTO dropped
    FROM information_schema.triggers
    WHERE (trigger_schema, event_object_schema) = ('public', 'public');

    FOR tn, et IN (SELECT trigger_name, event_object_table FROM information_schema.triggers
                  WHERE (trigger_schema, event_object_schema) = ('public', 'public'))
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || tn || ' ON "' || et || '";';
        END LOOP;
END;
$$;

CALL drop_triggers(null);

-- 4) Создана хранимая процедура с входным параметром, которая выводит имена и описания типа объектов
-- (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка,
-- задаваемая параметром процедуры.

CREATE OR REPLACE PROCEDURE find_routines_have_code_like(string_specified varchar, REFCURSOR) AS
$$
BEGIN
    OPEN $2 FOR
        SELECT routine_name, routine_type, routine_definition
        FROM information_schema.routines
        WHERE routine_type IN ('PROCEDURE', 'FUNCTION')
          AND specific_schema NOT IN ('information_schema', 'pg_catalog')
          AND specific_schema IN ('public', 'myschema')
          AND routine_definition ~ string_specified;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL find_routines_have_code_like('SELECT', 'cur');
FETCH ALL IN cur;
CLOSE cur;
COMMIT;
