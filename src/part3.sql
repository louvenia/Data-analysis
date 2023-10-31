-- 1) Написана функция, возвращающая таблицу TransferredPoints в более человекочитаемом виде

CREATE
    OR REPLACE FUNCTION transferredpoints_total()
    RETURNS TABLE
            (
                peer1        varchar,
                peer2        varchar,
                pointsamount int
            )
AS
$$
BEGIN

    RETURN QUERY SELECT LEAST(checking_peer, checked_peer)    AS peer1,
                        GREATEST(checking_peer, checked_peer) AS peer2,
                        SUM(CASE WHEN checking_peer < checked_peer THEN points_amount ELSE - points_amount END)::int
                 FROM transferredpoints
                 GROUP BY peer1, peer2
                 ORDER BY 1, 2;
END
$$
    LANGUAGE plpgsql;

SELECT * FROM transferredpoints_total();

-- 2) Написана функция, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP

CREATE
    OR REPLACE FUNCTION peer_completed_tasks()
    RETURNS TABLE
            (
                peer varchar,
                task varchar,
                xp   bigint
            )
AS
$$
BEGIN

    RETURN QUERY SELECT p.peer      AS peer,
                        t.title     AS task,
                        xp.xpamount AS xp
                 FROM peers AS p
                          INNER JOIN checks AS ch
                                     ON p.peer = ch.peer
                          INNER JOIN p2p
                                     ON ch.id = p2p.check_id
                          LEFT JOIN verter AS v
                                    ON ch.id = v.check_id
                          INNER JOIN tasks AS t
                                     ON ch.task = t.title
                          INNER JOIN xp
                                     ON ch.id = xp.check_id
                 WHERE p2p.state = 'Success'
                   AND (v.state = 'Success' OR v.state IS NULL)
                 ORDER BY 1, 2, 3;
END
$$
    LANGUAGE plpgsql;

SELECT * FROM peer_completed_tasks();

-- 3) Написана функция, определяющая пиров, которые не выходили из кампуса в течение всего дня

CREATE
    OR REPLACE FUNCTION peers_all_day_in_campus(
    day date
)
    RETURNS TABLE
            (
                peer varchar
            )
AS
$$
BEGIN

    RETURN QUERY SELECT tt.peer AS peer
                 FROM timetracking AS tt
                 WHERE "date" = day
                   AND state = 2
                   AND (SELECT COUNT(tt.peer) FROM timetracking AS tt WHERE "date" = day AND state = 2) = 1
                 ORDER BY 1;
END
$$
    LANGUAGE plpgsql;

SELECT * FROM peers_all_day_in_campus('2023-01-01');

-- 4) Посчитаны изменения в количестве пир поинтов каждого пира по таблице TransferredPoints

CREATE
    OR REPLACE FUNCTION peers_change_points_from_table()
    RETURNS TABLE
            (
                peer         varchar,
                pointschange int
            )
AS
$$
BEGIN

    RETURN QUERY SELECT p.peer,
                        COALESCE(SUM(CASE
                                         WHEN p.peer = tp.checking_peer THEN tp.points_amount
                                         ELSE -tp.points_amount END)::int, 0)
                 FROM peers AS p
                          LEFT JOIN transferredpoints AS tp
                                    ON p.peer = tp.checking_peer OR p.peer = tp.checked_peer
                 GROUP BY 1
                 ORDER BY 2 DESC;
END
$$
    LANGUAGE plpgsql;

SELECT * FROM peers_change_points_from_table();

-- 5) Посчитаны изменения в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3

CREATE
    OR REPLACE FUNCTION peers_change_points_from_func()
    RETURNS TABLE
            (
                peer         varchar,
                pointschange int
            )
AS
$$
BEGIN
    RETURN QUERY SELECT p.peer,
                        COALESCE(SUM(CASE
                                         WHEN p.peer = ttol.peer1 THEN ttol.pointsamount
                                         ELSE -ttol.pointsamount END)::int, 0)
                 FROM peers AS p
                          LEFT JOIN transferredpoints_total() AS ttol
                                    ON p.peer = ttol.peer1 OR p.peer = ttol.peer2
                 GROUP BY 1
                 ORDER BY 2 DESC;
END
$$
    LANGUAGE plpgsql;

SELECT * FROM peers_change_points_from_func();

-- 6) Определяется самое часто проверяемое задание за каждый день

CREATE
    OR REPLACE FUNCTION output_max_count(check_day date)
    RETURNS int AS
$$
BEGIN
    RETURN (SELECT count(*) AS count
            FROM checks
            GROUP BY date, task
            HAVING date = check_day
            ORDER BY count DESC
            LIMIT 1);
END;
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE prc_frequently_task(refcursor) AS
$$
BEGIN
    OPEN $1 FOR
        WITH count_task_date AS (SELECT date, task, count(*) AS count
                                 FROM checks
                                 GROUP BY date, task
                                 ORDER BY date DESC)
        SELECT TO_CHAR(date, 'DD.MM.YYYY') AS day, SUBSTRING(task from '^(.+?)_') AS task
        FROM count_task_date ctd
        WHERE count = output_max_count(ctd.date);
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_frequently_task('cur');
FETCH ALL IN cur;
END;

-- 7) Проводится поиск всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания

CREATE
    OR REPLACE PROCEDURE prc_completed_all_block(REFCURSOR, block_name VARCHAR) AS
$$
BEGIN
    OPEN $1 FOR
        WITH output_block AS (SELECT title
                              FROM tasks
                              WHERE title SIMILAR TO block_name || '[0-9]%'),
             peer_completed_block AS (SELECT peer, task, date
                                      FROM checks c
                                               JOIN p2p p ON p.check_id = c.id AND p.state = 'Success' AND
                                                             task IN (SELECT title FROM output_block)
                                               LEFT JOIN verter v ON v.check_id = c.id
                                      WHERE v.state = 'Success'
                                         OR v.state IS NULL)
        SELECT peer, TO_CHAR(MAX(date), 'DD.MM.YYYY') AS day
        FROM peer_completed_block
        GROUP BY peer
        HAVING COUNT(DISTINCT task) = (SELECT count(*) FROM output_block)
        ORDER BY day;
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_completed_all_block('cur', 'C');
FETCH ALL IN cur;
END;

-- 8) Определяется, к какому пиру стоит идти на проверку каждому обучающемуся

CREATE
    OR REPLACE PROCEDURE prc_friendly_peer(REFCURSOR) AS
$$
BEGIN
    OPEN $1 FOR
        WITH check_friends AS (SELECT p.peer, (CASE WHEN p.peer = f.peer1 THEN peer2 ELSE peer1 END) AS friend
                               FROM peers p
                                        JOIN friends f ON f.peer1 = p.peer OR f.peer2 = p.peer),
             comp_friend_recommendation AS (SELECT cf.peer AS             peer
                                                 , COUNT(recommendedpeer) count
                                                 , recommendedpeer
                                            FROM check_friends cf
                                                     JOIN recommendations r
                                                          ON r.peer = cf.friend
                                            WHERE r.recommendedpeer IS NOT NULL
                                              AND r.recommendedpeer != cf.peer
                                            GROUP BY 1, 3),
             check_max_recommendation AS (SELECT peer, MAX(count) AS max_count
                                          FROM comp_friend_recommendation
                                          GROUP BY 1)
        SELECT cpr.peer, cpr.recommendedpeer
        FROM comp_friend_recommendation cpr
                 JOIN check_max_recommendation cmr ON cmr.peer = cpr.peer AND cmr.max_count = cpr.count
        ORDER BY 1;
end;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_friendly_peer('cur');
FETCH ALL IN cur;
END;

-- 9) Определяется процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному

CREATE
    OR REPLACE PROCEDURE prc_percent_started_block(REFCURSOR, block_name1 VARCHAR, block_name2 VARCHAR) AS
$$
BEGIN
    OPEN $1 FOR
        WITH output_block1 AS (SELECT DISTINCT peer
                               FROM checks
                               WHERE task SIMILAR TO block_name1 || '[0-9]%'),
             output_block2 AS (SELECT DISTINCT peer
                               FROM checks
                               WHERE task SIMILAR TO block_name2 || '[0-9]%'),
             peer_started_both AS (SELECT * FROM output_block1 INTERSECT SELECT * FROM output_block2),
             peer_not_started_both AS (SELECT peer
                                       FROM peers
                                       EXCEPT
                                       (SELECT * FROM output_block1 UNION SELECT * FROM output_block2)),
             percent_started_block1 AS (SELECT ((COUNT(ob1.*) * 100) / (SELECT COUNT(*) FROM peers)) AS percent1
                                        FROM output_block1 ob1),
             percent_started_block2 AS (SELECT ((COUNT(ob2.*) * 100) / (SELECT COUNT(*) FROM peers)) AS percent2
                                        FROM output_block2 ob2),
             percent_started_block1_2 AS (SELECT ((COUNT(b.*) * 100) / (SELECT COUNT(*) FROM peers)) AS percent_b
                                          FROM peer_started_both b),
             percent_not_started_both AS (SELECT ((COUNT(n_b.*) * 100) / (SELECT COUNT(*) FROM peers)) AS percent_nb
                                          FROM peer_not_started_both n_b)
        SELECT (SELECT * FROM percent_started_block1)   AS StartedBlock1,
               (SELECT * FROM percent_started_block2)   AS StartedBlock2,
               (SELECT * FROM percent_started_block1_2) AS StartedBothBlocks,
               (SELECT * FROM percent_not_started_both) AS DidntStartAnyBlock;
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_percent_started_block('cur', 'C', 'DO'); -- Использовать A и CPP для процента, где не приступили ни к одному
FETCH ALL IN cur;
END;

-- 10) Определяется процент пиров, которые когда-либо успешно проходили проверку в свой день рождения

CREATE
    OR REPLACE PROCEDURE prc_check_on_birthday(REFCURSOR) AS
$$
BEGIN
    OPEN $1 FOR
        WITH change_birthday AS (SELECT peer, TO_CHAR(MAX(birthday), 'DD.MM') AS birthday FROM peers GROUP BY peer),
             change_date_checks AS (SELECT id, peer, task, TO_CHAR(MAX(date), 'DD.MM') AS check_date
                                    FROM checks
                                    GROUP BY id, peer, task),
             match_date_birthday AS (SELECT cdc.id, cdc.peer, cdc.task, cdc.check_date
                                     FROM change_date_checks cdc
                                              JOIN change_birthday cb
                                                   ON cb.birthday = cdc.check_date AND cb.peer = cdc.peer),
             check_success AS (SELECT peer
                               FROM match_date_birthday mdb
                                        JOIN p2p p2 ON p2.check_id = mdb.id AND p2.state = 'Success'
                                        LEFT JOIN verter v ON v.check_id = mdb.id
                               WHERE v.state = 'Success'
                                  OR v.state IS NULL),
             check_fail_verter AS (SELECT peer
                                   FROM match_date_birthday mdb
                                            JOIN verter v ON v.check_id = mdb.id AND v.state = 'Failure'),
             check_fail AS ((SELECT peer
                             FROM match_date_birthday mdb
                                      JOIN p2p p2 ON p2.check_id = mdb.id AND p2.state = 'Failure')
                            UNION
                            (SELECT *
                             FROM check_fail_verter))
        SELECT (((SELECT COUNT(*) FROM check_success) * 100) /
                (SELECT COUNT(*) FROM match_date_birthday)) AS SuccessfulChecks,
               (((SELECT COUNT(*) FROM check_fail) * 100) /
                (SELECT COUNT(*) FROM match_date_birthday)) AS UnsuccessfulChecks;
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_check_on_birthday('cur');
FETCH ALL IN cur;
END;

-- 11) Определяются все пиры, которые сдали заданные задания 1 и 2, но не сдали задание 3

CREATE
    OR REPLACE PROCEDURE prc_three_task(REFCURSOR, task1 VARCHAR, task2 VARCHAR, task3 VARCHAR) AS
$$
BEGIN
    OPEN $1 FOR
        SELECT peer
        FROM ((SELECT peer
               FROM peer_completed_tasks()
               WHERE task1 = task)
              INTERSECT
              (SELECT peer
               FROM peer_completed_tasks()
               WHERE task2 = task)
              EXCEPT
              (SELECT peer
               FROM peer_completed_tasks()
               WHERE task3 = task)) AS choose_peer;
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_three_task('cur', 'C3_s21_string+', 'C7_SmartCalc_v1.0', 'CPP7_MLP');
FETCH ALL IN cur;
END;

BEGIN;
CALL prc_three_task('cur', 'DO1_Linux', 'DO2_Linux Network', 'C6_s21_matrix');
FETCH ALL IN cur;
END;

-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи выводится кол-во предшествующих ей задач

CREATE
    OR REPLACE PROCEDURE get_number_of_preceding_tasks(REFCURSOR)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN $1 FOR
    WITH RECURSIVE prev AS (SELECT (CASE WHEN parenttask ISNULL THEN 0 ELSE 1 END) cnt,
                                   title,
                                   parenttask                                      curren_task,
                                   parenttask
                            FROM tasks
                            UNION ALL
                            SELECT (CASE WHEN next.parenttask NOTNULL THEN cnt + 1 ELSE cnt END) cnt,
                                   next.title,
                                   next.parenttask                                               current_task,
                                   prev.title                                                    parrent_task
                            FROM tasks next
                                     CROSS JOIN prev
                            WHERE prev.title LIKE next.parenttask)
    SELECT title    Task,
           MAX(cnt) PrevCount
    FROM prev
    GROUP BY Task
    ORDER BY Task;
END;
$$;

BEGIN;
CALL get_number_of_preceding_tasks('cur');
FETCH ALL IN cur;
END;


-- 13) Производится поиск "удачных" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки

CREATE
    OR REPLACE PROCEDURE get_lucky_days_for_checks(REFCURSOR, N integer)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN $1 FOR
    WITH start AS (SELECT check_id, date, time
                   FROM p2p
                            JOIN checks c
                                 on c.id = p2p.check_id
                   WHERE state = 'Start')
       , num_check AS (SELECT ch.date,
                              s.time,
                              p.check_id,
                              coalesce(v.state, p.state)     state,
                              xpamount * 1.0 / t.maxxp * 1.0 check_xp,
                              row_number() over (partition by ch.date, coalesce(v.state, p.state)
                                  order by s.time) as        num
                       FROM checks ch
                                JOIN p2p p
                                     on ch.id = p.check_id
                                JOIN
                            start s
                            ON ch.id = s.check_id
                                LEFT JOIN verter v ON ch.id = v.check_id AND v.state != 'Start'
                                JOIN tasks t on t.title = ch.task
                                LEFT JOIN xp x on ch.id = x.check_id
                       WHERE p.state != 'Start')
    SELECT date
    FROM num_check
    WHERE state = 'Success'
      AND check_xp >= 0.8
      AND num >= N;
END;
$$;

BEGIN;
CALL get_lucky_days_for_checks('cur', 2);
FETCH ALL IN cur;
END;

-- 14) Определяется пир с наибольшим количеством XP

CREATE
    OR REPLACE FUNCTION get_peer_highest_amount_xp()
    RETURNS TABLE
            (
                peer varchar,
                xp   numeric
            )
AS
$$
BEGIN

    RETURN QUERY SELECT ch.peer, SUM(xpamount) xp
                 FROM checks ch
                          JOIN xp ON ch.id = xp.check_id
                 GROUP BY ch.peer
                 ORDER BY xp DESC
                 LIMIT 1;
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM get_peer_highest_amount_xp();

-- 15) Определяются пиры, приходившие раньше заданного времени не менее N раз за всё время

CREATE
    OR REPLACE PROCEDURE get_peer_came_before_given_time(REFCURSOR, given_time time, N integer)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN $1 FOR
    WITH enter AS (SELECT peer, date, time AS first_check_in
                   FROM timetracking
                   WHERE state = 1)
    SELECT peer
    FROM enter
    WHERE first_check_in < given_time
    GROUP BY peer
    HAVING COUNT(peer) > N;
END;
$$;

BEGIN;
CALL get_peer_came_before_given_time('cur', '12:00:00', 1);
FETCH ALL IN cur;
END;

-- 16) Определяются пиры, выходившие за последние N дней из кампуса больше M раз

CREATE
    OR REPLACE PROCEDURE get_peer_left_campus_last_N_days_more_than_M_times(REFCURSOR, N integer, M integer)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN $1 FOR
    SELECT peer
    FROM timetracking
    WHERE state = 2
      AND date > now():: DATE - N
    GROUP BY peer
    HAVING COUNT(1) > M;
END;
$$;

BEGIN;
CALL get_peer_left_campus_last_N_days_more_than_M_times('cur', 90, 2);
FETCH ALL IN cur;
END;

-- 17) Определяется для каждого месяца процент ранних входов

CREATE
    OR REPLACE PROCEDURE get_percentage_of_early_entries(REFCURSOR)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN $1 FOR
    WITH total_entries AS (SELECT TO_CHAR(p.birthday, 'Month') AS month, count(1) total
                           FROM peers AS p
                                    JOIN timetracking t
                                         ON p.peer = t.peer
                           WHERE state = 1
                             AND TO_CHAR(p.birthday
                                     , 'Month') = TO_CHAR(t.date
                                     , 'Month')
                           GROUP BY p.peer),
         early_entries AS (SELECT TO_CHAR(p.birthday, 'Month') as month, count(*) early
                           FROM peers AS p
                                    JOIN timetracking t
                                         ON p.peer = t.peer
                           WHERE state = 1
                             AND TO_CHAR(p.birthday
                                     , 'Month') = TO_CHAR(t.date
                                     , 'Month')
                             AND t.time
                               < '12:00:00'
                           GROUP BY p.peer)
    SELECT e.month, (e.early::numeric * 100 / t.total) ::int EarlyEntries
    FROM early_entries e
             JOIN total_entries t ON e.month = t.month
    ORDER BY e.month;
END;
$$;

BEGIN;
CALL get_percentage_of_early_entries('cur');
FETCH ALL IN cur;
END;
