-- 1) Написана процедура добавления P2P проверки

CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer varchar,
    checker_peer varchar,
    task_name varchar,
    p2p_status status,
    check_time time
)
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF p2p_status = 'Start'
    THEN
        IF (SELECT state
            FROM p2p
                     INNER JOIN checks
                                ON task = task_name
            WHERE checking_peer = checker_peer
            ORDER BY "time" DESC
            limit 1) = 'Start'
        THEN
            RAISE EXCEPTION 'Start record already exists';
        ELSE
            INSERT INTO checks(peer, task, "date")
            VALUES (checked_peer, task_name, now());
        END IF;
    ELSEIF (SELECT state
            FROM p2p
                     INNER JOIN checks
                                ON task = task_name
            WHERE checking_peer = checker_peer
            ORDER BY "time" DESC
            limit 1) <> 'Start'
    THEN
        RAISE EXCEPTION 'No start record';
    END IF;
    INSERT INTO p2p(check_id, checking_peer, state, "time")
    SELECT checks.id, checker_peer, p2p_status, now()
    FROM checks
    WHERE peer = checked_peer
      AND task = task_name
    ORDER BY "date" DESC
    limit 1;
END
$$;

-- 2) Написана процедура добавления проверки Verter'ом

CREATE PROCEDURE add_verter_check(checking_peer_ varchar, checking_task varchar, checking_status status,
                                  checking_time time)
    LANGUAGE plpgsql
AS
$$
DECLARE
    check_ bigint = (SELECT check_id
                     FROM checks
                              JOIN p2p p ON checks.id = p.check_id
                     WHERE task = checking_task
                       AND peer = checking_peer_
                       AND state = 'Success'
                     ORDER BY time DESC
                     LIMIT 1);
BEGIN
    IF (check_ IS NOT NULL) THEN
        IF (NOT EXISTS(SELECT check_id FROM verter WHERE check_id = check_ AND state = checking_status))
        THEN
            INSERT INTO Verter (check_id, state, time)
            VALUES (check_, checking_status, checking_time);
        END IF;
    END IF;
END;
$$
;

CALL add_verter_check('dariobla', 'C2_SimpleBashUtils', 'Start', '20:00'); --exception
CALL add_verter_check('badaluda', 'C2_SimpleBashUtils', 'Start', '10:00'); --success
CALL add_verter_check('badaluda', 'C2_SimpleBashUtils', 'Success', '10:01'); --success

-- 3) Написан триггер: после добавления записи со статутом "начало" в таблицу P2P,
-- изменяется соответствующая запись в таблице TransferredPoints

CREATE OR REPLACE FUNCTION fnc_trg_p2p_start_insert()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.state = 'Start' THEN
        IF EXISTS(SELECT *
                  FROM transferredpoints
                  WHERE checking_peer = NEW.checking_peer
                    AND checked_peer =
                        (SELECT peer
                         FROM checks
                                  INNER JOIN p2p ON p2p.check_id = checks.id
                         WHERE p2p.id = NEW.id))
        THEN
            UPDATE transferredpoints
            SET points_amount = points_amount + 1
            WHERE checking_peer = NEW.checking_peer
              AND checked_peer =
                  (SELECT peer
                   FROM checks
                            INNER JOIN p2p ON p2p.check_id = checks.id
                   WHERE p2p.id = NEW.id);
        ELSE
            INSERT INTO transferredpoints(checking_peer, checked_peer, points_amount)
            SELECT NEW.checking_peer, checks.peer, 1
            FROM checks
                     INNER JOIN p2p ON p2p.check_id = checks.id
            WHERE p2p.id = NEW.id;
        END IF;
    END IF;
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p_insert
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE fnc_trg_p2p_start_insert();


CALL add_p2p_check('dariobla', 'priluruv', 'C2_SimpleBashUtils', 'Failure', '20:00'); --exception
CALL add_p2p_check('dariobla', 'priluruv', 'C2_SimpleBashUtils', 'Start', '20:00'); --success
CALL add_p2p_check('dariobla', 'priluruv', 'C2_SimpleBashUtils', 'Start', '20:00'); --exception
CALL add_p2p_check('dariobla', 'priluruv', 'C2_SimpleBashUtils', 'Failure', '21:00'); --success

CALL add_p2p_check('badaluda', 'dariobla', 'C2_SimpleBashUtils', 'Start', '10:00'); --no record in tp
CALL add_p2p_check('badaluda', 'dariobla', 'C2_SimpleBashUtils', 'Success', '10:00');

-- 4) Написан триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи

CREATE
    OR REPLACE FUNCTION fnc_insert_check_xp() RETURNS TRIGGER AS
$$
BEGIN
    IF (SELECT state
        FROM p2p p
        WHERE p.check_id = new.check_id
          AND p.state IN ('Success', 'Failure')) = 'Failure' THEN
        RAISE EXCEPTION 'Запись не прошла проверку p2p';
    ELSEIF (SELECT state
            FROM verter v
            WHERE v.check_id = new.check_id
              AND v.state IN ('Success', 'Failure')) = 'Failure' THEN
        RAISE EXCEPTION 'Запись не прошла проверку verter';
    ELSEIF (SELECT maxxp
            FROM checks c
                     JOIN tasks t ON t.title = c.task
            WHERE c.id = new.check_id) < new.xpamount THEN
        RAISE EXCEPTION 'Количество XP превышает максимальное доступное для проверяемой задачи';
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_insert_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_insert_check_xp();

DROP FUNCTION IF EXISTS fnc_insert_check_xp() CASCADE;

SELECT *
FROM xp;
INSERT INTO xp (id, check_id, xpamount)
VALUES (23, 1, 250); -- Проверка полного прохождения критериев
INSERT INTO xp (id, check_id, xpamount)
VALUES (24, 3, 400); -- Проверка прохождения p2p
INSERT INTO xp (id, check_id, xpamount)
VALUES (25, 2, 250); -- Проверка прохождения verter
INSERT INTO xp (id, check_id, xpamount)
VALUES (26, 1, 333); -- Проверка прохождения очков

DELETE FROM xp WHERE id > 22;