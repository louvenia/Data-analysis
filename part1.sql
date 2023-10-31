--enumeration type for the check status
CREATE TYPE status AS ENUM ('Start', 'Success', 'Failure');

--create Peers Table
CREATE TABLE IF NOT EXISTS Peers (
	peer varchar PRIMARY KEY,
	birthday date NOT NULL
    );

--create Tasks Table
CREATE TABLE IF NOT EXISTS Tasks
(
    Title      varchar not null primary key,
    ParentTask varchar,
    MaxXP      bigint  not null
    );

--create Checks Table
CREATE TABLE IF NOT EXISTS Checks
(
    id serial primary key,
    peer varchar not null REFERENCES Peers,
    task varchar not null REFERENCES Tasks,
    "date" date default now() not null
    );

--create Verter Table
CREATE TABLE IF NOT EXISTS Verter
(
    id       serial primary key,
    check_id bigint                  not null REFERENCES Checks,
    state    status                  not null,
    "time"   time default now() not null
    );

--create XP Table
CREATE TABLE IF NOT EXISTS XP
(
    id       serial primary key,
    check_id bigint not null REFERENCES Checks,
    XPAmount bigint not null
    );

--create P2P Table
CREATE TABLE IF NOT EXISTS P2P
(
    id serial primary key,
    check_id bigint not null REFERENCES Checks,
    checking_peer varchar not null REFERENCES Peers,
    state status not null,
    "time" time default now() not null
    );

--create TransferredPoints Table
CREATE TABLE IF NOT EXISTS TransferredPoints
(
    id serial primary key,
    checking_peer varchar not null REFERENCES Peers,
    checked_peer varchar not null REFERENCES Peers,
    points_amount int default 0
    );

--create Friends Table
CREATE TABLE IF NOT EXISTS Friends (
	id serial PRIMARY KEY,
	peer1 varchar NOT NULL,
	peer2 varchar NOT NULL,
	CONSTRAINT friends_peer1_fk
		FOREIGN KEY (peer1)
			REFERENCES Peers (peer),
	CONSTRAINT friends_peer2_fk
		FOREIGN KEY (peer2)
			REFERENCES Peers (peer),
	CONSTRAINT friends_peers_uq
		UNIQUE (peer1, peer2),
	CONSTRAINT friends_peers_ch
		CHECK (peer1 <> peer2)
    );

--create Recommendations Table
CREATE TABLE IF NOT EXISTS Recommendations (
	id serial PRIMARY KEY,
	peer varchar NOT NULL,
	recommendedpeer varchar,
	CONSTRAINT recommendations_peer_fk
		FOREIGN KEY (peer)
			REFERENCES Peers (peer),
	CONSTRAINT recommendations_recommendedpeer_fk
		FOREIGN KEY (recommendedpeer)
			REFERENCES Peers (peer),
	CONSTRAINT recommendations_peers_uq
		UNIQUE NULLS NOT DISTINCT(peer, recommendedpeer),
	CONSTRAINT recommendations_peers_ch
		CHECK (peer <> recommendedpeer)
    );

--create TimeTracking Table
CREATE TABLE IF NOT EXISTS TimeTracking (
	id serial PRIMARY KEY,
	peer varchar NOT NULL,
	"date" date NOT NULL,
	"time" time NOT NULL,
	State int NOT NULL,
	CONSTRAINT timetracking_peer_fk
			FOREIGN KEY (peer)
				REFERENCES Peers (peer),
	CONSTRAINT timetracking_state_ch
		CHECK (State = 1 OR State = 2)
);

CREATE OR REPLACE PROCEDURE from_csv(path text, separator char = ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE ('COPY Peers (peer, birthday) FROM '''|| path || '/peers.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Tasks (title, parenttask, maxxp) FROM '''|| path ||'/tasks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Checks (peer, task, "date") FROM '''|| path ||'/checks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Verter (check_id, state, "time") FROM '''|| path ||'/verter.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY XP (check_id, xpamount) FROM '''|| path ||'/xp.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY P2P (check_id, checking_peer, state, "time") FROM '''|| path ||'/p2p.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY TransferredPoints (checking_peer, checked_peer, points_amount) FROM '''|| path || '/transferred_points.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Friends (peer1, peer2) FROM '''|| path || '/friends.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Recommendations (peer, recommendedpeer) FROM '''|| path || '/recommendations.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY TimeTracking (peer, "date", "time", state) FROM '''|| path || '/timetracking.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
END
$$;

CREATE OR REPLACE PROCEDURE to_csv(path text, separator char = ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE ('COPY Peers (peer, birthday) TO '''|| path || '/peers.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Tasks (title, parenttask, maxxp) TO '''|| path ||'/tasks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Checks (peer, task, "date") TO '''|| path ||'/checks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Verter (check_id, state, "time") TO '''|| path ||'/verter.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY XP (check_id, xpamount) TO '''|| path ||'/xp.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY P2P (check_id, checking_peer, state, "time") TO '''|| path ||'/p2p.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY TransferredPoints (checking_peer, checked_peer, points_amount) TO '''|| path || '/transferred_points.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Friends (peer1, peer2) TO '''|| path || '/friends.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Recommendations (peer, recommendedpeer) TO '''|| path || '/recommendations.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY TimeTracking (peer, "date", "time", state) TO '''|| path || '/timetracking.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
END
$$;

CALL from_csv('/tmp/csv');