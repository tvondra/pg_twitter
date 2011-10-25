# createdb twitter;
# psql twitter;

# vytvoreni zakladni normalizovane struktury pro uzivatele

CREATE TABLE users (
  id         INT PRIMARY KEY,
  username   VARCHAR(32) 
);

CREATE TABLE followers (
  followed_id   INT REFERENCES users(id),
  follower_id   INT REFERENCES users(id),
  PRIMARY KEY (followed_id, follower_id)
);

-- Let's add some users, A and B
INSERT INTO users VALUES (1, 'A');
INSERT INTO users VALUES (2, 'B');

-- User A follows user B
INSERT INTO followers VALUES (2,1);

-- Let's add another user, C"
INSERT INTO users VALUES (3, 'C');

-- User C follows user B
INSERT INTO followers VALUES (2,3);

-- User A follows user C
INSERT INTO followers VALUES (3,1);

-- Display A's followers
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id) WHERE followed_id = 1;

-- Display B's followers
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id) WHERE followed_id = 2;

-- Display B's followers
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id) WHERE followed_id = 3;

-- Who is followed by both A and C?
SELECT u.* FROM users u JOIN followers f ON (f.followed_id = u.id) WHERE follower_id = 1
INTERSECT
SELECT u.* FROM users u JOIN followers f ON (f.followed_id = u.id) WHERE follower_id = 3;

-- Who is not followed back by C?
SELECT u.* FROM users u JOIN followers f ON (f.followed_id = u.id) WHERE follower_id = 3
MINUS
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id) WHERE followed_id = 3;


-- denormalizovane pres intarray contrib modul
DELETE FROM followers;
DELETE FROM users;

ALTER TABLE users ADD COLUMN followers  INT[]; -- sledovani uzivatelem
ALTER TABLE users ADD COLUMN followed  INT[];  -- uzivatel sleduje

-- alternativa
CREATE TABLE users (
  id         INT PRIMARY KEY,
  username   VARCHAR(32),
  followers  INT[],
  followed   INT[]
);

-- triggery
CREATE FUNCTION followers_insert() RETURNS trigger AS $$
BEGIN
  UPDATE users SET followers = followers + NEW.follower_id WHERE id = NEW.followed_id;
  UPDATE users SET followed = followed + NEW.followed_id WHERE id = NEW.follower_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION followers_delete() RETURNS trigger AS $$
BEGIN
  UPDATE users SET followers = followers - OLD.follower_id WHERE id = OLD.followed_id;
  UPDATE users SET followed = followed - OLD.followed_id WHERE id = OLD.follower_id;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER followers_ins AFTER INSERT ON followers FOR EACH ROW EXECUTE PROCEDURE followers_insert();
CREATE TRIGGER followers_ins AFTER DELETE ON followers FOR EACH ROW EXECUTE PROCEDURE followers_delete();

-- znovu vlozim data 
INSERT INTO users VALUES (1, 'A');
INSERT INTO users VALUES (2, 'B');
INSERT INTO users VALUES (3, 'C');

INSERT INTO followers VALUES (2,1);
INSERT INTO followers VALUES (2,3);
INSERT INTO followers VALUES (3,1);

SELECT u.followers FROM users u WHERE followed_id = 1;
SELECT u.followers FROM users u WHERE followed_id = 2;
SELECT u.followers FROM users u WHERE followed_id = 3;

-- Who is followed by both A and C?
SELECT (SELECT u.followers FROM users u WHERE id = 1) & (SELECT u.followers FROM users u WHERE id = 3);

-- Who is not followed back by C?
SELECT (SELECT u.followed FROM users u WHERE id = 3) - (SELECT u.followers FROM users u WHERE id = 3);


-- messages
CREATE TABLE tweets (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id),
  tweet_time  TIMESTAMP NOT NULL DEFAULT now(),
  message VARCHAR(255)
);

CREATE INDEX tweets_user_idx ON tweets(user_id);
CREATE INDEX tweets_time_idx ON tweets(tweet_time);

-- B tweetuje
INSERT INTO tweets(id, user_id, message) VALUES (2, 'message from B "ahoj"');

-- C tweetuje
INSERT INTO tweets(id, user_id, message) VALUES (3, 'message from C "ahoj"');

-- A tweetuje taky
INSERT INTO tweets(id, user_id, message) VALUES (1, 'message from A "ahoj"');



-- normalizovane
SELECT * FROM tweets WHERE user_id = 1
UNION
SELECT * FROM tweets JOIN followers ON (user_id = followed_id) WHERE follower_id = 1;

SELECT * FROM tweets WHERE user_id = 2
UNION
SELECT * FROM tweets JOIN followers ON (user_id = followed_id) WHERE follower_id = 2;

SELECT * FROM tweets WHERE user_id = 3
UNION
SELECT * FROM tweets JOIN followers ON (user_id = followed_id) WHERE follower_id = 3;




-- denormalizovane
-- tweety ktere vidi A
SELECT * FROM tweets WHERE user_id IN (SELECT (followed + id) FROM users WHERE id = 1);

-- tweety ktere vidi B
SELECT * FROM tweets WHERE user_id IN (SELECT (followed + id) FROM users WHERE id = 2);

-- tweety ktere vidi C
SELECT * FROM tweets WHERE user_id IN (SELECT (followed + id) FROM users WHERE id = 3);


-- denormalizovane

CREATE PROCEDURE publish_tweet(p_user_id INT, p_message VARCHAR) RETURNS void AS $$
DECLARE
  v_followers INT[];
  v_idx INT;
BEGIN

  INSERT INTO tweets (user_id, message) VALUES (p_user_id, p_message);

  SELECT followers INTO v_followers FROM users WHERE id = p_user_id;

  FOR v_idx IN 1 .. array_length(v_followers, 1) LOOP
    INSERT INTO tweets (user_id, message) VALUES (v_followers[v_idx], p_message);
  END LOOP;

  RETURN;

END;  
$$ LANGUAGE plpgsql;


SELECT publish_tweet(2, 'message from B "ahoj"');
SELECT publish_tweet(3, 'message from C "ahoj"');
SELECT publish_tweet(1, 'message from A "ahoj"');

-- tweety ktere vidi A
SELECT * FROM tweets WHERE user_id = 1;
SELECT * FROM tweets WHERE user_id = 2;
SELECT * FROM tweets WHERE user_id = 3;

