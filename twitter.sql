/*
This file was used to demonstrate relational (PostgreSQL) approach to
the Twitter example, in constrast to a NoSQL (Redis) approach (available
here http://karmi.github.com/redis_twitter_example/). It's useful to see
both parts side-by side, to compare the approach.

The intent was not to prove that relational approach is the only correct
one, but to demonstrate the differences between relational and NoSQL
approaches, pros/cons etc.

The Twitter example was chosen because it's a de-facto hello world app
in the NoSQL world, it's quite simple to understand (everyone knows what
a tweet or follower is, etc.).

To run this file you'll need PostgreSQL (tested with 9.0, should work with
8.x releases) and intarray contrib module.
*/


/* create a database, install the intarray module and log-in (from a shell) */
$ createdb twitter;
$ psql twitter < `pg_config --sharedir`/contrib/_int.sql
$ psql twitter;


/***** USERS AND FOLLOWERS *****/

/* So let's play a bit with users and followers for a while (we'll talk
   about tweets later). */

/* So a user is simply a username and an ID - in reality there'd be probably
   and e-mail, date of the last log-in, full name, ... but we don't need that
   so we're using just (id,username).

   We don't want duplicate usernames, and we require the user to have a user
   name so we declare the column as "NOT NULL UNIQUE."

   So let's create the table.

*/
CREATE TABLE users (
  id         INT PRIMARY KEY,
  username   VARCHAR(32) NOT NULL UNIQUE
);

/* We'd like to track followers - each user can track other users and can be
   tracked by other users at the same time. That means we're talking about 
   many-to-many relationship and the correct way to represent this in a
   normalized database is a junction table.

   Every user can track each other user only once (he either tracks him or not,
   tracking twice does not make sense and it would cause some strange issues).

*/
CREATE TABLE followers (
  followed_id   INT REFERENCES users(id),
  follower_id   INT REFERENCES users(id),
  PRIMARY KEY (followed_id, follower_id)
);

CREATE INDEX follower_idx ON followers(follower_id);

/* That's all we need for tracking users and followers, so let's create three 
   users (A, B and C) and then define some followers. */
INSERT INTO users VALUES (1, 'A');
INSERT INTO users VALUES (2, 'B');
INSERT INTO users VALUES (3, 'C');

/* Now let's define that user A follows both B and C. */
INSERT INTO followers VALUES (2,1);
INSERT INTO followers VALUES (3,1);

/* And user C follows user B. */
INSERT INTO followers VALUES (2,3);

/* That means B is followed both by A and C but does not follow anyone. So let's
   play with the followers a bit ... */

/* Let's display A's followers first (no one follows A) */
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id)
WHERE followed_id = 1;

/* Now let's see who follows B (should be both A and C) */
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id)
WHERE followed_id = 2;

/* And finallyu let's see who follows C (should be A only) */
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id)
WHERE followed_id = 3;

/* Now we can do a bit of social analysis, and see who's followed both by A and C.
   As expected, it's B. */
SELECT u.* FROM users u JOIN followers f ON (f.followed_id = u.id)
WHERE follower_id = 1
  INTERSECT
SELECT u.* FROM users u JOIN followers f ON (f.followed_id = u.id)
WHERE follower_id = 3;

/* And now let's see who is not followed back by C, i.e. who follows C but C does
   not follow him? Obviously it has to be B, because C follows only B. */
SELECT u.* FROM users u JOIN followers f ON (f.followed_id = u.id)
WHERE follower_id = 3
  EXCEPT
SELECT u.* FROM users u JOIN followers f ON (f.follower_id = u.id)
WHERE followed_id = 3;


/* As you can see, this is actually quite simple - if you're familiar with the 
   concept of joining, it's actually quite natural. Everything is peachy as long
   as the number users and connections grows, the join may be quite expensive.

   Try for example this to create 100.000 users and about 10 connections for
   each of them (each user follows about 10 other users) using those commands

   --------------
   \set n 100000
   insert into users select i, md5(i::text) from generate_series(1,:n) s(i);
   insert into followers select distinct ceil(:n*random()), ceil(:n*random())
                           from generate_series(1,10*:n);
   analyze;
   --------------

   and then try to run the previous queries again. Not bad, but it gets worse
   as you add more joins.
*/

/***** USERS AND FOLLOWERS / DENORMALIZED *****/

/* It's possible to avoid this 'join' cost in most cases using intentional
   denormalization. Let's see how that could be done in this case. I'll add
   two more columns to the 'users' table - both will be int arrays, one will
   hold IDs of followed users, the other one will hold IDs of followers.
   And to get rid of the update anomalies those columns will be managed by
   two simple triggers on 'followers' table (and intarray contrib module).
*/

/* Let's delete the old data first - we'll have to reinsert them so that the
   triggers fire properly. */
DELETE FROM followers;
DELETE FROM users;

/* Then let's add the columns. */
ALTER TABLE users ADD COLUMN followers  INT[] NOT NULL DEFAULT '{}'::int[];
ALTER TABLE users ADD COLUMN followed  INT[] NOT NULL DEFAULT '{}'::int[];

/* So now the table looks like this. */
CREATE TABLE users (
  id         INT PRIMARY KEY,
  username   VARCHAR(32) NOT NULL UNIQUE,
  followers  INT[] NOT NULL DEFAULT '{}'::int[].
  followed   INT[] NOT NULL DEFAULT '{}'::int[]
);

/* And now let's create the triggers. The first one takes of INSERT and adds
   IDs to the proper arrays. */
CREATE OR REPLACE FUNCTION followers_insert() RETURNS trigger AS $$
BEGIN

  UPDATE users SET followers = followers + NEW.follower_id
   WHERE id = NEW.followed_id;

  UPDATE users SET followed = followed + NEW.followed_id 
   WHERE id = NEW.follower_id;

  RETURN NEW;

END;
$$ LANGUAGE plpgsql;

/* The second one takes care of DELETE and removes the IDs from the proper
   arrays. */
CREATE OR REPLACE FUNCTION followers_delete() RETURNS trigger AS $$
BEGIN

  UPDATE users SET followers = followers - OLD.follower_id
   WHERE id = OLD.followed_id;

  UPDATE users SET followed = followed - OLD.followed_id
   WHERE id = OLD.follower_id;

  RETURN OLD;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER followers_ins AFTER INSERT ON followers
       FOR EACH ROW EXECUTE PROCEDURE followers_insert();

CREATE TRIGGER followers_del AFTER DELETE ON followers
       FOR EACH ROW EXECUTE PROCEDURE followers_delete();


/* OK, lets insert the data users/connections again. */
INSERT INTO users VALUES (1, 'A');
INSERT INTO users VALUES (2, 'B');
INSERT INTO users VALUES (3, 'C');

INSERT INTO followers VALUES (2,1);
INSERT INTO followers VALUES (2,3);
INSERT INTO followers VALUES (3,1);

/* Now we can query the followers much easier. */
SELECT u.followers FROM users u WHERE id = 1;
SELECT u.followers FROM users u WHERE id = 2;
SELECT u.followers FROM users u WHERE id = 3;

/* Who is followed by both A and C? */
SELECT
   (SELECT u.followers FROM users u WHERE id = 1)
 & (SELECT u.followers FROM users u WHERE id = 3);

/* Who is not followed back by C? */
SELECT
   (SELECT u.followed FROM users u WHERE id = 3)
 - (SELECT u.followers FROM users u WHERE id = 3);

/* Obviously, everything comes at a price - this denormalization makes
   querying easier and usually much faster, but adding new followers is
   a bit more expensive (due to the triggers). But that's not a serious
   issue as that's less frequent operation than querying.

   A bit more serious problem is that currently there are no statistics
   on arrays, so the planning is not exactly flawless.
*/

/***** TWEETS *****/

/* Tweets are just short text messages submitted by users, so a very
   simple representation might look like this: */

CREATE TABLE tweets (
  id          SERIAL PRIMARY KEY,
  user_id     INT NOT NULL REFERENCES users(id),
  tweet_time  TIMESTAMP NOT NULL DEFAULT now(),
  message     VARCHAR(255) NOT NULL
);

/* And some indexes ... */
CREATE INDEX tweets_user_idx ON tweets(user_id);
CREATE INDEX tweets_time_idx ON tweets(tweet_time);

/* Let's create some tweets - each user (A, B and C) tweets once: */
INSERT INTO tweets(user_id, message) VALUES (1, 'message from A "ahoj"');
INSERT INTO tweets(user_id, message) VALUES (2, 'message from B "ahoj"');
INSERT INTO tweets(user_id, message) VALUES (3, 'message from C "ahoj"');

/* Fine, now lets see tweets for each user - each user can see his tweets and tweets
   from users he follows. */

/* It's simple to access tweets posted by a user */
SELECT t.* FROM tweets t WHERE user_id = 1 ORDER BY tweet_time DESC;

/* but tweets of the followers need to be accessed using a join */
SELECT t.* FROM tweets t JOIN followers ON (user_id = followed_id)
WHERE follower_id = 1 ORDER BY tweet_time DESC;

/* So to get both parts we can use UNION. The user A can see tweets of all other
   users (he's following both B and C) */
SELECT t.* FROM tweets t WHERE user_id = 1
  UNION
SELECT t.* FROM tweets t JOIN followers ON (user_id = followed_id)
WHERE follower_id = 1 ORDER BY tweet_time DESC;

/* The user B can see only his own tweets (because he's not following anyone) */
SELECT t.* FROM tweets t WHERE user_id = 2
UNION
SELECT t.* FROM tweets t JOIN followers ON (user_id = followed_id)
WHERE follower_id = 2 ORDER BY tweet_time DESC;

/* The user C can see tweets by B */
SELECT t.* FROM tweets t WHERE user_id = 3
UNION
SELECT t.* FROM tweets t JOIN followers ON (user_id = followed_id)
WHERE follower_id = 3 ORDER BY tweet_time DESC;

/* But we have already added followers/followed columns to the users table, so why not
   to use them instead of the join, and we can actully get rid of the union too. So for
   user A you can do
*/

SELECT t.* FROM tweets t JOIN (
  SELECT unnest(followed + id) uid FROM users WHERE id = 1
) foo ON (foo.uid = t.user_id) ORDER BY tweet_time DESC;

/* And for B or C you can do this */
SELECT t.* FROM tweets t JOIN (
  SELECT unnest(followed + id) uid FROM users WHERE id = 2
) foo ON (foo.uid = t.user_id) ORDER BY tweet_time DESC;

SELECT t.* FROM tweets t JOIN (
  SELECT unnest(followed + id) uid FROM users WHERE id = 3
) foo ON (foo.uid = t.user_id) ORDER BY tweet_time DESC;


/***** TWEETS / DENORMALIZED *****/

/* Still, this 'own tweets and tweets of all followed users' is a bit
   expensive. We can make it a bit cheaper by adding even more
   denormalization, this time in the tweets table. Instead of inserting
   one tweet and the querying the table with multiple user IDs, we'll
   create multiple additional copies - one for each follower.

   So the 'tweets' actually serves as an global inbox, user_id is used
   for the owner of the inbox (the recipient of the tweet). And we'll
   add one more column to keep track of the author (user who posted the
   tweet). We could add another ID to handle retweets, but we don't care.
*/

ALTER TABLE tweets ADD COLUMN author_id INT NOT NULL REFERENCES users(id);

/* And the tweets will be handled by a stored procedure (we could probably
   do that with a trigger) */

CREATE OR REPLACE FUNCTION publish_tweet(p_user_id INT, p_message VARCHAR) RETURNS void AS $$
DECLARE
  v_followers INT[];
  v_idx INT;
BEGIN

  INSERT INTO tweets (author_id, user_id, message) VALUES (p_user_id, p_user_id, p_message);

  SELECT followers INTO v_followers FROM users WHERE id = p_user_id;

  FOR v_idx IN 1 .. array_length(v_followers, 1) LOOP
    INSERT INTO tweets (author_id, user_id, message) VALUES (p_user_id, v_followers[v_idx], p_message);
  END LOOP;

  RETURN;

END;  
$$ LANGUAGE plpgsql;

/* Publishing the tweets is actually very simple - just call the procedure. */
SELECT publish_tweet(2, 'message from B "ahoj"');
SELECT publish_tweet(3, 'message from C "ahoj"');
SELECT publish_tweet(1, 'message from A "ahoj"');

/* And querying the tweets is very simple too - this time there's no join, no array
   magic. */
SELECT * FROM tweets WHERE user_id = 1;
SELECT * FROM tweets WHERE user_id = 2;
SELECT * FROM tweets WHERE user_id = 3;

/* But again, everything comes at a price - denormalization makes querying much
   easier and possibly more effective, but it means more data has to be stored. */
