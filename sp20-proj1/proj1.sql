DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v;

-- Question 0
CREATE VIEW q0(era) 
AS
   SELECT MAX(era)
   FROM pitching -- replace this line
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300 -- replace this line
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst like '% %' -- replace this line
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*) 
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear asc -- replace this line
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*) 
  FROM people
  GROUP BY birthyear
  HAVING AVG(height) > 70
  ORDER BY birthyear asc -- replace this line
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, HallofFame.playerid, HallofFame.yearid
  FROM  HallofFame, people
  WHERE people.playerid = HallofFame.playerid and HallofFame.inducted = 'Y'
  ORDER BY HallofFame.yearid desc -- replace this line
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, HallofFame.playerid, CollegePlaying.schoolid, HallofFame.yearid
  FROM  HallofFame, people, schools, CollegePlaying
  WHERE people.playerid = HallofFame.playerid 
  	and people.playerid = CollegePlaying.playerid
  	and CollegePlaying.schoolid = Schools.schoolid
  	and HallofFame.inducted = 'Y'
  	and Schools.schoolstate = 'CA'
  ORDER BY HallofFame.yearid desc, CollegePlaying.schoolid, HallofFame.playerid asc-- replace this line
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT  HallofFame.playerid, namefirst, namelast, CollegePlaying.schoolid 
	FROM  HallofFame, people
	LEFT JOIN CollegePlaying on people.playerid = CollegePlaying.playerid
	WHERE people.playerid = HallofFame.playerid  
  	and HallofFame.inducted = 'Y'
	ORDER BY HallofFame.playerid desc, CollegePlaying.schoolid asc -- replace this line
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT people.playerid, people.namefirst, people.namelast, b.yearid,
         (b.h - b.h2b - b.h3b - b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr) 
                / (cast(b.ab as real)) AS slg
  FROM people INNER JOIN batting as b ON people.playerid = b.playerid
  WHERE b.ab > 50
  ORDER BY slg DESC, b.yearid, people.playerid ASC
  LIMIT 10 -- replace this line
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT people.playerid, people.namefirst, people.namelast,
         SUM(b.h - b.h2b - b.h3b - b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr)
             / cast(SUM(b.ab) as real) as lslg
  FROM people INNER JOIN batting as b on b.playerid = people.playerid
  WHERE b.ab > 0
  GROUP BY people.playerid
  HAVING(SUM(b.ab) > 50)
  ORDER BY lslg DESC, people.playerid ASC
  LIMIT 10 -- replace this line
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  WITH Q AS (
      SELECT people.playerid, 
             SUM(b.h - b.h2b - b.h3b - b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr)
                / cast(SUM(b.ab) as real) as lslg
      FROM people
           INNER JOIN batting as b on b.playerid = people.playerid
      WHERE b.ab > 0
      GROUP BY people.playerid
      HAVING(SUM(b.ab) > 50))
  SELECT people.namefirst, people.namelast, q.lslg
  FROM people INNER JOIN Q AS q ON people.playerid = q.playerid
  WHERE q.lslg > (SELECT lslg FROM q WHERE playerid = 'mayswi01') -- replace this line
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
   SELECT yearid, MIN(salary), MAX(salary), AVG(salary), stddev(salary)
  	FROM salaries
  	GROUP BY yearid
  	ORDER BY yearid ASC -- replace this line
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH Q AS (SELECT MIN(salary), MAX(salary)
             FROM salaries WHERE yearid = '2016'
  			),
  	   R AS (SELECT i AS binid, 
                  i*(Q.max-Q.min)/10.0 + Q.min AS low,
                  (i+1)*(Q.max-Q.min)/10.0 + Q.min AS high
           FROM generate_series(0,9) AS i, Q)
  SELECT binid, low, high, COUNT(*) 
  FROM R INNER JOIN salaries 
         ON salaries.salary >= R.low 
            AND (salaries.salary < R.high OR binid = 9 AND salaries.salary <= R.high)
            AND yearid = '2016'
  GROUP BY binid, low, high
  ORDER BY binid ASC -- replace this line
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH Q AS (SELECT yearid, MIN(salary), MAX(salary), AVG(salary)
             FROM salaries GROUP BY yearid)
  SELECT y2.yearid,
         y2.min - y1.min AS mindiff,
         y2.max - y1.max AS maxdiff,
         y2.avg - y1.avg AS avgdiff
  FROM Q AS y1 INNER JOIN Q AS y2 ON y2.yearid = y1.yearid + 1
  ORDER BY y2.yearid ASC-- replace this line
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  WITH Q AS(SELECT yearid, MAX(salary) FROM salaries
            WHERE yearid IN (2000,2001)
            GROUP BY yearid)
  SELECT people.playerid, namefirst, namelast, salary, q.yearid
  FROM people 
       NATURAL JOIN salaries AS s
       INNER JOIN Q as q ON salary = q.max AND s.yearid = q.yearid -- replace this line
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  WITH Q AS(SELECT a.teamid, s.salary, a.playerid 
  			from AllStarFull as a 
  				join Salaries as s on a.playerid = s.playerid 
  			WHERE a.yearid = 2016
  			and s.yearid = 2016)
  SELECT Q.teamid, MAX(Q.salary) - MIN(Q.salary) as diffAvg
  from Q
  GROUP BY Q.teamid
  ORDER BY Q.teamid -- replace this line
;

