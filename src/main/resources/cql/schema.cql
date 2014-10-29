CREATE KEYSPACE cassandra_spark_mlbdata WITH replication = {
'class': 'SimpleStrategy',
'replication_factor': '1'
};

use cassandra_spark_mlbdata;

CREATE TABLE allstarfull (
playerid text,
gameid text,
gamenum int,
gp int,
lgid text,
startingpos int,
teamid text,
yearid int,
PRIMARY KEY (playerid)
);

CREATE TABLE appearances_by_year_team (
yearid int,
teamid text,
playerid text,
g_1b int,
g_2b int,
g_3b int,
g_all int,
g_batting int,
g_c int,
g_cf int,
g_defense int,
g_dh int,
g_lf int,
g_of int,
g_p int,
g_ph int,
g_pr int,
g_rf int,
g_ss int,
gs int,
lgid text,
PRIMARY KEY ((yearid, teamid), playerid)
);

CREATE TABLE appearances_by_player_year (
playerid text,
yearid int,
teamid text,
g_1b int,
g_2b int,
g_3b int,
g_all int,
g_batting int,
g_c int,
g_cf int,
g_defense int,
g_dh int,
g_lf int,
g_of int,
g_p int,
g_ph int,
g_pr int,
g_rf int,
g_ss int,
gs int,
lgid text,
PRIMARY KEY ((playerid, yearid), teamid)
);

CREATE TABLE awardsmanagers (
playerid text,
awardid text,
yearid int,
lgid text,
notes text,
tie text,
PRIMARY KEY (playerid, awardid, yearid)
);

CREATE TABLE awardssharemanagers (
awardid text,
yearid int,
lgid text,
playerid text,
pointsmax int,
pointswon int,
votesfirst int,
PRIMARY KEY (awardid, yearid, lgid, playerid)
);

CREATE TABLE awardsshareplayers (
awardid text,
yearid int,
lgid text,
playerid text,
pointsmax float,
pointswon float,
votesfirst float,
PRIMARY KEY (awardid, yearid, lgid, playerid)
);

CREATE TABLE awardsplayers (
playerid text,
awardid text,
yearid int,
lgid text,
notes text,
tie text,
PRIMARY KEY (playerid, awardid, yearid)
);

CREATE TABLE batting (
playerid text,
yearid int,
stint int,
teamid text,
ab int,
bb int,
cs int,
g int,
g_batting int,
g_old int,
gidp int,
h int,
hbp int,
hr int,
ibb int,
lgid text,
r int,
rbi int,
sb int,
sf int,
sh int,
so int,
threeb int,
twob int,
PRIMARY KEY (playerid, yearid, stint, teamid)
);

CREATE TABLE battingpost (
yearid int,
round text,
playerid text,
ab int,
bb int,
cs int,
g int,
g_batting int,
gidp int,
h int,
hbp int,
hr int,
ibb int,
lgid text,
r int,
rbi int,
sb int,
sf int,
sh int,
so int,
teamid text,
threeb int,
twob int,
PRIMARY KEY (yearid, round, playerid)
);

CREATE TABLE fieldingof (
playerid text,
yearid int,
stint int,
gcf int,
glf int,
grf int,
PRIMARY KEY (playerid, yearid, stint)
);

CREATE TABLE fielding (
playerid text,
yearid int,
stint int,
a int,
cs int,
dp int,
e int,
g int,
gs int,
innouts int,
lgid text,
pb int,
po int,
pos int,
sb int,
teamid text,
wp int,
zr int,
PRIMARY KEY (playerid, yearid, stint)
);

CREATE TABLE halloffame (
playerid text,
yearid int,
ballots int,
category text,
inducted text,
need_note text,
needed int,
votedby text,
votes int,
PRIMARY KEY (playerid, yearid)
);

CREATE TABLE managers (
playerid text,
yearid int,
g int,
inseason int,
l int,
lgid text,
plyrmgr text,
rank int,
teamid text,
w int,
PRIMARY KEY (playerid, yearid)
);

CREATE TABLE managershalf (
playerid text,
yearid int,
teamid text,
g int,
half int,
inseason int,
l int,
lgid text,
rank int,
w int,
PRIMARY KEY (playerid, yearid, teamid)
);

CREATE TABLE master (
playerid text,
bats text,
bbrefid text,
birthcity text,
birthcountry text,
birthday int,
birthmonth int,
birthstate text,
birthyear int,
deathcity text,
deathcountry text,
deathday int,
deathmonth int,
deathstate text,
deathyear int,
debut text,
finalgame text,
height int,
namefirst text,
namegiven text,
namelast text,
retroid text,
throws text,
weight int,
PRIMARY KEY (playerid)
);

CREATE TABLE pitching (
playerid text,
baopp float,
bb int,
bfp int,
bk int,
cg int,
er int,
era float,
g int,
gf int,
gidp int,
gs int,
h int,
hbp int,
hr int,
ibb int,
ipouts int,
l int,
lgid text,
r int,
sf int,
sh int,
sho int,
so int,
stint int,
sv int,
teamid text,
w int,
wp int,
yearid int,
PRIMARY KEY (playerid)
);

CREATE TABLE pitchingpost (
playerid text,
baopp float,
bb int,
bfp int,
bk int,
cg int,
er int,
era float,
g int,
gf int,
gidp int,
gs int,
h int,
hbp int,
hr int,
ibb int,
ipouts int,
l int,
lgid text,
r int,
round text,
sf int,
sh int,
sho int,
so int,
sv int,
teamid text,
w int,
wp int,
yearid int,
PRIMARY KEY (playerid)
);

CREATE TABLE salaries (
yearid int,
teamid text,
playerid text,
lgid text,
salary int,
PRIMARY KEY (yearid, teamid, playerid)
);

CREATE TABLE schools (
schoolid text,
schoolcity text,
schoolname text,
schoolnick text,
schoolstate text,
PRIMARY KEY (schoolid)
);

CREATE TABLE schoolsplayers (
playerid text,
schoolid text,
yearmax int,
yearmin int,
PRIMARY KEY (playerid, schoolid)
);

CREATE TABLE seriespost (
yearid int,
round text,
lgidloser text,
lgidwinner text,
losses int,
teamidloser text,
teamidwinner text,
ties int,
wins int,
PRIMARY KEY (yearid, round)
);

CREATE TABLE teams (
yearid int,
lgid text,
teamid text,
ab int,
attendance int,
bb int,
bba int,
bpf int,
cg int,
cs int,
divid text,
divwin int,
dp int,
e int,
er int,
era int,
fp int,
franchid text,
g int,
ghome int,
h int,
ha int,
hbp int,
hr int,
hra int,
ipouts int,
l int,
lgwin int,
name text,
park text,
ppf int,
r int,
ra int,
rank int,
sb int,
sf int,
sho int,
so int,
soa int,
sv int,
teamidbr text,
teamidlahman45 text,
teamidretro text,
threeb int,
twob int,
w int,
wcwin int,
wswin int,
PRIMARY KEY (yearid, lgid, teamid)
);

CREATE TABLE teamsfranchises (
franchid text,
active text,
franchname text,
naassoc text,
PRIMARY KEY (franchid)
);

CREATE TABLE teamshalf (
yearid int,
lgid text,
teamid text,
divid text,
divwin int,
g int,
half int,
l int,
rank int,
w int,
PRIMARY KEY (yearid, lgid, teamid)
);