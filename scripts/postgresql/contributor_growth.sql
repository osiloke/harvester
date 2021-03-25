/*
 PostgreSQL
 Date: 08/06/2014 22:51:41 PM
*/

-- ----------------------------
--  Table structure for contributorgrowth
-- ----------------------------
DROP TABLE IF EXISTS "contributorgrowth";
CREATE TABLE "contributorgrowth" (
	"time" timestamp(6) NULL,
	"harvest_id" varchar(255) NOT NULL COLLATE "default",
	"territory" varchar(255) COLLATE "default",
	"network" varchar(75) COLLATE "default",
	"contributor_id" varchar(255) COLLATE "default",
	"likes" int8,
	"talking_about" int8,
	"were_here" int8,
	"checkins" int8,
	"views" int8,
	"status_updates" int8,
	"listed" int8,
	"favorites" int8,
	"followers" int8,
	"following" int8,
	"plus_ones" int8,
	"comments" int8
)
WITH (OIDS=FALSE);

-- ----------------------------
--  Primary key structure for table contributorgrowth
-- ----------------------------
ALTER TABLE "contributorgrowth" ADD PRIMARY KEY ("harvest_id") NOT DEFERRABLE INITIALLY IMMEDIATE;

-- ----------------------------
--  Indexes structure for table contributorgrowth
-- ----------------------------
CREATE INDEX  "cg_contributor_id_key" ON "contributorgrowth" USING btree(contributor_id COLLATE "default" DESC NULLS LAST);
CREATE INDEX  "cg_time_key" ON "contributorgrowth" USING btree("time" DESC NULLS LAST);

