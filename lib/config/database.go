// Social Harvest is a social media analytics platform.
//     Copyright (C) 2014 Tom Maiaroto, Shift8Creative, LLC (http://www.socialharvest.io)
//
//     This program is free software: you can redistribute it and/or modify
//     it under the terms of the GNU General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
//
//     This program is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY; without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU General Public License for more details.
//
//     You should have received a copy of the GNU General Public License
//     along with this program.  If not, see <http://www.gnu.org/licenses/>.

package config

import (
	"encoding/json"
	"log"

	//"github.com/asaskevich/govalidator"
	_ "github.com/SocialHarvestVendors/pq"
	"github.com/SocialHarvestVendors/sqlx"
	"github.com/osiloke/dostow-contrib/api"

	// _ "github.com/mathume/monet"

	"reflect"
	"time"
)

type SocialHarvestDB struct {
	Postgres *sqlx.DB
	dostow   *api.Client
	Series   []string
	Schema   struct {
		Compact bool `json:"compact"`
	}
	RetentionDays int
	PartitionDays int
}

var database = SocialHarvestDB{}

// Settings Optional settings table/collection holds Social Harvest configurations and configured dashboards for persistence and clustered servers it is more or less a key value store.
// Data is stored as JSON string. The Social Harvest config JSON string should easily map to the SocialHarvestConf struct. Other values could be for JavaScript on the front-end.
type Settings struct {
	Key      string    `json:"id" db:"id" bson:"id"`
	Value    string    `json:"value" db:"value" bson:"value"`
	Modified time.Time `json:"modified" db:"modified" bson:"modified"`
}

// NewDatabase Initializes the database and returns the client, setting it to `database.Postgres` in the current package scope
func NewDatabase(config SocialHarvestConf) *SocialHarvestDB {
	// A database is not required to use Social Harvest
	if config.Database.Type == "" {
		return &database
	}
	database.dostow = api.NewAdminClient(config.Store.APIURL, config.Store.APIGroupKey, config.Store.APIToken)
	// var err error

	// Holds some options that will adjust the schema
	database.Schema = config.Schema
	// Data older than the (optional) retention period won't be stored.
	database.RetentionDays = config.Database.RetentionDays
	// Optional partitioning (useful for Postgres which has a PARTITION feature)
	database.PartitionDays = config.Database.PartitionDays

	// Keep a list of series (tables/collections/series - whatever the database calls them, we're going with series because we're really dealing with time with just about all our data)
	// These do relate to structures in lib/config/series.go
	database.Series = []string{"messages", "sharedlinks", "mentions", "hashtags", "contributorgrowth"}

	return &database
}

// SaveSettings Saves a settings key/value (Social Harvest config or dashboard settings, etc. - anything that needs configuration data can optionally store it using this function)
// TODO: Maybe just make this update the JSON file OR save to some sort of localstore so the settings don't go into the database where data is harvested
func (database *SocialHarvestDB) SaveSettings(settingsRow Settings) {
	var err error
	if len(settingsRow.Key) > 0 {
		_, err = database.dostow.Store.Update("settings", settingsRow.Key, map[string]interface{}{"value": settingsRow.Value})
		if err != nil {
			log.Println(err)
			return
		}
	} else {
		_, err = database.dostow.Store.Create("settings", map[string]interface{}{"value": settingsRow.Value})
		if err != nil {
			log.Println(err)
			return
		}

	}
	return
}

// SetLastHarvestTime Sets the last harvest time for a given action, value, network set.
// For example: "facebook" "publicPostsByKeyword" "searchKeyword" 1402260944
// We can use the time to pass to future searches, in Facebook's case, an "until" param
// that tells Facebook to not give us anything before the last harvest date...assuming we
// already have it for that particular search query. Multiple params separated by colon.
func (database *SocialHarvestDB) SetLastHarvestTime(territory string, network string, action string, value string, lastTimeHarvested time.Time, lastIdHarvested string, itemsHarvested int) {
	lastHarvestRow := SocialHarvestHarvest{
		Territory:         territory,
		Network:           network,
		Action:            action,
		Value:             value,
		LastTimeHarvested: lastTimeHarvested,
		LastIdHarvested:   lastIdHarvested,
		ItemsHarvested:    itemsHarvested,
		HarvestTime:       time.Now(),
	}

	//log.Println(lastTimeHarvested)
	database.StoreRow(lastHarvestRow)
}

// GetLastHarvestTime Gets the last harvest time for a given action, value, and network (NOTE: This doesn't necessarily need to have been set, it could be empty...check with time.IsZero()).
func (database *SocialHarvestDB) GetLastHarvestTime(territory string, network string, action string, value string) time.Time {
	var lastHarvestTime time.Time
	var lastHarvest SocialHarvestHarvest
	if database.Postgres != nil {
		database.Postgres.Get(&lastHarvest, "SELECT * FROM harvest WHERE network = $1 AND action = $2 AND value = $3 AND territory = $4", network, action, value, territory)
	}

	// log.Println(lastHarvest)
	lastHarvestTime = lastHarvest.LastTimeHarvested
	return lastHarvestTime
}

// GetLastHarvestID Gets the last harvest id for a given task, param, and network.
func (database *SocialHarvestDB) GetLastHarvestID(territory string, network string, action string, value string) string {
	var lastHarvest SocialHarvestHarvest
	// if database.Postgres != nil {
	// 	database.Postgres.Get(&lastHarvest, "SELECT * FROM harvest WHERE network = $1 AND action = $2 AND value = $3 AND territory = $4", network, action, value, territory)
	// }
	raw, err := database.dostow.Store.Search("harvest", api.Query(struct {
		Network   string `json:"network"`
		Action    string `json:"action"`
		Value     string `json:"value"`
		Territory string `json:"territory"`
	}{
		network, action, value, territory,
	}))
	if err == nil {
		err = json.Unmarshal(*raw, &lastHarvest)
		if err == nil {
			return lastHarvest.LastIdHarvested
		}
	}
	return ""
}

// StoreRow Stores a harvested row of data
func (database *SocialHarvestDB) StoreRow(row interface{}) {
	// A database connection is not required to use Social Harvest (could be logging to file)
	if database.dostow == nil {
		log.Println("There appears to be no store setup.")
		return
	}

	// If data is to be expired after a certain point, don't try to save data that is beyond that expiration (the harvester can pull in data from the past - sometimes months in the past)
	if database.RetentionDays > 0 {
		// Only certain series will have a "Time" field, if FieldByName("Time") was ran on the wrong row value, then it would panic and crash the application.
		// TODO: Rethink the use of an interface{} here because I worry about the performance with reflection. Or at least benchmark all this.
		// It would stink to have "StoreMessage" and "StoreSharedLink" etc. So interface{} was convenient... But also a little annoying.
		switch row.(type) {
		case SocialHarvestMessage, SocialHarvestSharedLink, SocialHarvestMention, SocialHarvestHashtag:
			v := reflect.ValueOf(row)
			rowTime := v.FieldByName("Time").Interface().(time.Time).Unix()
			now := time.Now().Unix()
			retentionSeconds := int64(database.RetentionDays * 86400)
			if rowTime <= (now - retentionSeconds) {
				// log.Println("Harvested data falls outside retention period.")
				return
			}
		}
	}
	var err error
	if database.dostow != nil {
		store := ""
		// Check if valid type to store and determine the proper table/collection based on it
		switch row.(type) {
		case SocialHarvestMessage:
			store = "messages"
		case SocialHarvestSharedLink:
			store = "sharedlinks"
		case SocialHarvestMention:
			store = "mentions"
		case SocialHarvestHashtag:
			store = "hashtags"
		case SocialHarvestContributorGrowth:
			store = "contributorgrowth"
		case SocialHarvestHarvest:
			store = "harvest"
		default:
			log.Println("trying to store unknown collection")
			return
		}
		_, err = database.dostow.Store.Create(store, row)
		if err != nil {
			log.Println(err)
		} else {
			log.Println("created " + store)
		}
	}
}

// HasAccess Checks access to the database
func (database *SocialHarvestDB) HasAccess() bool {
	var err error

	if database.Postgres != nil {
		var c int
		err = database.Postgres.Get(&c, "SELECT COUNT(*) FROM messages")
		if err == nil {
			return true
		} else {
			return false
		}
	}

	return false
}
