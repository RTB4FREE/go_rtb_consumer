//
// Read the Campaign Manager MySQL Database
//  Do this if you need to decorate your aggregation log records with additional campaign information.
//

package main

import (
	mysqlpkg "database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// CampaignBannerFields - Joined fields Campaigns and Banners
//	Add other database fields as required.
type CampaignBannerFields struct {
	ID       int64
	BannerID int64
	Regions  mysqlpkg.NullString
}

// CampaignVideoFields - Joined fields Campaigns and BannerVideos
//	Add other database fields as required.
type CampaignVideoFields struct {
	ID      int64
	VideoID int64
	Regions mysqlpkg.NullString
}

// CampaignCreativeFields - generic campaign and creative fields
type CampaignCreativeFields struct {
	Regions mysqlpkg.NullString
}

// CampaignBanners - Array of records with structure CampaignBannerFields
type CampaignBanners []CampaignBannerFields

// CampaignVideos - Array of records with structure CampaignVideoFields
type CampaignVideos []CampaignVideoFields

// Instantiate the banner and video objects
var dbCampaignBanners = CampaignBanners{}
var dbCampaignVideos = CampaignVideos{}

//
// Read the rtb4free mysql database and set the banner/vidoe objects.
func readMySQLTables(mysqlHost string, mysqlDbname string, mysqlUser string, mysqlPassword string) bool {
	log1 := logger.GetLogger("readMySQLTables")
	var retError = false
	var dsn = mysqlUser + ":" + mysqlPassword + "@tcp(" + mysqlHost + ")/" + mysqlDbname
	db, err := mysqlpkg.Open("mysql", dsn)
	if err != nil {
		log1.Error(err.Error())
		return true
	}
	defer db.Close()

	iface, _ := executeMySQLSelect(db, "select campaigns.id,banners.id as banner_id,campaigns.regions from banners, campaigns where banners.campaign_id=campaigns.id AND campaigns.status=\"runnable\"", "campaign_banner") //  c1x table
	if camprecs, ok := iface.([]CampaignBannerFields); ok {
		dbCampaignBanners = camprecs
	} else {
		return true
	}

	iface, _ = executeMySQLSelect(db, "select campaigns.id,videos.id as video_id,campaigns.regions from banner_videos as videos, campaigns where videos.campaign_id=campaigns.id AND campaigns.status=\"runnable\"", "campaign_video") //  c1x table
	if camprecs, ok := iface.([]CampaignVideoFields); ok {
		dbCampaignVideos = camprecs
	} else {
		return true
	}

	return retError
}

// Execute an SQL statement
func executeMySQLSelect(db *mysqlpkg.DB, selectStmt string, rtype string) (interface{}, error) {
	log1 := logger.GetLogger("executeMySQLSelect")
	var rvals interface{}
	rows, err := db.Query(selectStmt)
	if err != nil {
		log1.Error(err.Error())
		return rvals, errors.New("Query error -" + selectStmt)
	}
	defer rows.Close()
	switch rtype {
	case "campaign_banner":
		recs := []CampaignBannerFields{}
		count := 0
		for rows.Next() {
			rec := CampaignBannerFields{}
			err = rows.Scan(&rec.ID, &rec.BannerID, &rec.Regions)
			if err != nil {
				log1.Error(err.Error())
				return rvals, errors.New("Row error on select -" + selectStmt)
			}
			recs = append(recs, rec)
			count++
		}
		rvals = recs
		if err = rows.Err(); err != nil {
			log1.Error(err.Error())
			return rvals, errors.New("Rows error on select -" + selectStmt)
		}
		log1.Info(fmt.Sprintf("%d Campaign-Banner records read.", count))
	case "campaign_video":
		recs := []CampaignVideoFields{}
		count := 0
		for rows.Next() {
			rec := CampaignVideoFields{}
			err = rows.Scan(&rec.ID, &rec.VideoID, &rec.Regions)
			if err != nil {
				log1.Error(err.Error())
				return rvals, errors.New("Row error on select -" + selectStmt)
			}
			recs = append(recs, rec)
			count++
		}
		rvals = recs
		if err = rows.Err(); err != nil {
			log1.Error(err.Error())
			return rvals, errors.New("Rows error on select -" + selectStmt)
		}
		log1.Info(fmt.Sprintf("%d Campaign-Video records read.", count))
	default:
		log1.Error("executeMySQLSelect can't find select type - ", rtype)
	}
	return rvals, nil
}

// Find the campaign and creative attributes, given the camp and creative ID
func findCampaign(campIDint int64, creatIDint int64) CampaignCreativeFields {
	for _, v := range dbCampaignBanners {
		if v.ID == creatIDint {
			return CampaignCreativeFields{
				Regions: v.Regions,
			}
		}
	}
	for _, v := range dbCampaignVideos {
		if v.ID == creatIDint {
			return CampaignCreativeFields{
				Regions: v.Regions,
			}
		}
	}
	if val, found := dbCampaignBanners.findID(campIDint, creatIDint); found {
		return val
	}
	if val, found := dbCampaignVideos.findID(campIDint, creatIDint); found {
		return val
	}
	return CampaignCreativeFields{}
}

func (campaigns CampaignBanners) findID(ID int64, BannerID int64) (CampaignCreativeFields, bool) {
	log1 := logger.GetLogger("CampaignBanners findID")
	log1.Debug(fmt.Sprintf("Look in campaign-banner records for id %d.", ID))
	for _, v := range campaigns {
		if v.ID == ID && v.BannerID == BannerID {
			return CampaignCreativeFields{
				Regions: v.Regions,
			}, true
		}
	}
	return CampaignCreativeFields{}, false
}

func (campaigns CampaignVideos) findID(ID int64, VideoID int64) (CampaignCreativeFields, bool) {
	log1 := logger.GetLogger("CampaignVideos findID")
	log1.Info(fmt.Sprintf("Look in campaign-video records for id %d.", ID))
	for _, v := range campaigns {
		if v.ID == ID && v.VideoID == VideoID {
			return CampaignCreativeFields{
				Regions: v.Regions,
			}, true
		}
	}
	return CampaignCreativeFields{}, false
}
