//
// Write aggregation results to the log
//  Replace this with a write to your custom data store
//

package main

import (
	"encoding/json"
	"fmt"
	"time"
)

//
// AggCounter - Counts aggregation record format. set as JSON.
//
type AggCounter struct {
	CampaignID  int64     `json:"campaignId"`
	CreativeID  int64     `json:"creativeId"`
	Interval    string    `json:"interval"`
	Region      string    `json:"region"`
	Timestamp   time.Time `json:"timestamp"`
	DbTimestamp time.Time `json:"dbTimestamp"`
	Bids        int64     `json:"bids"`
	Wins        int64     `json:"wins"`
	Pixels      int64     `json:"pixels"`
	Clicks      int64     `json:"clicks"`
}

//
// Print the aggregation record for the last interval
//
func writeAggregatedRecords(allkeys *map[RecordKey]struct{}) {
	log1 := logger.GetLogger("writeAggregatedRecords")
	for k := range *allkeys {
		log1.Debug(fmt.Sprintf("Writing entry key %v:", k))
		// Lock this recordkey's counters
		aggBids.Lock(k)
		aggWins.Lock(k)
		aggClicks.Lock(k)
		aggPixels.Lock(k)

		campaignID := k.CampaignID
		creativeID := k.CreativeID
		intervalStr := k.IntervalStr
		campaignRec := findCampaign(campaignID, creativeID)
		var intervalTime time.Time
		if aggBids[k].count > 0 {
			intervalTime = aggBids[k].intervalTm
		} else if aggWins[k].count > 0 {
			intervalTime = aggWins[k].intervalTm
		} else if aggPixels[k].count > 0 {
			intervalTime = aggPixels[k].intervalTm
		} else if aggClicks[k].count > 0 {
			intervalTime = aggClicks[k].intervalTm
		} else {
			intervalTime = time.Now()
		}
		// Create a aggregation record in JSON
		now := time.Now().UTC()
		aggrec := AggCounter{
			CampaignID:  campaignID,
			CreativeID:  creativeID,
			Interval:    intervalStr,
			Region:      campaignRec.Regions.String,
			Timestamp:   intervalTime,
			DbTimestamp: now,
			Bids:        aggBids[k].count,
			Wins:        aggWins[k].count,
			Pixels:      aggPixels[k].count,
			Clicks:      aggClicks[k].count,
		}
		jsonStr, _ := json.Marshal(aggrec)
		log1.Info(fmt.Sprintf("Agg record %s", jsonStr))

		// Delete the counter object for this recordkey since we've written it
		delete(aggBids, k)
		delete(aggWins, k)
		delete(aggPixels, k)
		delete(aggClicks, k)
		// Unlock counters
		aggBids.Unlock(k)
		aggWins.Unlock(k)
		aggClicks.Unlock(k)
		aggPixels.Unlock(k)
	}
	return
}
