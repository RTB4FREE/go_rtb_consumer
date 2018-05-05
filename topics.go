//  Subscribe to the Kafka topics and read the JSON messages.
//  Create a counter entry for the timestamp and increment for each bid, win, pixel, click message.
//

package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
)

// BidFields - message format of Kafka bids topic
type BidFields struct {
	CampaignID int64   `json:"adid,string"`
	CreativeID int64   `json:"crid,string"`
	AdType     string  `json:"adtype"`
	Domain     string  `json:"domain"`
	Exchange   string  `json:"exchange"`
	Cost       float64 `json:"cost"`
	Timestamp  int64   `json:"timestamp"`
}

// WinFields - message format of Kafka wins topic
type WinFields struct {
	CampaignID int64   `json:"adId,string"`
	CreativeID int64   `json:"cridId,string"`
	AdType     string  `json:"adtype"`
	Exchange   string  `json:"pubId"`
	Cost       float64 `json:"cost,string"`
	Price      float64 `json:"price,string"`
	Timestamp  int64   `json:"timestamp"`
	Domain     string  `json:"domain"`
}

// PixelFields - message format of Kafka pixels topic
type PixelFields struct {
	CampaignID int64  `json:"ad_id,string"`
	CreativeID int64  `json:"creative_id,string"`
	AdType     string `json:"adtype"` // Not in msg
	Exchange   string `json:"exchange"`
	Timestamp  int64  `json:"timestamp"`
	Domain     string `json:"domain"`
}

// ClickFields - message format of Kafka clicks topic
type ClickFields struct {
	CampaignID int64  `json:"ad_id,string"`
	CreativeID int64  `json:"creative_id,string"`
	AdType     string `json:"adtype"` //Not in msg
	Exchange   string `json:"exchange"`
	Timestamp  int64  `json:"timestamp"`
	Domain     string `json:"domain"`
}

// Separate go routine for each topic
func getTopic(config *cluster.Config, brokers []string, topics []string) {
	log1 := logger.GetLogger("getTopic")
	log1.Info("Connect to brokers ", brokers, " topics ", topics)
	consumer, err2 := cluster.NewConsumer(brokers, "rtb-consumer-group-1", topics, config)
	if err2 != nil {
		log1.Alert(err2.Error())
		panic(err2)
	}
	//defer consumer.Close()
	go func() {
		log1 := logger.GetLogger("getTopic go func")
		for {
			select {
			case part, ok := <-consumer.Partitions():
				if !ok {
					log1.Error("consumer.Partions error.")
					return
				}
				// start a separate goroutine to consume messages
				go func(pc cluster.PartitionConsumer) {
					log1 := logger.GetLogger("getTopic kafka partition")
					for msg := range pc.Messages() {
						log1.Debug(fmt.Sprintf("%s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value))
						switch msg.Topic {
						case "bids":
							aggBids.addCount(msg.Topic, msg.Value)
						case "wins":
							aggWins.addCount(msg.Topic, msg.Value)
						case "pixels":
							aggPixels.addCount(msg.Topic, msg.Value)
						case "clicks":
							aggClicks.addCount(msg.Topic, msg.Value)
						default:
							log1.Alert(fmt.Sprintf("Unexpected topic %s.", msg.Topic))
							return
						}
						consumer.MarkOffset(msg, "") // mark message as processed
					}
				}(part)
			}
		}
	}()
	return
}

// Update counters
func (agg OutputCounts) addCount(topic string, msg []byte) {
	log1 := logger.GetLogger("addCount")
	var key RecordKey
	var ts string
	var tsMs int64
	var tm time.Time
	switch topic {
	case "bids":
		field := BidFields{}
		if err := json.Unmarshal(msg, &field); err != nil {
			logger.Error(fmt.Sprintf("JSON unmarshaling failed: %s", err))
			return
		}
		ts, tsMs, tm = intervalTimestamp(field.Timestamp, intervalSecs)
		// Create unique aggregation key
		key = RecordKey{
			CampaignID:  field.CampaignID,
			CreativeID:  field.CreativeID,
			IntervalStr: intervalStr,
			IntervalTs:  ts,
		}
	case "wins":
		field := WinFields{}
		if err := json.Unmarshal(msg, &field); err != nil {
			logger.Error(fmt.Sprintf("JSON unmarshaling failed: %s", err))
			return
		}
		ts, tsMs, tm = intervalTimestamp(field.Timestamp, intervalSecs)
		key = RecordKey{
			CampaignID:  field.CampaignID,
			CreativeID:  field.CreativeID,
			IntervalStr: intervalStr,
			IntervalTs:  ts,
		}
	case "pixels":
		field := PixelFields{}
		if err := json.Unmarshal(msg, &field); err != nil {
			logger.Error(fmt.Sprintf("JSON unmarshaling failed: %s", err))
			return
		}
		ts, tsMs, tm = intervalTimestamp(field.Timestamp, intervalSecs)
		key = RecordKey{
			CampaignID:  field.CampaignID,
			CreativeID:  field.CreativeID,
			IntervalStr: intervalStr,
			IntervalTs:  ts,
		}
	case "clicks":
		field := ClickFields{}
		if err := json.Unmarshal(msg, &field); err != nil {
			logger.Error(fmt.Sprintf("JSON unmarshaling failed: %s", err))
			return
		}
		ts, tsMs, tm = intervalTimestamp(field.Timestamp, intervalSecs)
		key = RecordKey{
			CampaignID:  field.CampaignID,
			CreativeID:  field.CreativeID,
			IntervalStr: intervalStr,
			IntervalTs:  ts,
		}
	default:
		log1.Alert(fmt.Sprintf("Unexpected topic %s.", topic))
		return

	}
	// Check if agg record already exists for this camp/creat/interval
	_, ok := agg[key]
	if !ok {
		// Doesn't exists so initialize
		agg[key] = CountFields{new(sync.Mutex), 0, tsMs, tm}
	}
	agg[key].lock.Lock() // mutex lock
	tmp := agg[key]      // increment count
	tmp.count++
	agg[key] = tmp
	agg[key].lock.Unlock() // mutex unlock

}

// Compute interval timestamp - string and epoch milliseconds
func intervalTimestamp(timestamp int64, interval int64) (string, int64, time.Time) {
	log1 := logger.GetLogger("intervalTimestamp")
	// Round off to interval
	timestamp = timestamp / 1000 // convert to epoch secs
	timestamp = int64(timestamp/interval) * interval
	tm := time.Unix(int64(timestamp), 0)
	str := tm.UTC().Format(time.RFC3339)
	epochms := int64(tm.Unix()) * 1000
	log1.Debug(fmt.Sprintf("Interval Time stamp string: %s, %d", str, epochms))
	return str, epochms, tm
}

// Lock counter entry
func (agg OutputCounts) Lock(key RecordKey) {
	if _, found := agg[key]; found {
		agg[key].lock.Lock()
	}
}

// Unlock counter entry
func (agg OutputCounts) Unlock(key RecordKey) {
	if _, found := agg[key]; found {
		agg[key].lock.Unlock()
	}
}
