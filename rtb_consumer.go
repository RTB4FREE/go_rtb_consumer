//
//   This is a sample program to read RTB4FREE event data that is published on Kafka.
//   The bid, win, pixel and click events are aggregated and printed every 5 minutues.
//   See rtb4free.com for complete information on the OpenRTB bidding platform.
//

package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	log "github.com/go-ozzo/ozzo-log"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

//
//  Define command line options and flags
//
var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka:9092").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
	// MySQL parameters for accessing campaign manager database
	mysqlHost     = kingpin.Flag("mysqlHost", "MySQL database server host name.").Default("web_db").String()
	mysqlDbname   = kingpin.Flag("mysqlDbname", "MySQL database name.").Default("rtb4free").String()
	mysqlUser     = kingpin.Flag("mysqlUser", "MySQL database user id.").Default("ben").String()
	mysqlPassword = kingpin.Flag("mysqlPassword", "MySQL database password.").Default("test").String()

	debug = kingpin.Flag("debug", "Output debug messages.").Bool()
)

// CountFields - stores message count and time interval of message topic.
type CountFields struct {
	lock       *sync.Mutex // Mutex lock to prevent concurrent writes on this counter
	count      int64       // Count field, increments for each occurrence of the bid, win, pixel, click
	intervalTs int64       // Epoch time in milleseconds timestamp for the interval.
	intervalTm time.Time   // Time object timestamp for the interval.
}

// RecordKey - key values to be used as a map key. Will map to CountFields
// This is the unique identified for the count aggregation - ie, count for each campaign/creative's time interval.
type RecordKey struct {
	CampaignID  int64
	CreativeID  int64
	IntervalStr string
	IntervalTs  string
}

// OutputCounts - Map of unique interval keys to counts
// Within each interval, this will be created and the count incremented for the unique recordkey
type OutputCounts map[RecordKey]CountFields

// TIme interval for the aggregated record is every 5 minutes
const intervalStr string = "5m"
const intervalSecs int64 = 300

// RTB Counters - we want to count bids, wins, pixels and clicks
var (
	aggBids   OutputCounts
	aggWins   OutputCounts
	aggPixels OutputCounts
	aggClicks OutputCounts
)

// logger - Create custom logger for each function. Helps debug concurrency.
var logger *log.Logger

func main() {
	logger = log.NewLogger()
	t1 := log.NewConsoleTarget()
	logger.Targets = append(logger.Targets, t1)
	logger.Open()
	log1 := logger.GetLogger("main") // Set the logger "app" field to this functions.
	defer logger.Close()

	// Set variables from command line
	kingpin.Parse()

	// Override if environment variable defined
	// We need to override command line if deploying with Docker compose environment variables.
	//   env variable format: RTBAGG_<uppercase key>, ie RTBAGG_BROKERLIST

	if v := getEnvValue("brokerList"); v != "" {
		*brokerList = v
	}
	if v := getEnvValue("partition"); v != "" {
		*partition = v
	}
	if v := getEnvValue("offsetType"); v != "" {
		val, err := strconv.Atoi(v)
		if err == nil {
			*offsetType = val
		}
	}
	if v := getEnvValue("messageCountStart"); v != "" {
		val, err := strconv.Atoi(v)
		if err == nil {
			*messageCountStart = val
		}
	}
	if v := getEnvValue("mysqlHost"); v != "" {
		*mysqlHost = v
	}
	if v := getEnvValue("mysqlDbname"); v != "" {
		*mysqlDbname = v
	}
	if v := getEnvValue("mysqlUser"); v != "" {
		*mysqlUser = v
	}
	if v := getEnvValue("mysqlPassword"); v != "" {
		*mysqlPassword = v
	}
	if v := getEnvValue("debug"); v != "" {
		if v == "true" || v == "TRUE" {
			*debug = true
		} else {
			*debug = false
		}
	}
	brokers := strings.Split(*brokerList, ",")
	if *debug {
		logger.MaxLevel = log.LevelDebug
	} else {
		logger.MaxLevel = log.LevelInfo
	}
	log1.Info("Console output level is " + logger.MaxLevel.String())
	log1.Info(fmt.Sprintf("Looking for kafka brokers: %s", brokers))
	log1.Info(fmt.Sprintf("Read from db host: %s", *mysqlHost))

	// Initialize the counter maps
	aggBids = OutputCounts{}
	aggWins = OutputCounts{}
	aggPixels = OutputCounts{}
	aggClicks = OutputCounts{}

	// Read the MySQL table to get campaign and creative attributes
	err := readMySQLTables(*mysqlHost, *mysqlDbname, *mysqlUser, *mysqlPassword)
	if err {
		log1.Alert("MySQL error on initial read.")
		panic("MySQL error on initial read.") // Let docker restart to reread.  Need initial db to be set.
	}

	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions

	// Set up CTL-C to break program
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	//
	//

	// Channel to catch CTL-C
	doneCh := make(chan struct{})
	ticker := time.NewTicker(time.Duration(intervalSecs) * time.Second)

	//Subscribe to Kafka topics
	go getTopic(config, brokers, []string{"bids"})
	go getTopic(config, brokers, []string{"wins"})
	go getTopic(config, brokers, []string{"pixels"})
	go getTopic(config, brokers, []string{"clicks"})

	// Wait for CTL-C. Will kill all go getTopic routines
	go func() {
		log1 := logger.GetLogger("main go")
		for {
			select {
			case <-signals:
				log1.Alert("Interrupt detected")
				// Drain remaining writes
				allkeys := make(map[RecordKey]struct{})
				var tsMs int64
				setMapKeys(&allkeys, &aggBids, tsMs, true)
				setMapKeys(&allkeys, &aggWins, tsMs, true) // cost keys are included in win keys
				setMapKeys(&allkeys, &aggPixels, tsMs, true)
				setMapKeys(&allkeys, &aggClicks, tsMs, true)

				writeAggregatedRecords(&allkeys)
				log1.Info("Finished sending remaining writes.")
				doneCh <- struct{}{}
			case <-ticker.C:
				log1.Info(fmt.Sprintf("\nTicker at %s.", time.Now()))
				writeLastInterval()
			}
		}
	}()
	<-doneCh
	log1.Info("End main")
}

//
// Calculate the aggregated records to be printed by examining the recordKeys.
// Then print only those records that have expired.
func writeLastInterval() {
	log1 := logger.GetLogger("writeLastInterval")

	// Set time value for history
	tsStr, tsMs, _ := intervalTimestamp(int64(int64(time.Now().Unix())*1000), intervalSecs)
	log1.Info(fmt.Sprintf("Interval Time stamp string: %s, %d", tsStr, tsMs))
	allkeys := make(map[RecordKey]struct{})
	tsMs -= intervalSecs * 1000 // Get the previous interval

	// Get all counters that are ready to print
	setMapKeys(&allkeys, &aggBids, tsMs, false)
	setMapKeys(&allkeys, &aggWins, tsMs, false)
	setMapKeys(&allkeys, &aggPixels, tsMs, false)
	setMapKeys(&allkeys, &aggClicks, tsMs, false)

	writeAggregatedRecords(&allkeys)
}

//
// Look at the recordKey for counters
//
func setMapKeys(set *map[RecordKey]struct{}, mymaps *OutputCounts, tsMs int64, sendAll bool) {
	for k, fields := range *mymaps {
		if sendAll || (fields.intervalTs <= tsMs) {
			(*set)[k] = struct{}{} // only set if timestamp >tsMs
		}
	}
	return
}

//
// Read the command line variables.
// Check if there is an ENV variable, and override if exists.
// This way we can set options in Docker compose files.
//
func getEnvValue(key string) string {
	log1 := logger.GetLogger("getEnvValue")
	envkey := "RTBAGG_" + strings.ToUpper(string(key))
	if v := os.Getenv(envkey); v != "" {
		log1.Info(fmt.Sprintf("Setting %s to env key %s value: %s", key, envkey, v))
		return v
	}
	return ""
}
