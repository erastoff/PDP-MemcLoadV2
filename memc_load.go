package main

import (
	"PDP-MemcLoadV2/appsinstalled"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// DevMemcCon Create map to store the opened Memcached connections
var DevMemcCon = make(map[string]*MemcachedStorage)
var normalErrRate = 0.01

// MemcachedStorage Struct for Memcached Storage
type MemcachedStorage struct {
	Client *memcache.Client
}

// NewMemcachedStorage creates new connection or get it from DevMemcCon
func NewMemcachedStorage(addr string) *MemcachedStorage {
	storage, exists := DevMemcCon[addr]
	if !exists {
		// If not exist, create new connection and add it into DevMemcCon map
		client := memcache.New(addr)
		storage = &MemcachedStorage{Client: client}
		DevMemcCon[addr] = storage
	}
	return storage

}

// AppsInstalled struct for pb file fields (namedtuple was used in Python)
type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []int
}

// main function defines shell arguments parsing and run file processing
func main() {
	var (
		test    bool
		logFile string
		dryRun  bool
		pattern string
		idfa    string
		gaid    string
		adid    string
		dvid    string
	)
	// arguments with default values
	flag.BoolVar(&test, "test", false, "Run tests")
	flag.StringVar(&logFile, "log", "", "Log file path")
	flag.BoolVar(&dryRun, "dry", false, "Dry run mode")
	flag.StringVar(&pattern, "pattern", "data/appsinstalled/*.tsv.gz", "File pattern")
	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "IDFA memcached address")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "GAID memcached address")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "ADID memcached address")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", "DVID memcached address")
	flag.Parse()

	// logger initialisation (if you specify file in the args, logs will be saved there)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	// devices initialization
	deviceMemc := map[string]string{
		"idfa": idfa,
		"gaid": gaid,
		"adid": adid,
		"dvid": dvid,
	}
	// fetch file list for pattern
	log.Printf("File list creating in pattern '%s'\n...\n", pattern)
	fileList, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("Error finding files: %v", err)
	}
	// concurrency with goroutine: file processing occurs for several files simultaneously
	var wg sync.WaitGroup
	for _, filename := range fileList {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			// target function
			processFile(filename, deviceMemc, dryRun)
		}(filename)
	}
	// waiting finishing all goroutines
	wg.Wait()
}

// processFile is a entry function for each file processing
func processFile(filePath string, deviceMemc map[string]string, dryRun bool) {
	var processed, errors int
	log.Printf("Processing %s", filePath)
	// file opening
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		log.Printf("Error creating gzip reader: %v", err)
		return
	}
	defer gzReader.Close()

	// scanning each line using bufio scanner
	scanner := bufio.NewScanner(gzReader)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		appsinstalled := parseAppsinstalled(line)
		if appsinstalled == nil {
			errors++
			continue
		}
		memcAddr, ok := deviceMemc[appsinstalled.DevType]
		if !ok {
			errors++
			log.Printf("Unknown device type: %s", appsinstalled.DevType)
			continue
		}
		ok = insertAppsinstalled(memcAddr, *appsinstalled, dryRun)
		if ok {
			processed++
		} else {
			errors++
		}
	}
	// dot prefix for processed file
	if processed == 0 {
		dotRename(filePath)
		return
	}
	// printing result of processing
	errRate := float64(errors) / float64(processed)
	if errRate < normalErrRate {
		log.Printf("Acceptable error rate (%.2f). Successful load", errRate)
	} else {
		log.Printf("High error rate (%.2f > %.2f). Failed load", errRate, normalErrRate)
	}
	dotRename(filePath)
}

// parseAppsinstalled is a func to parse each line of the target file. Return AppsInstalled struct
func parseAppsinstalled(line string) *AppsInstalled {
	lineParts := strings.Split(strings.TrimSpace(line), "\t")
	if len(lineParts) < 5 {
		return nil
	}
	devType, devID, latStr, lonStr, rawApps := lineParts[0], lineParts[1], lineParts[2], lineParts[3], lineParts[4]
	if devType == "" || devID == "" {
		return nil
	}
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		log.Printf("Invalid latitude: %v", err)
		return nil
	}
	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		log.Printf("Invalid longitude: %v", err)
		return nil
	}
	var apps []int
	for _, app := range strings.Split(rawApps, ",") {
		appID, err := strconv.Atoi(strings.TrimSpace(app))
		if err != nil {
			log.Printf("Invalid app ID: %v", err)
			continue
		}
		apps = append(apps, appID)
	}
	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     lat,
		Lon:     lon,
		Apps:    apps,
	}
}

// dotRename func to rename file after process completion
func dotRename(filePath string) {
	dir, file := filepath.Split(filePath)
	if err := os.Rename(filePath, filepath.Join(dir, "."+file)); err != nil {
		log.Printf("Error renaming file: %v", err)
	}
}

// insertAppsinstalled func to set line into Memcached connection
func insertAppsinstalled(memcAddr string, ai AppsInstalled, dryRun bool) bool {
	// transform lat and lon into pointers
	lat := ai.Lat
	lon := ai.Lon

	// transform apps values from []int into []uint32
	apps := make([]uint32, len(ai.Apps))
	for i, v := range ai.Apps {
		apps[i] = uint32(v)
	}
	ua := &appsinstalled.UserApps{
		Lat:  &lat,
		Lon:  &lon,
		Apps: apps,
	}
	key := fmt.Sprintf("%s:%s", ai.DevType, ai.DevID)
	// Serialization of UserApps struct
	packed, err := proto.Marshal(ua)
	if err != nil {
		log.Printf("Cannot marshal protobuf: %v", err)
		return false
	}
	// Define new connection and set new value
	memc := NewMemcachedStorage(memcAddr)
	if dryRun {
		log.Printf("%s - %s -> %v", memcAddr, key, ua)
	} else {
		item := &memcache.Item{Key: key, Value: packed}
		err := memc.Client.Set(item)
		if err != nil {
			log.Printf("Cannot write to memc %s: %v", memcAddr, err)
			return false
		}
	}
	return true
}
