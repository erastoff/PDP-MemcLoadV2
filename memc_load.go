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
)

var DEV_MEMC_CON = make(map[string]*MemcacheStorage)
var normalErrRate = 0.01

type MemcacheStorage struct {
	Client *memcache.Client
}

func NewMemcacheStorage(addr string) *MemcacheStorage {
	storage, exists := DEV_MEMC_CON[addr]
	if !exists {
		// Если не существует, создаем новый клиент и сохраняем его в мапе
		client := memcache.New(addr)
		storage = &MemcacheStorage{Client: client}
		DEV_MEMC_CON[addr] = storage
	}
	return storage
	//mc := memcache.New(addr)
	//return &MemcacheStorage{Client: mc}

}

type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []int
}

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
	flag.BoolVar(&test, "test", false, "Run tests")
	flag.StringVar(&logFile, "log", "", "Log file path")
	flag.BoolVar(&dryRun, "dry", false, "Dry run mode")
	flag.StringVar(&pattern, "pattern", "data/appsinstalled/*.tsv.gz", "File pattern")
	//flag.StringVar(&pattern, "pattern", "*.go", "File pattern")
	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "IDFA memcached address")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "GAID memcached address")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "ADID memcached address")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", "DVID memcached address")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	deviceMemc := map[string]string{
		"idfa": idfa,
		"gaid": gaid,
		"adid": adid,
		"dvid": dvid,
	}
	fmt.Printf("File list creating in pattert '%s'\n...\n", pattern)
	fileList, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("Error finding files: %v", err)
	}
	for _, filename := range fileList {
		processFile(filename, deviceMemc, dryRun)
	}
}

func processFile(filePath string, deviceMemc map[string]string, dryRun bool) {
	var processed, errors int
	//var errors int
	log.Printf("Processing %s", filePath)
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

	scanner := bufio.NewScanner(gzReader)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		appsinstalled := parseAppsinstalled(line)
		fmt.Println(appsinstalled)
		if appsinstalled == nil {
			errors++
			continue
		}
		memcAddr, ok := deviceMemc[appsinstalled.DevType]
		//_, ok := deviceMemc[appsinstalled.DevType]
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

	if processed == 0 {
		dotRename(filePath)
		return
	}
	errRate := float64(errors) / float64(processed)
	if errRate < normalErrRate {
		log.Printf("Acceptable error rate (%.2f). Successful load", errRate)
	} else {
		log.Printf("High error rate (%.2f > %.2f). Failed load", errRate, normalErrRate)
	}
	dotRename(filePath)
}

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

func dotRename(filePath string) {
	dir, file := filepath.Split(filePath)
	if err := os.Rename(filePath, filepath.Join(dir, "."+file)); err != nil {
		log.Printf("Error renaming file: %v", err)
	}
}

func insertAppsinstalled(memcAddr string, ai AppsInstalled, dryRun bool) bool {
	ua := &appsinstalled.UserApps{
		Lat:  ai.Lat,
		Lon:  ai.Lon,
		Apps: ai.Apps,
	}
	key := fmt.Sprintf("%s:%s", ai.DevType, ai.DevID)
	packed, err := proto.Marshal(ua)
	if err != nil {
		log.Printf("Cannot marshal protobuf: %v", err)
		return false
	}
	data := map[string][]byte{key: packed}
	memc := NewMemcacheStorage(memcAddr)
	if dryRun {
		log.Printf("%s - %s -> %s", memcAddr, key, ua)
	} else {
		err := memc.Client.Set(data)
		if err != nil {
			log.Printf("Cannot write to memc %s: %v", memcAddr, err)
			return false
		}
	}
	return true
}
