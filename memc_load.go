package main

import (
	"flag"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"os"
	"path/filepath"
)

var DEV_MEMC_CON = map[string]MemcacheStorage{}

type MemcacheStorage struct {
	Client *memcache.Client
}

func NewMemcacheStorage(addr string) *MemcacheStorage {
	mc := memcache.New(addr)
	return &MemcacheStorage{Client: mc}
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
	flag.StringVar(&pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "File pattern")
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

	fileList, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("Error finding files: %v", err)
	}

	//for _, filename := range fileList {
	//	processFile(filename, deviceMemc, dryRun)
	//}
}
