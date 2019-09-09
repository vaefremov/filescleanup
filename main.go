package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/op/go-logging"
	"github.com/vaefremov/p4db"
)

var (
	howOldDays = flag.Int("days", 7, "Process files older than (days)")
	unlink     = flag.Bool("unlink", false, "Delete files")
	verbosity  = flag.Int("verbosity", 0, "Verbosity level (from 0 to 10)")
	host       = flag.String("host", "127.0.0.1", "Host to connect to")
	dbRoot     = flag.String("db_root", "/opt/PANGmisc/DB_ROOT", "Massive data root catalogue")
	szLimit    = flag.Int64("sz_limit", 10000, "Consider only files larger than this parameter (bytes)")
	nProc      = flag.Int("n_proc", 5, "Number of processors")
)

var (
	log               = logging.MustGetLogger("filescleanup")
	format            = logging.MustStringFormatter(`%{time:02-01-2006 15:04:05} %{shortfunc} %{level:.3s} %{message}`)
	backend1          = logging.NewLogBackend(os.Stderr, "", 0)
	backend1Formatter = logging.NewBackendFormatter(backend1, format)
)

// PathsSet describes set of valid (sctual) paths relative to *dbRoot
type PathsSet map[string]bool

type StorageItem struct {
	Path    string
	Project string
	Info    os.FileInfo
}

// PathsSetPerProject Describes set of valid paths related to project
type PathsSetPerProject struct {
	ProjectName string
	ProjectPath string
	ProjectId   int64
	Paths       PathsSet
}

var storageItemsChan chan StorageItem
var readyJobs chan PathsSetPerProject
var itemGeneratorsN sync.WaitGroup
var processorsN sync.WaitGroup

var totalBytesProcessed int64

func main() {

	flag.Parse()
	setupVerbosity()

	log.Info("Process older than:", *howOldDays, "days")
	log.Info("Unlink:", *unlink)
	log.Info("Verbosity:", *verbosity)
	log.Info("Host:", *host)
	log.Info("DB_ROOT:", *dbRoot)
	dsn := fmt.Sprintf("panadm:pan123@tcp(%s:3306)/PANGEA?allowOldPasswords=1&parseTime=true&charset=utf8", *host)
	log.Info("DSN:", dsn)
	selectedProjects := flag.Args()
	log.Info("Selected projects: ", selectedProjects)
	log.Info("Number of processors:", *nProc)

	projectsToProcess, err := makeProjectsListToProcess(dsn, selectedProjects)
	if err != nil {
		log.Fatal(err)
	}

	log.Info(projectsToProcess, len(projectsToProcess), cap(projectsToProcess))

	storageItemsChan = make(chan StorageItem)
	readyJobs = make(chan PathsSetPerProject)

	// Start in the direcottory where static files are situalted
	if err = os.Chdir(filepath.Join(*dbRoot, "PROJECTS")); err != nil {
		log.Fatal("Error:", err)
	}

	// Start processors goroutines, each reading StorageItem from the corresponding
	// storageItemsChan
	for i := 0; i < *nProc; i++ {
		processorsN.Add(1)
		go finalProcessor(i)
	}

	// Start storage items generator goroutines, each will be waiting when a project ready to
	// process appears in the readyJobs channel. It will collect
	// files in the storage file system and put them to the storageItemsChan channel
	for i := 0; i < *nProc; i++ {
		itemGeneratorsN.Add(1)
		go storageItemsGenerator(i)
	}

	// Start fetching paths for each project to process, when project is ready
	//

	databaseFeeder(dsn, projectsToProcess)

	itemGeneratorsN.Wait()
	close(storageItemsChan)
	processorsN.Wait()
	log.Info("Finished! Total Gbytes processed: ", float64(atomic.LoadInt64(&totalBytesProcessed))/1.e9)
}

func storageItemsGenerator(i int) {
	log.Debug("**ItemsGenerator started", i)
	for job := range readyJobs {
		if _, err := os.Stat(job.ProjectPath); os.IsNotExist(err) {
			log.Warning("Warning:", err)
			continue
		}
		walkProjectTree(job.ProjectName, job.ProjectPath, job.Paths)
	}
	log.Debug("**ItemsGenerator finished", i)
	itemGeneratorsN.Done()
}

func finalProcessor(i int) {
	log.Debug("**FinalProcessor started", i)
	for item := range storageItemsChan {
		if okToExpire(item.Path, item.Info) {
			log.Debug("==Processor", i, item)
			atomic.AddInt64(&totalBytesProcessed, item.Info.Size())
		}
	}
	log.Debug("==FinalProcessor", i, "finished")
	processorsN.Done()
}

var nFeeders sync.WaitGroup

func databaseFeeder(dsn string, projectsToProcess []p4db.NamePath) (err error) {
	for _, projInfo := range projectsToProcess {
		nFeeders.Add(1)
		go databaseFeederForProjectStarter(dsn, projInfo)
	}
	nFeeders.Wait()
	close(readyJobs)
	return nil
}

var sema = make(chan struct{}, 5)

func databaseFeederForProjectStarter(dsn string, projInfo p4db.NamePath) {
	sema <- struct{}{}
	defer func() { <-sema }()
	defer func() {
		log.Debug("**** Feeder ended")
		nFeeders.Done()
	}()
	log.Debug("**** Feeder started")
	if err := databaseFeederForProject(dsn, projInfo); err != nil {
		log.Fatal("failed to create feeder for project ", projInfo.Name, err)
	}
}

func databaseFeederForProject(dsn string, projInfo p4db.NamePath) (err error) {
	db, err := p4db.New(dsn)
	if err != nil {
	}
	log.Debug("--Starting project", projInfo.Name)
	if paths, err := buildSetOfPaths4Project(db, projInfo); err == nil {
		log.Debug("--Sending job", paths.ProjectName, len(paths.Paths))
		readyJobs <- paths
	} else {
		log.Fatal(err)
	}
	return nil
}

func makeProjectsSet(projects []string) (res map[string]bool, isEmpty bool) {
	res = make(map[string]bool)
	isEmpty = true
	for _, p := range projects {
		res[p] = true
		isEmpty = false
	}
	return
}

func buildSetOfPaths4Project(db *p4db.P4db, projInfo p4db.NamePath) (res PathsSetPerProject, err error) {
	pathsSet := make(PathsSet)
	sqlTmpl := `select DataValue from DataValuesC as c,
	Containers as cn2
	where
	cn2.TopParent = ?
	and c.Status='Actual'
	and cn2.Status='Actual'
	and c.LinkContainer = cn2.CodeContainer 
	and c.LinkMetaData in (select CodeData from MetaData where KeyWord like 'Path')`
	if rows, err := db.C.Query(sqlTmpl, projInfo.Id); err == nil {
		var pPath string
		for rows.Next() {
			if err = rows.Scan(&pPath); err == nil {
				pPath = path.Join(*dbRoot, "PROJECTS", pPath)
				pathsSet[pPath] = true
			} else {
				return PathsSetPerProject{}, err
			}

		}
	}

	res = PathsSetPerProject{ProjectName: projInfo.Name, ProjectId: projInfo.Id, ProjectPath: projInfo.Path, Paths: pathsSet}
	return
}

func okToExpire(path string, info os.FileInfo) bool {
	if info.IsDir() {
		return false
	}
	dir, nm := filepath.Split(path)
	if dir == "META-INF" {
		return false
	}
	if filepath.Ext(nm) != ".dx" {
		return false
	}
	if info.Size() < *szLimit {
		return false
	}
	if time.Since(info.ModTime()).Hours() < float64(*howOldDays*24) {
		return false
	}
	return true
}

func walkProjectTree(projName string, projDir string, activePaths PathsSet) (err error) {
	err = filepath.Walk(projDir, func(fPath string, info os.FileInfo, err error) error {
		if info == nil {
			log.Fatal("Error: nil file info for ", fPath)
		}
		fPath = path.Join(*dbRoot, "PROJECTS", fPath)
		if !activePaths[fPath] {
			storageItemsChan <- StorageItem{Path: fPath, Project: projName, Info: info}
		} else {
			// log.Println("++Walk: found in active path", fPath)
		}
		return nil
	})
	return
}

func makeProjectsListToProcess(dsn string, cliProjects []string) (projectsToProcess []p4db.NamePath, err error) {
	db, err := p4db.New(dsn)
	if err != nil {
		log.Fatal("Unable to connect to DB", err)
	}

	db.C.DB.SetConnMaxLifetime(0)
	db.C.DB.SetMaxOpenConns(100)
	db.C.DB.SetMaxIdleConns(10)

	projects, err := db.ProjectsNamePath()
	if err != nil {
		log.Fatal("Failed to get projects list from DB", err)
	}
	// Filter projects so that only projects specified on the command line
	// are left, or all the projects in the case when command line is empty
	projectsToProcess = make([]p4db.NamePath, 0, len(projects))
	if projSet, isEmpty := makeProjectsSet(cliProjects); !isEmpty {
		for _, p := range projects {
			if _, ok := projSet[p.Name]; ok {
				projectsToProcess = append(projectsToProcess, p)
			}
		}
	} else {
		projectsToProcess = projects
	}
	return
}

// setupVerbosity selects logging backend and sets verbosity according to
// values stored in *verbosity
func setupVerbosity() {
	logging.SetBackend(backend1Formatter)
	switch {
	case *verbosity >= 8:
		logging.SetLevel(logging.DEBUG, "")
	case *verbosity >= 5:
		logging.SetLevel(logging.INFO, "")
	case *verbosity >= 4:
		logging.SetLevel(logging.NOTICE, "")
	case *verbosity >= 3:
		logging.SetLevel(logging.WARNING, "")
	case *verbosity >= 1:
		logging.SetLevel(logging.ERROR, "")
	case *verbosity == 0:
		logging.SetLevel(logging.CRITICAL, "")
	default:
		logging.SetLevel(logging.INFO, "")
	}

}
