package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
	fmt.Println("Starting garbage collection...")
	flag.Parse()
	fmt.Println("Process older than:", *howOldDays)
	fmt.Println("Unlink:", *unlink)
	fmt.Println("Verbosity:", *verbosity)
	fmt.Println("Host:", *host)
	fmt.Println("DB_ROOT:", *dbRoot)
	dsn := fmt.Sprintf("panadm:pan123@tcp(%s:3306)/PANGEA?allowOldPasswords=1&parseTime=true&charset=utf8", *host)
	fmt.Println("DSN:", dsn)
	selectedProjects := flag.Args()
	fmt.Println("Selected projects: ", selectedProjects)

	db, err := p4db.Connect(dsn)
	defer db.Close()
	if err != nil {
		log.Fatal("Unable to connect to DB", err)
	}
	projects, err := db.ProjectsNamePath()
	if err != nil {
		log.Fatal("Failed to get projects list from DB", err)
	}
	// Filter projects so that only projects specified on the command line
	// are left, or all the projects in the case when command line is empty
	projectsToProcess := make([]p4db.NamePath, 0, len(projects))
	if projSet, isEmpty := makeProjectsSet(selectedProjects); !isEmpty {
		for _, p := range projects {
			if _, ok := projSet[p.Name]; ok {
				projectsToProcess = append(projectsToProcess, p)
			}
		}
	} else {
		projectsToProcess = projects
	}

	fmt.Println(projectsToProcess, len(projectsToProcess), cap(projectsToProcess))

	storageItemsChan = make(chan StorageItem)
	readyJobs = make(chan PathsSetPerProject)

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
		go storageItemsGenerator(0)
	}

	// Start fetching paths for each project to process, when project is ready
	//

	for _, projInfo := range projectsToProcess {
		if paths, err := buildSetOfPaths4Project(db, projInfo); err == nil {
			log.Println("sending job", paths.ProjectName, len(paths.Paths))
			readyJobs <- paths
		} else {
			log.Fatal(err)
		}
	}
	close(readyJobs)

	itemGeneratorsN.Wait()
	close(storageItemsChan)
	processorsN.Wait()
	log.Println("Finished! Total Gbytes processed: ", float64(atomic.LoadInt64(&totalBytesProcessed))/1.e9)
}

func storageItemsGenerator(i int) {
	log.Println("**ItemsGenerator started", i)
	for job := range readyJobs {
		walkProjectTree(job.ProjectName, job.ProjectPath, job.Paths)
	}
	log.Println("**ItemsGenerator finished", i)
	itemGeneratorsN.Done()
}

func finalProcessor(i int) {
	log.Println("**FinalProcessor started", i)
	for item := range storageItemsChan {
		if okToExpire(item.Path, item.Info) {
			log.Println("==Processor", i, item)
			atomic.AddInt64(&totalBytesProcessed, item.Info.Size())
		}
	}
	log.Println("==FinalProcessor", i, "finished")
	processorsN.Done()
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
	log.Println(info.Size(), *szLimit)
	if info.Size() < *szLimit {
		return false
	}
	if time.Since(info.ModTime()).Hours() < float64(*howOldDays*24) {
		return false
	}
	return true
}

func walkProjectTree(projName string, projDir string, activePaths PathsSet) (err error) {
	if err = os.Chdir(filepath.Join(*dbRoot, "PROJECTS", projDir)); err != nil {
		log.Println("Warning:", err)
		return
	}
	err = filepath.Walk(".", func(fPath string, info os.FileInfo, err error) error {
		fPath = path.Join(*dbRoot, "PROJECTS", projDir, fPath)
		if !activePaths[fPath] {
			storageItemsChan <- StorageItem{Path: fPath, Project: projName, Info: info}
		} else {
			log.Println("++Walk: found in active path", fPath)
		}
		return nil
	})
	return
}
