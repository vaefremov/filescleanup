package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
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
)

// PathsSet describes set of valid (sctual) paths relative to *dbRoot
type PathsSet map[string]bool

// PathsSetPerProject Describes set of valid paths related to project
type PathsSetPerProject struct {
	ProjectName string
	ProjectPath string
	ProjectId   int64
	Paths       PathsSet
}

var (
	generalDbPaths      PathsSet = make(PathsSet)
	generalExpiredPaths PathsSet = make(PathsSet)
)

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

	projSet, isEmpty := makeProjectsSet(selectedProjects)

	db, err := p4db.Connect(dsn)
	defer db.Close()

	if err != nil {
		log.Fatal("Unable to connect to DB", err)
	}

	// Stage 1: build general set of paths that are not mentionned in the DB
	// by fetching list of active paths in project and then walking
	// into the project directory and finding files the meet expiration criteria
	// (size, extension, not belonging to META-INF)
	projects, err := db.ProjectsNamePath()
	if err != nil {
		log.Fatal("Failed to get projects list from DB", err)
	}
	for _, projInfo := range projects {
		_, isInArgs := projSet[projInfo.Name]
		if isEmpty || isInArgs {
			if paths, err := buildSetOfPaths4Project(db, projInfo); err == nil {
				// generalDbPaths.updatePahSet(paths.Paths)
				log.Println("Total: ", len(generalDbPaths), "Project: ", projInfo.Name, len(paths.Paths))
				if expired, err := findExpired(projInfo.Path, paths.Paths); err == nil {
					generalExpiredPaths.updatePahSet(expired)
				} else {
					log.Println("Warning: ", err, "during walking in ", projInfo.Path)
				}
			} else {
				log.Fatal("Error while fetching files list: ", err)
			}
		}
	}

	// Stage 2: Find all stray files that are found in DB_ROOT and
	// do not belong to any projects

	// Stage 3: make the clean-up basing on the files list built in stage 1
	makeCleanup()
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
	log.Println(time.Since(info.ModTime()).Hours(), float64(*howOldDays*24))
	if time.Since(info.ModTime()).Hours() < float64(*howOldDays*24) {
		return false
	}
	return true
}

func findExpired(projDir string, activePaths PathsSet) (res PathsSet, err error) {
	res = make(PathsSet)
	resChan := make(chan string)
	log.Println("Finding expired files in ", projDir)
	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("unable to access a path %q: %v\n", path, err)
			return filepath.SkipDir
		}
		if !info.IsDir() && okToExpire(path, info) {
			path = filepath.Join(*dbRoot, "PROJECTS", projDir, path)
			if _, ok := activePaths[path]; !ok {
				resChan <- path
			} else {
				log.Println("Active: ", path)
			}
		} else {
			log.Println("Skip: ", path)
		}
		return nil
	}
	go func() {
		defer close(resChan)
		if err = os.Chdir(filepath.Join(*dbRoot, "PROJECTS", projDir)); err != nil {
			return
		}
		err = filepath.Walk(".", walkFn)
	}()
	for p := range resChan {
		fp := filepath.Join(*dbRoot, "PROJECTS", projDir, p)
		res[fp] = true
	}
	return
}

func makeCleanup() (err error) {
	fmt.Println(len(generalDbPaths))
	for p := range generalExpiredPaths {
		fmt.Println(p)
	}
	fmt.Println(len(generalExpiredPaths))
	return
}

func (s *PathsSet) updatePahSet(otherSet PathsSet) {
	for k, v := range otherSet {
		(*s)[k] = v
	}
}
