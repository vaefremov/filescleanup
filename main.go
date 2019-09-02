package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/vaefremov/p4db"
)

var (
	howOldDays = flag.Int("days", 7, "Process files older than (days)")
	unlink     = flag.Bool("unlink", false, "Delete files")
	verbosity  = flag.Int("verbosity", 0, "Verbosity level (from 0 to 10)")
	host       = flag.String("host", "127.0.0.1", "Host to connect to")
	dbRoot     = flag.String("db_root", "/opt/PANGmisc/DB_ROOT", "Massive data root catalogue")
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
	generalStoragePaths PathsSet = make(PathsSet)
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

	db, err := p4db.Connect(dsn)
	defer db.Close()

	if err != nil {
		log.Fatal("Unable to connect to DB", err)
	}

	// Stage 1: build general set of paths that are mentionned in the DB
	projects, err := db.ProjectsNamePathWc("test2%")
	if err != nil {
		log.Fatal("Failed to get projects list from DB", err)
	}
	for _, projInfo := range projects {
		if paths, err := buildSetOfPaths4Project(db, projInfo); err == nil {
			generalDbPaths.updatePahSet(paths.Paths)
		} else {
			log.Println("Warning: ", err)
		}
	}
	// Stage 2: build set of paths that are found in active project's catalogues
	// (and subcatalogues)

	for _, projInfo := range projects {
		if s, err := walkTree(projInfo.Path); err == nil {
			generalStoragePaths.updatePahSet(s)
		}
	}

	// Stage 3: Find all stray files that are found in DB_ROOT and
	// do not belong to any projects

	// Stage 4: make the clean-u(err error)p
	makeCleanup()
}

func projectsList(db *p4db.P4db) (res []string, err error) {
	if tmp, err := db.ProjectsNamePath(); err == nil {
		fmt.Println(tmp)
	}
	return
}

func buildSetOfPaths4Project(db *p4db.P4db, projInfo p4db.NamePath) (res PathsSetPerProject, err error) {
	pathsSet := make(PathsSet)
	res = PathsSetPerProject{ProjectName: projInfo.Name, ProjectId: projInfo.Id, ProjectPath: projInfo.Path, Paths: pathsSet}
	return
}

func walkFunc(path string, info os.FileInfo, err error) error {
	if err != nil {
		log.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
		return err
	}
	if !info.IsDir() {
		log.Printf("visited file or dir: %q\n", path)
		// fmt.Println(info.Name(), info.ModTime(), info.Size())
		generalStoragePaths[path] = true
	}
	return nil
}
func walkTree(projDir string) (res PathsSet, err error) {
	res = make(PathsSet)
	resChan := make(chan string)

	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("unable to access a path %q: %v\n", path, err)
			return filepath.SkipDir
		}
		if !info.IsDir() {
			resChan <- path
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
	fmt.Println(generalDbPaths)
	fmt.Println(generalStoragePaths)
	return
}

func (s *PathsSet) updatePahSet(otherSet PathsSet) {
	for k, v := range otherSet {
		(*s)[k] = v
	}
}
