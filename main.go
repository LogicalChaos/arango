package main

import (
	"flag"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/davecgh/go-spew/spew"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type File struct {
	Name     string    `json:"name"`
	FileSize int64     `json:"size"`
	Modified time.Time `json:"created"`
}

type Directory struct {
	Path string `json:"path"`
}

type Contains struct {
	From string `json:"_from"`
	To   string `json:"_to"`
}

var (
	client      driver.Client
	db          driver.Database
	graph       driver.Graph
	edges       driver.Collection
	directories driver.Collection
	fileobjects driver.Collection
)

func main() {
	setupClient()
	setupDatabase(client)
	setupGraph()
	fileobjects = setupVertexCollection("fileobjects")
	directories = setupVertexCollection("directories")
	_, _, _ = fileobjects.EnsureHashIndex(nil, []string{"name"}, nil)
	_, _, _ = directories.EnsureHashIndex(nil, []string{"path"}, nil)

	commandPtr := flag.String("cmd", "", "truncate|clean|scan|count")
	pathPtr := flag.String("path", "./", "path to command")
	countPtr := flag.Int("count", 0, "run loop, adds a 3 digit suffix to prefix, only on scan")
	startPtr := flag.Int("start", 0, "start of loop counter, only used with count")
	prefixPtr := flag.String("prefix", "", "Prefix to add to each scan, like '/neo/scan1'")
	flag.Parse()

	root := filepath.Join(*prefixPtr, *pathPtr)
	switch *commandPtr {
	case "clean":
		deleteDirectoryRecursive(root)
		break
	case "truncate":
		_ = edges.Truncate(nil)
		_ = directories.Truncate(nil)
		_ = fileobjects.Truncate(nil)
		break
	case "scan":
		ds := GetDirectoryServer()
		ds.Start()
		defer ds.Stop()
		channel := ds.GetFileHandlerPayloadChannel()

		info, err := os.Stat(*pathPtr)
		if err != nil {
			log.Fatalf("could not stat %v: %v", *pathPtr, err)
		}

		loop := 1
		prefixer := func(x int) string { return *prefixPtr }
		if *countPtr > 0 {
			loop = *countPtr
			prefixer = func(x int) string {
				newPrefix := fmt.Sprintf("%s%03d", *prefixPtr, x+*startPtr)
				root = filepath.Join(newPrefix, *pathPtr)
				return newPrefix
			}
		}

		for i := 0; i < loop; i++ {
			prefix := prefixer(i)
			if len(prefix) > 0 {
				path := "/"
				pathElements := strings.Split(root, string(filepath.Separator))
				for _, element := range pathElements[1:] {
					path = filepath.Join(path, element)
					processDirectoryPayload(ds, FileHandlerPayload{info, path})
				}
			}
			err = filepath.Walk(*pathPtr, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				channel <- FileHandlerPayload{info, filepath.Join(prefix, path)}
				return nil
			})
			if err != nil {
				log.Fatalf("error walking directory: %v", err)
			}
		}

		for len(channel) > 0 {
			time.Sleep(time.Millisecond)
		}
		break
	case "count":
		categories := NewCategories()
		fillCategories(root, categories)
		_, _ = spew.Println(categories.CategoriesDto)
		break
	default:
		log.Fatalf("unrecognized command: %v", *commandPtr)
	}
}

func fillCategories(root string, categories *Categories) {
	filesFound := uint64(0)
	_, meta, err := getDirectory(root)
	if err != nil {
		log.Fatalf("failed querying directory %v: %v", root, err)
	}

	query := "FOR v IN 0..10000 OUTBOUND @start GRAPH 'contains' FILTER IS_SAME_COLLECTION('fileobjects', v) RETURN v"
	bindVars := map[string]interface{}{"start": meta.ID}
	cursor, err := db.Query(nil, query, bindVars)
	if err != nil {
		log.Fatalf("failed querying graph starting at %v: %v", root, err)
	}
	defer func() {
		err := cursor.Close()
		if err != nil {
			log.Printf("failed to close graph iterator: %v\n", err)
		}
	}()

	file := File{}
	for cursor.HasMore() {
		filesFound++
		_, err := cursor.ReadDocument(nil, &file)
		if err != nil {
			log.Printf("failed to read cursor: %v\n", err)
			continue
		}
		categories.CategorizeFile(&file.Modified, file.FileSize)
	}
	fmt.Printf("Found %d files\n", filesFound)
}

func setupVertexCollection(name string) driver.Collection {
	exists, _ := db.CollectionExists(nil, name)
	var collection driver.Collection
	var err error
	if exists {
		collection, err = graph.VertexCollection(nil, name)
		if err != nil {
			log.Fatalf("error getting collection %v: %v", name, err)
		}
	} else {
		collection, err = graph.CreateVertexCollection(nil, name)
		if err != nil {
			log.Fatalf("error creating collection %v: %v", name, err)
		}
	}
	return collection
}

func setupGraph() {
	var err error
	exists, _ := db.GraphExists(nil, "contains")
	if exists {
		graph, err = db.Graph(nil, "contains")
		if err != nil {
			log.Fatalf("error getting contains collection: %v", err)
		}
	} else {
		// define the edgeCollection to store the edges
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "contains"
		// define a set of collections where an edge is going out...
		edgeDefinition.From = []string{"directories"}
		// repeat this for the collections where an edge is going into
		edgeDefinition.To = []string{"directories", "fileobjects"}
		// A graph can contain additional vertex collections, defined in the set of orphan collections
		var options driver.CreateGraphOptions
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}
		//now it's possible to create a graph
		var err error
		graph, err = db.CreateGraph(nil, "contains", &options)
		if err != nil {
			log.Fatalf("error connecting to contains collection: %v", err)
		}
	}
	edges, _, err = graph.EdgeCollection(nil, "contains")
	if err != nil {
		log.Fatalf("error getting edges collection: %v", err)
	}
}

func setupDatabase(c driver.Client) {
	var err error
	databases, err := c.AccessibleDatabases(nil)
	if err != nil {
		log.Fatalf("error connecting to db <neo>: %v", err)
	}
	for _, database := range databases {
		if database.Name() == "neo" {
			db, err = c.Database(nil, "neo")
			if err != nil {
				log.Fatalf("error connecting to db <neo>: %v", err)
			}
			return
		}
	}
	db, err = c.CreateDatabase(nil, "neo", nil)
	if err != nil {
		log.Fatalf("error connecting to db <neo>: %v", err)
	}
}

func setupClient() {
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})
	if err != nil {
		log.Fatalf("error creating client: %v", err)
	}
	c, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication("root", ""),
	})
	if err != nil {
		log.Fatalf("error creating client: %v", err)
	}
	client = c
}

func getDirectory(path string) (Directory, driver.DocumentMeta, error) {
	query := "FOR d IN directories FILTER d.path == @name RETURN d"
	bindVars := map[string]interface{}{"name": path}
	cursor, err := db.Query(nil, query, bindVars)
	if err != nil {
		return Directory{}, driver.DocumentMeta{}, fmt.Errorf("failed querying directory %v: %v", path, err)
	}
	defer func() { _ = cursor.Close() }()
	directory := Directory{}

	if !cursor.HasMore() {
		return Directory{}, driver.DocumentMeta{}, fmt.Errorf("no directory %v", path)
	}

	metaParent, err := cursor.ReadDocument(nil, &directory)
	if err != nil {
		return Directory{}, driver.DocumentMeta{}, fmt.Errorf("failed reading cursor: %v", err)
	}
	return directory, metaParent, nil
}

func deleteDirectoryRecursive(root string) {
	log.Printf("deleting directory entries start %s\n", root)
	removed := uint64(0)
	defer func() {
		log.Printf("deleting directory entries complete (%d) %s\n", removed, root)
	}()

	_, meta, err := getDirectory(root)
	if err != nil {
		log.Fatalf("failed querying directory %v: %v", root, err)
	}

	query := "FOR v IN 0..10000 OUTBOUND @start GRAPH 'contains' FILTER IS_SAME_COLLECTION('directories', v) RETURN v"
	bindVars := map[string]interface{}{"start": meta.ID}
	cursor, err := db.Query(nil, query, bindVars)
	if err != nil {
		log.Fatalf("failed querying graph starting at %v: %v", root, err)
	}
	defer func() {
		err := cursor.Close()
		if err != nil {
			log.Printf("failed to close directory iterator: %v\n", err)
		}
	}()

	directory := Directory{}
	for cursor.HasMore() {
		removed++
		meta, err := cursor.ReadDocument(nil, &directory)
		if err != nil {
			log.Printf("failed to read cursor: %v\n", err)
			continue
		}
		_, err = directories.RemoveDocument(nil, meta.Key)
		if err != nil {
			log.Printf("failed to remove : %v\n", err)
			continue
		}
	}
}

func getEdge(edge Contains) (driver.DocumentMeta, error) {
	query := "FOR e IN contains FILTER e._from == @from AND e._to == @to RETURN e"
	bindVars := map[string]interface{}{"from": edge.From, "to": edge.To}
	cursor, err := db.Query(nil, query, bindVars)
	if err != nil {
		return driver.DocumentMeta{}, fmt.Errorf("failed querying edge %v: %v", edge, err)
	}
	defer func() { _ = cursor.Close() }()
	contains := Contains{}

	if !cursor.HasMore() {
		return driver.DocumentMeta{}, fmt.Errorf("no edge %v", edge)
	}

	meta, err := cursor.ReadDocument(nil, &contains)
	if err != nil {
		return driver.DocumentMeta{}, fmt.Errorf("failed reading cursor: %v", err)
	}
	return meta, nil
}
