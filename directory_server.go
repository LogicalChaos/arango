package main

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/karlseguin/ccache"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const (
	ChannelSize                = 100
	ChannelUpdateEmitFrequency = time.Minute
)

type FileHandlerPayload struct {
	FileInfo os.FileInfo
	FullPath string
}

/*
 * The directory server is a singleton goroutine responsible for ingesting FileHandlePayload objects emitted
 * by the scan pipeline and maintaining the directories collection, which is a graph-database representation
 * of the directory structure being scanned.
 */
type DirectoryServer struct {
	fileHandlerChannel        chan FileHandlerPayload
	filePayloadChannel        chan FileHandlerPayload
	stopChannel               chan struct{}
	running                   bool
	m                         sync.Mutex
	lastEmittedUpdateTime     time.Time
	lastEmittedWarningTime    time.Time
	lastEmittedErrorTime      time.Time
	totalDirectoriesProcessed uint64
	totalFilesProcessed       uint64
	filesProcessed            float64
}

var (
	directoryServerInstance *DirectoryServer
	directoryServerOnce     sync.Once
	ParallelFilePayload     int
	lruDirectoryCache       = ccache.New(ccache.Configure())
)

func init() {
	ParallelFilePayload = 5 * runtime.NumCPU() / 6
}

func GetDirectoryServer() *DirectoryServer {
	directoryServerOnce.Do(func() {
		directoryServerInstance = &DirectoryServer{
			fileHandlerChannel:        make(chan FileHandlerPayload, ChannelSize),
			filePayloadChannel:        make(chan FileHandlerPayload, ParallelFilePayload*10),
			stopChannel:               make(chan struct{}),
			running:                   false,
			lastEmittedUpdateTime:     time.Now(),
			lastEmittedWarningTime:    time.Now(),
			lastEmittedErrorTime:      time.Now(),
			totalDirectoriesProcessed: 0,
			totalFilesProcessed:       0,
			filesProcessed:            0,
		}
	})
	return directoryServerInstance
}

func (ds *DirectoryServer) Start() {
	ds.m.Lock()
	defer ds.m.Unlock()
	if !ds.running {
		ds.running = true
		go listen(ds)
		for i := 0; i < ParallelFilePayload; i++ {
			go processFilePayload(ds)
		}
		return
	}
}

func (ds *DirectoryServer) Stop() {
	ds.m.Lock()
	defer ds.m.Unlock()
	if ds.running {
		ds.running = false
		ds.stopChannel <- struct{}{}
		return
	}
}

func (ds *DirectoryServer) GetFileHandlerPayloadChannel() chan FileHandlerPayload {
	return ds.fileHandlerChannel
}

func listen(ds *DirectoryServer) {
	for {
		channelElements := len(ds.fileHandlerChannel)

		select {
		case filePayload := <-ds.fileHandlerChannel:
			if filePayload.FileInfo == nil {
				continue
			}
			if filePayload.FileInfo.IsDir() {
				processDirectoryPayload(ds, filePayload)
			} else {
				ds.filePayloadChannel <- filePayload
			}
			logUpdateIfNecessary(ds, filePayload, channelElements)
		case <-ds.stopChannel:
			for len(ds.fileHandlerChannel) > 0 {
				<-ds.fileHandlerChannel
			}
			return
		}
	}
}

func logUpdateIfNecessary(ds *DirectoryServer, filePayload FileHandlerPayload, channelElements int) {
	now := time.Now()
	lastUpdate := now.Sub(ds.lastEmittedUpdateTime)
	if lastUpdate > ChannelUpdateEmitFrequency {
		fmt.Printf("%d deep, processed %d files (%.1f/s), %d directories - %s\n", channelElements,
			ds.totalFilesProcessed, ds.filesProcessed/lastUpdate.Seconds(),
			ds.totalDirectoriesProcessed, filePayload.FullPath)
		ds.lastEmittedUpdateTime = now
		ds.filesProcessed = 0
	}
}

func processFilePayload(ds *DirectoryServer) {

	for {
		filePayload := <-ds.filePayloadChannel
		ds.m.Lock()
		ds.totalFilesProcessed++
		ds.filesProcessed++
		ds.m.Unlock()

		var parentDirectoryMeta driver.DocumentMeta
		value := lruDirectoryCache.Get(filePayload.FullPath)
		if value == nil {
			var parentDirectory Directory
			var err error
			parentDirectory, parentDirectoryMeta, err = getDirectory(filepath.Dir(filePayload.FullPath))
			if err != nil {
				parentDirectory = Directory{Path: filepath.Dir(filePayload.FullPath)}
				parentDirectoryMeta, err = directories.CreateDocument(nil, parentDirectory)
				if err != nil {
					log.Printf("failed creating parent directory %#v: %v\n", parentDirectory, err)
					return
				}
			}
			lruDirectoryCache.Set(filePayload.FullPath, &parentDirectoryMeta, time.Hour*24)
		} else {
			parentDirectoryMeta = *(value.Value().(*driver.DocumentMeta))
		}

		file := File{Name: filePayload.FullPath, FileSize: filePayload.FileInfo.Size(), Modified: filePayload.FileInfo.ModTime()}
		fileMeta, err := fileobjects.CreateDocument(nil, file)
		if err != nil {
			log.Printf("failed creating file %v: %v\n", file, err)
			return
		}
		edge := Contains{"directories/" + parentDirectoryMeta.Key, "fileobjects/" + fileMeta.Key}
		_, err = edges.CreateDocument(nil, edge)
		if err != nil {
			log.Printf("failed creating edge %#v: %v\n", edge, err)
			return
		}
	}
}

func processDirectoryPayload(ds *DirectoryServer, filePayload FileHandlerPayload) {
	ds.totalDirectoriesProcessed++
	created := false

	currentDirectory, currentDirectoryMeta, err := getDirectory(filePayload.FullPath)
	if err != nil {
		currentDirectory = Directory{Path: filePayload.FullPath}
		currentDirectoryMeta, err = directories.CreateDocument(nil, currentDirectory)
		if err != nil {
			log.Printf("failed creating  directory %#v: %v\n", currentDirectory, err)
			return
		}
		created = true
	}
	lruDirectoryCache.Set(filePayload.FullPath, &currentDirectoryMeta, time.Hour*24)

	var parentDirectoryMeta driver.DocumentMeta
	parent := filepath.Dir(filePayload.FullPath)

	value := lruDirectoryCache.Get(parent)
	if value == nil {
		_, parentDirectoryMeta, err = getDirectory(parent)
	} else {
		parentDirectoryMeta = *(value.Value().(*driver.DocumentMeta))
	}

	if created && err == nil {
		edge := Contains{"directories/" + parentDirectoryMeta.Key, "directories/" + currentDirectoryMeta.Key}
		_, err := getEdge(edge)
		if err != nil {
			_, err = edges.CreateDocument(nil, edge)
			if err != nil {
				log.Printf("failed creating edge %#v: %v\n", edge, err)
			}
		}
	}
}
