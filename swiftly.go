package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ncw/swift"
)

// Obj is the object which is being operated on
type Obj struct {
	hash     string
	filePath string
	objPath  string
}

var (
	conn       swift.Connection
	dir        = flag.String("dir", "", "The directory which should be synced.")
	domain     = flag.String("domain", "", "Your domain name.  eg: www.example.com or example.com")
	endpoint   = flag.String("endpoint", "https://auth-east.cloud.ca/v2.0", "The Cloud.ca object storage public url")
	exclude    = flag.String("exclude", "", "A comma separated list of files or directories to exclude from upload.")
	identity   = flag.String("identity", "", "Your Cloud.ca object storage identity")
	password   = flag.String("password", "", "Your Cloud.ca object storage password")
	concurrent = flag.Int("concurrent", 4, "The number of files to be uploaded concurrently (reduce if 'too many files open' errors occur)")
)

func main() {
	flag.Parse()

	// verify dir has been passed
	if *dir == "" {
		fmt.Println("\nERROR: 'dir' is required")
		flag.Usage()
		os.Exit(2)
	}

	// verify and parse swift parameters
	if *identity == "" || *password == "" {
		fmt.Println("\nERROR: 'identity' and 'password' are required")
		flag.Usage()
		os.Exit(2)
	}

	// get the identity parts
	parts := strings.Split(*identity, ":")
	var tenant, username string
	if len(parts) > 1 {
		tenant = parts[0]
		username = parts[1]
	} else {
		fmt.Println("\nERROR: The 'identity' needs to be formated as '<tenant>:<username>'")
		flag.Usage()
		os.Exit(2)
	}

	// make dir absolute so it is easier to work with
	absDir, err := filepath.Abs(*dir)
	if err != nil {
		fmt.Printf("\nERROR: Problem locating directory '%s'\n->%s\n\n", *dir, err.Error())
		os.Exit(2)
	}

	// determine which bucket to upload it to
	_url := *domain
	if !strings.HasPrefix(*domain, "http") {
		_url = fmt.Sprintf("http://%s", *domain)
	}
	rawURL, _ := url.Parse(_url)
	bucket := rawURL.Host

	// upload contents of `absDir`
	// make a swift connection
	conn = swift.Connection{
		Tenant:   tenant,
		UserName: username,
		ApiKey:   *password,
		AuthUrl:  *endpoint,
	}

	// authenticate swift user
	err = conn.Authenticate()
	if err != nil {
		fmt.Println("\nERROR: Authentication failed.  Validate your credentials are correct")
		os.Exit(2)
	}

	// create the container if it does not already exist
	err = conn.ContainerCreate(bucket, nil)
	if err != nil {
		fmt.Println("\nERROR: Problem (creating | validating) the bucket")
		fmt.Println(err)
		os.Exit(2)
	}
	fmt.Printf("Using bucket: %s\n", bucket)

	// update container headers
	metadata := make(swift.Metadata, 0)
	metadata["web-index"] = "index.html" // serve index.html files
	metadata["web-error"] = ".html"      // serve 404.html on 404 error
	headers := metadata.ContainerHeaders()
	headers["X-Container-Read"] = ".r:*,.rlistings" // make the container public
	err = conn.ContainerUpdate(bucket, headers)
	if err != nil {
		fmt.Println("\nERROR: Problem updating headers for bucket")
		fmt.Println(err)
		os.Exit(2)
	}

	// get object names of all existing objects so we can delete stale objects
	objClean, err := conn.ObjectNamesAll(bucket, nil)
	if err != nil {
		fmt.Println("\nERROR: Problem getting existing object names")
		fmt.Println(err)
	}

	// walk the file system and pull out the important info (because 'Walk' is a blocking function)
	dirs := make([]*Obj, 0)
	objs := make([]*Obj, 0)
	prePath := ""
	prePathParts := strings.Split(prePath, string(os.PathSeparator))
	preDirs := ""
	// loop through the path parts to build all sub folder objects as well
	for i := 0; i < len(prePathParts); i++ {
		if preDirs == "" {
			preDirs = prePathParts[i]
		} else {
			preDirs = strings.Join([]string{preDirs, prePathParts[i]}, "/")
		}
		dirs = append(dirs, &Obj{
			objPath: preDirs,
		})
	}
	// walk the file structure to build the object structure
	err = filepath.Walk(absDir, func(path string, info os.FileInfo, _ error) (err error) {
		if !excludePath(path, *exclude) {
			objPath := strings.TrimPrefix(path, absDir)                     // remove absDir from path
			objPath = strings.TrimPrefix(objPath, string(os.PathSeparator)) // remove leading slash if it exists
			if len(objPath) > 0 {
				if prePath != "" {
					objPath = strings.Join([]string{prePath, objPath}, string(os.PathSeparator))
				}
				objPath = filepath.ToSlash(objPath) // fix windows paths
				if info.IsDir() {                   // add as directory
					dirs = append(dirs, &Obj{
						objPath: objPath,
					})
					objClean = removeFrom(objClean, objPath)
				} else {
					if info.Mode().IsRegular() && objPath != ".DS_Store" { // add as object
						objs = append(objs, &Obj{
							filePath: path,
							objPath:  objPath,
						})
						objClean = removeFrom(objClean, objPath)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("\nERROR: Problem discovering a file")
		fmt.Println(err)
		os.Exit(2)
	}

	// remove all the stale objects which exist in the object store but are not needed anymore
	if len(objClean) > 0 {
		_, err := conn.BulkDelete(bucket, objClean)
		if err != nil {
			fmt.Println("\nERROR: Problem deleting stale objects")
			fmt.Println(err)
		}
		for i := 0; i < len(objClean); i++ {
			fmt.Printf(" removed: %s\n", objClean[i])

		}
	}

	// put all the dirs in place initially
	var dirWG sync.WaitGroup
	for _, p := range dirs {
		dirWG.Add(1)
		go func(objPath string) error {
			defer dirWG.Done()
			if objPath != "" {
				obj, _, err := conn.Object(bucket, objPath)
				if err == nil && obj.ContentType == "application/directory" {
					fmt.Printf("unchanged: %s\n", objPath)
				} else {
					err = conn.ObjectPutString(bucket, objPath, "", "application/directory")
					if err != nil {
						fmt.Printf("\nERROR: Problem creating folder '%s'\n", objPath)
						fmt.Println(err)
						return err
					}
					fmt.Printf("added dir: %s\n", objPath)
				}
			}
			return nil
		}(p.objPath)
	}
	dirWG.Wait()

	// now upload all the objects into the established dirs
	processPath := func(path, objPath string) error {
		hash, err := getHash(path)
		if err != nil {
			fmt.Printf("\nERROR: Problem creating hash for path '%s'\n", path)
			fmt.Println(err)
			return err
		}
		obj, _, err := conn.Object(bucket, objPath)
		if err != nil || obj.Hash != hash {
			fmt.Printf("  started: %s\n", objPath)
			f, err := os.Open(path)
			if err != nil {
				fmt.Printf("\nERROR: Problem opening file '%s'\n", path)
				fmt.Println(err)
				return err
			}
			defer f.Close()
			_, err = conn.ObjectPut(bucket, objPath, f, true, hash, "", nil)
			if err != nil {
				fmt.Printf("\nERROR: Problem uploading object '%s'\n", objPath)
				fmt.Println(err)
				return err
			}
			fmt.Printf(" uploaded: %s\n", objPath)
		} else {
			fmt.Printf(" unchanged: %s\n", objPath)
		}
		return nil
	}

	// setup 'processPath' concurrency controls
	pathC := make(chan *Obj)
	var objWG sync.WaitGroup
	// setup the number of concurrent goroutine workers
	for i := 0; i < *concurrent; i++ {
		objWG.Add(1)
		go func() {
			for p := range pathC {
				processPath(p.filePath, p.objPath)
			}
			objWG.Done()
		}()
	}
	// feed the paths into the concurrent goroutines to be executed
	for _, p := range objs {
		pathC <- p
	}
	close(pathC)
	objWG.Wait()

	// profit!!!  :P
}

func removeFrom(list []string, remove string) []string {
	// loop backwards so we can use the same index loop while changing the list
	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == remove {
			list, list[len(list)-1] = append(list[:i], list[i+1:]...), ""
		}
	}
	return list
}

func excludePath(path, excludeStr string) bool {
	if excludeStr != "" {
		excluded := make([]string, 0)
		excludeParts := strings.Split(excludeStr, ",")
		for _, p := range excludeParts {
			tmpPath, err := filepath.Abs(strings.TrimSpace(p))
			if err != nil {
				continue
			}
			excluded = append(excluded, tmpPath)
		}
		for _, p := range excluded {
			if strings.HasPrefix(path, p) {
				return true
			}
		}
	}
	return false
}

func getHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
