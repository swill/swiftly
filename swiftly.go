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

type Obj struct {
	hash      string
	file_path string
	obj_path  string
}

var (
	conn       swift.Connection
	dir        = flag.String("dir", "", "The directory which should be synced.")
	domain     = flag.String("domain", "", "Your domain name.  eg: www.example.com or example.com")
	endpoint   = flag.String("endpoint", "https://auth-east.cloud.ca/v2.0", "The Cloud.ca object storage public url")
	identity   = flag.String("identity", "", "Your Cloud.ca object storage identity")
	password   = flag.String("password", "", "Your Cloud.ca object storage password")
	concurrent = flag.Int("concurrent", 4, "The number of files to be uploaded concurrently (reduce if 'too many files open' errors occur)")
)

func main() {
	flag.Parse()

	// verify and parse swift parameters
	if *identity == "" || *password == "" {
		fmt.Println("\nERROR: 'identity' and 'password' are required\n")
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
		fmt.Println("\nERROR: The 'identity' needs to be formated as '<tenant>:<username>'\n")
		flag.Usage()
		os.Exit(2)
	}

	// make dir absolute so it is easier to work with
	abs_dir, err := filepath.Abs(*dir)
	if err != nil {
		fmt.Printf("\nERROR: Problem locating directory '%s'\n->%s\n\n", *dir, err.Error())
		os.Exit(2)
	}

	// determine which bucket to upload it to
	_url := *domain
	if !strings.HasPrefix(*domain, "http") {
		_url = fmt.Sprintf("http://%s", *domain)
	}
	raw_url, _ := url.Parse(_url)
	bucket := raw_url.Host

	// upload contents of `abs_dir`
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
		fmt.Println("\nERROR: Authentication failed.  Validate your credentials are correct\n")
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
	obj_clean, err := conn.ObjectNamesAll(bucket, nil)
	if err != nil {
		fmt.Println("\nERROR: Problem getting existing object names")
		fmt.Println(err)
	}

	// walk the file system and pull out the important info (because 'Walk' is a blocking function)
	dirs := make([]*Obj, 0)
	objs := make([]*Obj, 0)
	pre_path := "" //strings.Trim(*prefix, string(os.PathSeparator)) // object pre path
	pre_path_parts := strings.Split(pre_path, string(os.PathSeparator))
	pre_dirs := ""
	// loop through the path parts to build all sub folder objects as well
	for i := 0; i < len(pre_path_parts); i++ {
		if pre_dirs == "" {
			pre_dirs = pre_path_parts[i]
		} else {
			pre_dirs = strings.Join([]string{pre_dirs, pre_path_parts[i]}, "/")
		}
		dirs = append(dirs, &Obj{
			obj_path: pre_dirs,
		})
	}
	// walk the file structure to build the object structure
	err = filepath.Walk(abs_dir, func(path string, info os.FileInfo, _ error) (err error) {
		obj_path := strings.TrimPrefix(path, abs_dir)                     // remove abs_dir from path
		obj_path = strings.TrimPrefix(obj_path, string(os.PathSeparator)) // remove leading slash if it exists
		if len(obj_path) > 0 {
			if pre_path != "" {
				obj_path = strings.Join([]string{pre_path, obj_path}, string(os.PathSeparator))
			}
			obj_path = filepath.ToSlash(obj_path) // fix windows paths
			if info.IsDir() {                     // add as directory
				dirs = append(dirs, &Obj{
					obj_path: obj_path,
				})
				obj_clean = remove_from(obj_clean, obj_path)
			} else {
				if info.Mode().IsRegular() && obj_path != ".DS_Store" { // add as object
					objs = append(objs, &Obj{
						file_path: path,
						obj_path:  obj_path,
					})
					obj_clean = remove_from(obj_clean, obj_path)
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("\nERROR: Problem discovering a file\n")
		fmt.Println(err)
		os.Exit(2)
	}

	// remove all the stale objects which exist in the object store but are not needed anymore
	if len(obj_clean) > 0 {
		_, err := conn.BulkDelete(bucket, obj_clean)
		if err != nil {
			fmt.Println("\nERROR: Problem deleting stale objects")
			fmt.Println(err)
		}
		for i := 0; i < len(obj_clean); i++ {
			fmt.Printf(" removed: %s\n", obj_clean[i])

		}
	}

	// put all the dirs in place initially
	var dir_wg sync.WaitGroup
	for _, p := range dirs {
		dir_wg.Add(1)
		go func(obj_path string) error {
			defer dir_wg.Done()
			if obj_path != "" {
				obj, _, err := conn.Object(bucket, obj_path)
				if err == nil && obj.ContentType == "application/directory" {
					fmt.Printf("unchanged: %s\n", obj_path)
				} else {
					err = conn.ObjectPutString(bucket, obj_path, "", "application/directory")
					if err != nil {
						fmt.Printf("\nERROR: Problem creating folder '%s'\n", obj_path)
						fmt.Println(err)
						return err
					}
					fmt.Printf("added dir: %s\n", obj_path)
				}
			}
			return nil
		}(p.obj_path)
	}
	dir_wg.Wait()

	// now upload all the objects into the established dirs
	process_path := func(path, obj_path string) error {
		hash, err := get_hash(path)
		if err != nil {
			fmt.Printf("\nERROR: Problem creating hash for path '%s'\n", path)
			fmt.Println(err)
			return err
		}
		obj, _, err := conn.Object(bucket, obj_path)
		if err != nil || obj.Hash != hash {
			fmt.Printf("  started: %s\n", obj_path)
			f, err := os.Open(path)
			if err != nil {
				fmt.Printf("\nERROR: Problem opening file '%s'\n", path)
				fmt.Println(err)
				return err
			}
			defer f.Close()
			_, err = conn.ObjectPut(bucket, obj_path, f, true, hash, "", nil)
			if err != nil {
				fmt.Printf("\nERROR: Problem uploading object '%s'\n", obj_path)
				fmt.Println(err)
				return err
			}
			fmt.Printf(" uploaded: %s\n", obj_path)
		} else {
			fmt.Printf(" unchanged: %s\n", obj_path)
		}
		return nil
	}

	// setup 'process_path' concurrency controls
	pathc := make(chan *Obj)
	var obj_wg sync.WaitGroup
	// setup the number of concurrent goroutine workers
	for i := 0; i < *concurrent; i++ {
		obj_wg.Add(1)
		go func() {
			for p := range pathc {
				process_path(p.file_path, p.obj_path)
			}
			obj_wg.Done()
		}()
	}
	// feed the paths into the concurrent goroutines to be executed
	for _, p := range objs {
		pathc <- p
	}
	close(pathc)
	obj_wg.Wait()

	// profit!!!  :P
}

func remove_from(list []string, remove string) []string {
	// loop backwards so we can use the same index loop while changing the list
	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == remove {
			list, list[len(list)-1] = append(list[:i], list[i+1:]...), ""
		}
	}
	return list
}

func get_hash(path string) (string, error) {
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
